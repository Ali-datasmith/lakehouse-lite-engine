# src/lakehouse_engine/query/duckdb_adapter.py
import contextlib
import re
from collections.abc import Mapping, Sequence
from types import TracebackType
from typing import TYPE_CHECKING, Any

import duckdb
import polars as pl
import pyarrow as pa

from lakehouse_engine.exceptions import StreamConsumedError, UnsafeSqlError
from lakehouse_engine.query.protocols import ArrowStreamExportable

if TYPE_CHECKING:
    from lakehouse_engine.config import QuerySettings

IDENTIFIER_REGEX = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")
ALLOWED_SQL_PREFIXES = ("SELECT", "WITH", "DESCRIBE", "EXPLAIN")


class DuckDBSession:
    """Owns exactly one connection: duckdb.connect(":memory:", config={...}).

    Config: memory_limit, threads, temp_directory, preserve_insertion_order=False.
    """

    def __init__(self, settings: "QuerySettings") -> None:
        self._settings = settings
        self._consumed_streams: set[str] = set()

        mem_mb = f"{settings.duckdb_memory_limit_bytes // (1024 * 1024)}MB"
        temp_path = settings.duckdb_temp_dir.resolve()
        temp_path.mkdir(parents=True, exist_ok=True)
        with contextlib.suppress(Exception):
            temp_path.chmod(0o700)

        config: dict[str, str | bool | int | float | list[str]] = {
            "memory_limit": mem_mb,
            "threads": str(settings.duckdb_threads),
            "temp_directory": str(temp_path),
            "preserve_insertion_order": False,
        }

        self._con = duckdb.connect(":memory:", config=config)

    def __enter__(self) -> "DuckDBSession":
        return self

    def __exit__(
        self,
        et: type[BaseException] | None,
        ev: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        if self._con is not None:
            self._con.close()

    def register_stream(self, name: str, source: ArrowStreamExportable) -> None:
        """name must match ^[A-Za-z_][A-Za-z0-9_]{0,62}$ else UnsafeSqlError.

        Uses con.from_arrow(source) then rel.create_view(name). ONE-SHOT: a stream is consumed
        by the first query that scans it; a second scan raises StreamConsumedError.
        """
        if not IDENTIFIER_REGEX.match(name):
            raise UnsafeSqlError(f"Invalid relation name: '{name}'")

        if name in self._consumed_streams:
            raise StreamConsumedError(f"Stream '{name}' has already been registered and consumed.")

        rel = self._con.from_arrow(source)
        rel.create_view(name)
        self._consumed_streams.add(name)

    def register_frame(self, name: str, frame: pl.DataFrame) -> None:
        """Bounded results only. Uses DataFrame __arrow_c_stream__ (PyCapsule)."""
        if not IDENTIFIER_REGEX.match(name):
            raise UnsafeSqlError(f"Invalid relation name: '{name}'")

        rel = self._con.from_arrow(frame)
        rel.create_view(name)

    def _validate_query(self, query: str) -> None:
        cleaned = query.strip()
        lines = [line for line in cleaned.splitlines() if not line.strip().startswith("--")]
        cleaned = " ".join(lines).strip()
        first_word = cleaned.split()[0].upper() if cleaned else ""
        if first_word not in ALLOWED_SQL_PREFIXES:
            raise UnsafeSqlError(
                f"SQL statement must start with one of {ALLOWED_SQL_PREFIXES}, got '{first_word}'"
            )

    def sql(
        self, query: str, *, params: Mapping[str, object] | Sequence[object] | None = None
    ) -> duckdb.DuckDBPyRelation:
        self._validate_query(query)
        if params is not None:
            return self._con.sql(query, params=params)
        return self._con.sql(query)

    def sql_stream(
        self,
        query: str,
        *,
        params: Mapping[str, object] | None = None,
        batch_rows: int = 100_000,
    ) -> pa.RecordBatchReader:
        """Streaming Arrow result (no fetchall/fetchdf)."""
        self._validate_query(query)
        if params:
            param_list = list(params.values())
            res: Any = self._con.execute(query, param_list)
            reader: pa.RecordBatchReader = res.to_arrow_reader(batch_rows)
            return reader

        rel = self._con.sql(query)
        reader = rel.to_arrow_reader(batch_rows)
        return reader
