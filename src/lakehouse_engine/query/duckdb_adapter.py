import re
from collections.abc import Mapping, Sequence
from types import TracebackType
from typing import TYPE_CHECKING

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
    def __init__(self, settings: "QuerySettings") -> None:
        self._settings = settings
        self._consumed_streams: set[str] = set()
        mem_mb = f"{settings.duckdb_memory_limit_bytes // (1024 * 1024)}MB"
        temp_path = settings.duckdb_temp_dir.resolve()
        temp_path.mkdir(parents=True, exist_ok=True, mode=0o700)
        config: dict[str, str | bool | int | float | list[str]] = {
            "memory_limit": mem_mb,
            "threads": str(settings.duckdb_threads),
            "preserve_insertion_order": False,
        }
        self._con = duckdb.connect(":memory:", config=config)
        self._con.execute(f"SET temp_directory = '{temp_path}'")
        self._con.execute("SET enable_external_access = false")
        self._con.execute("SET lock_configuration = true")

    def __enter__(self) -> "DuckDBSession":
        return self

    def __exit__(
        self, et: type[BaseException] | None, ev: BaseException | None, tb: TracebackType | None
    ) -> None:
        self.close()

    def close(self) -> None:
        if self._con is not None:
            self._con.close()

    def register_stream(self, name: str, source: ArrowStreamExportable) -> None:
        if not IDENTIFIER_REGEX.match(name):
            raise UnsafeSqlError(f"Invalid relation name: '{name}'")
        if name in self._consumed_streams:
            raise StreamConsumedError(f"Stream '{name}' has already been registered and consumed.")
        rel = self._con.from_arrow(source)
        rel.create_view(name, replace=True)
        self._consumed_streams.add(name)

    def register_frame(self, name: str, frame: pl.DataFrame) -> None:
        if not IDENTIFIER_REGEX.match(name):
            raise UnsafeSqlError(f"Invalid relation name: '{name}'")
        rel = self._con.from_arrow(frame)
        rel.create_view(name, replace=True)

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
        self, query: str, *, params: Mapping[str, object] | None = None, batch_rows: int = 100_000
    ) -> pa.RecordBatchReader:
        self._validate_query(query)
        if params:
            self._con.execute(query, list(params.values()))
        else:
            self._con.execute(query)
        return self._con.fetch_record_batch(batch_rows)
