import contextlib
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

import polars as pl
import pyarrow as pa

from lakehouse_engine.governor import Mode
from lakehouse_engine.query.duckdb_adapter import DuckDBSession
from lakehouse_engine.query.polars_adapter import PolarsAdapter
from lakehouse_engine.query.protocols import ArrowStreamExportable

if TYPE_CHECKING:
    from lakehouse_engine.catalog.manager import CatalogManager
    from lakehouse_engine.config import QuerySettings
    from lakehouse_engine.governor import ResourceGovernor


class QueryService:
    def __init__(
        self, catalog: "CatalogManager", settings: "QuerySettings", governor: "ResourceGovernor"
    ) -> None:
        self._catalog = catalog
        self._settings = settings
        self._governor = governor
        self._polars_adapter = PolarsAdapter()

    def polars_lazy(
        self, *, snapshot_id: int | None = None, columns: Sequence[str] | None = None
    ) -> pl.LazyFrame:
        with self._governor.lease(Mode.QUERY):
            snap_id = snapshot_id if snapshot_id is not None else self._catalog.snapshot_id()
            tbl = self._catalog.current_table()
            if self._settings.polars_strategy == "parquet_files":
                lf = self._polars_adapter.scan_iceberg_files(tbl, snapshot_id=snap_id)
            else:
                try:
                    lf = self._polars_adapter.scan_iceberg(tbl, snapshot_id=snap_id)
                except Exception:
                    lf = self._polars_adapter.scan_iceberg_files(tbl, snapshot_id=snap_id)
            if columns:
                lf = lf.select(list(columns))
            return lf

    def collect_polars(
        self, *, snapshot_id: int | None = None, columns: Sequence[str] | None = None
    ) -> pl.DataFrame:
        with self._governor.lease(Mode.QUERY):
            lf = self.polars_lazy(snapshot_id=snapshot_id, columns=columns)
            return lf.collect(engine="streaming")

    def polars_from_stream(
        self, source: ArrowStreamExportable, *, schema: pa.Schema
    ) -> pl.LazyFrame:
        with self._governor.lease(Mode.QUERY):
            return self._polars_adapter.scan_stream(source, schema)

    def duckdb_session(self, *, snapshot_id: int | None = None) -> DuckDBSession:
        with self._governor.lease(Mode.QUERY):
            session = DuckDBSession(self._settings)
            snap_id = snapshot_id if snapshot_id is not None else self._catalog.snapshot_id()
            if snap_id is not None:
                with contextlib.suppress(Exception):
                    reader = self.to_arrow_reader(snapshot_id=snap_id)
                    session.register_stream("events", reader)
            return session

    def query_duckdb(
        self, query: str, *, snapshot_id: int | None = None, params: dict[str, Any] | None = None
    ) -> pa.Table:
        with (
            self._governor.lease(Mode.QUERY),
            self.duckdb_session(snapshot_id=snapshot_id) as session,
        ):
            rel = session.sql(query, params=params)
            return rel.fetch_arrow_table()

    def to_arrow_reader(
        self,
        *,
        snapshot_id: int | None = None,
        row_filter: str = "true",
        columns: Sequence[str] | None = None,
        batch_rows: int | None = None,
    ) -> pa.RecordBatchReader:
        with self._governor.lease(Mode.QUERY):
            snap_id = snapshot_id if snapshot_id is not None else self._catalog.snapshot_id()
            tbl = self._catalog.current_table()
            scan = tbl.scan(
                snapshot_id=snap_id,
                row_filter=row_filter,
                selected_fields=tuple(columns) if columns else ("*",),
            )
            reader: pa.RecordBatchReader = scan.to_arrow_batch_reader()
            return reader
