# src/lakehouse_engine/query/service.py
import contextlib
from collections.abc import Sequence
from typing import TYPE_CHECKING

import polars as pl
import pyarrow as pa
from pyiceberg.expressions import AlwaysTrue

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
        self,
        catalog: "CatalogManager",
        settings: "QuerySettings",
        governor: "ResourceGovernor",
    ) -> None:
        self._catalog = catalog
        self._settings = settings
        self._governor = governor
        self._polars_adapter = PolarsAdapter()

    def polars_lazy(
        self,
        *,
        snapshot_id: int | None = None,
        columns: Sequence[str] | None = None,
    ) -> pl.LazyFrame:
        """Snapshot-pinned LazyFrame."""
        with self._governor.lease(Mode.QUERY):
            snap_id = snapshot_id if snapshot_id is not None else self._catalog.snapshot_id()
            tbl = self._catalog.current_table()

            if self._settings.polars_strategy == "parquet_files":
                lf = self._polars_adapter.scan_iceberg_files(tbl, snapshot_id=snap_id)
            else:
                try:
                    lf = self._polars_adapter.scan_iceberg(tbl, snapshot_id=snap_id)
                except Exception:  # noqa: BLE001
                    lf = self._polars_adapter.scan_iceberg_files(tbl, snapshot_id=snap_id)

            if columns:
                lf = lf.select(list(columns))
            return lf

    def polars_from_stream(
        self, source: ArrowStreamExportable, *, schema: pa.Schema
    ) -> pl.LazyFrame:
        """Wrap any __arrow_c_stream__ object as an out-of-core LazyFrame."""
        with self._governor.lease(Mode.QUERY):
            return self._polars_adapter.scan_stream(source, schema)

    def duckdb_session(self, *, snapshot_id: int | None = None) -> DuckDBSession:
        """Creates a DuckDBSession."""
        with self._governor.lease(Mode.QUERY):
            session = DuckDBSession(self._settings)
            # Register current table as 'events' stream/relation if table has data
            snap_id = snapshot_id if snapshot_id is not None else self._catalog.snapshot_id()
            if snap_id is not None:
                with contextlib.suppress(Exception):
                    reader = self.to_arrow_reader(snapshot_id=snap_id)
                    session.register_stream("events", reader)
            return session

    def to_arrow_reader(
        self,
        *,
        snapshot_id: int | None = None,
        row_filter: str = "true",
        columns: Sequence[str] | None = None,
        batch_rows: int | None = None,
    ) -> pa.RecordBatchReader:
        """Streaming, snapshot-pinned PyArrow RecordBatchReader."""
        with self._governor.lease(Mode.QUERY):
            snap_id = snapshot_id if snapshot_id is not None else self._catalog.snapshot_id()
            tbl = self._catalog.current_table()

            _ = batch_rows if batch_rows is not None else self._settings.arrow_batch_rows

            scan = tbl.scan(
                snapshot_id=snap_id,
                row_filter=AlwaysTrue() if row_filter == "true" else row_filter,
                selected_fields=tuple(columns) if columns else ("*",),
                limit=None,
            )
            reader: pa.RecordBatchReader = scan.to_arrow_batch_reader()
            return reader
