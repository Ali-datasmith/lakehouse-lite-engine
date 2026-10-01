# src/lakehouse_engine/query/polars_adapter.py
import contextlib
from collections.abc import Generator
from typing import Any, cast
from urllib.parse import urlparse

import polars as pl
import pyarrow as pa
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.table import Table

from lakehouse_engine.exceptions import EngineCapabilityError
from lakehouse_engine.query.protocols import ArrowStreamExportable


class PolarsAdapter:
    def scan_iceberg(self, table: Table, *, snapshot_id: int | None = None) -> pl.LazyFrame:
        """Primary path using polars.scan_iceberg."""
        return pl.scan_iceberg(table, snapshot_id=snapshot_id)

    def scan_iceberg_files(self, table: Table, *, snapshot_id: int | None = None) -> pl.LazyFrame:
        """Fallback path resolving data file URIs for snapshot_id and scanning Parquet files."""
        scan = table.scan(snapshot_id=snapshot_id)
        file_uris: list[str] = [task.file.file_path for task in scan.plan_files()]
        if not file_uris:
            schema_arrow = schema_to_pyarrow(table.schema())
            empty_table = pa.Table.from_batches([], schema=schema_arrow)
            return cast("pl.LazyFrame", pl.from_arrow(empty_table).lazy())  # type: ignore[union-attr]

        # Handle local file:// vs path
        paths: list[str] = []
        for uri in file_uris:
            parsed = urlparse(uri)
            paths.append(parsed.path if parsed.scheme == "file" else uri)

        return pl.scan_parquet(paths)

    def scan_stream(self, source: ArrowStreamExportable, schema: pa.Schema) -> pl.LazyFrame:
        """Capability-probing stream adapter."""
        # Probe 1: pl.scan_arrow_c_stream
        if hasattr(pl, "scan_arrow_c_stream"):
            scan_fn: Any = pl.scan_arrow_c_stream
            return cast("pl.LazyFrame", scan_fn(source))

        # Probe 2: polars.io.plugins.register_io_source
        with contextlib.suppress(Exception):
            import polars.io.plugins as pl_plugins

            if hasattr(pl_plugins, "register_io_source"):
                reader = pa.RecordBatchReader.from_stream(source)

                def batch_generator() -> Generator[pl.DataFrame]:
                    for batch in reader:
                        res = pl.from_arrow(batch)
                        if isinstance(res, pl.DataFrame):
                            yield res

                reg_fn: Any = pl_plugins.register_io_source
                return cast("pl.LazyFrame", reg_fn(batch_generator, schema=schema))

        raise EngineCapabilityError(
            "Pinned Polars version lacks required streaming scan capabilities "
            "(neither scan_arrow_c_stream nor register_io_source is supported)."
        )
