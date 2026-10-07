from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

import pyarrow as pa
import pytest
from pyiceberg.exceptions import CommitFailedException

from lakehouse_engine.buffer.compaction import CompactionPlan
from lakehouse_engine.catalog.manager import CatalogManager
from lakehouse_engine.config import EngineSettings, IngestionSettings
from lakehouse_engine.exceptions import CatalogCommitError, CompactionError, EngineCapabilityError
from lakehouse_engine.ingestion.dlq import DeadLetter, NdjsonDeadLetterSink
from lakehouse_engine.query.polars_adapter import PolarsAdapter


def test_dlq_max_total_bytes_pruning(tmp_path: Path) -> None:
    settings = IngestionSettings(
        dlq_dir=tmp_path / "dlq",
        dlq_rotate_bytes=1024 * 1024,
        dlq_retention_days=30,
        dlq_max_total_bytes=10 * 1024 * 1024,
    )
    sink = NdjsonDeadLetterSink(settings)

    dl = DeadLetter(
        received_at=datetime.now(UTC),
        source="test",
        line_no=1,
        source_offset=1,
        reason="validation",
        errors=[],
        raw="x" * 2000,
        engine_schema_version="1",
    )

    for _ in range(5):
        sink.write(dl)
        sink.flush()

    sink.close()

    dlq_files = list((tmp_path / "dlq").glob("dlq-*.ndjson"))
    assert len(dlq_files) > 0


def test_compaction_row_mismatch_error(tmp_path: Path) -> None:
    settings = EngineSettings.load(
        catalog={
            "uri": f"sqlite:///{tmp_path}/catalog.db",
            "warehouse_uri": f"file://{tmp_path}/warehouse",
        },
        ingestion={"dlq_dir": tmp_path / "dlq"},
        query={"duckdb_temp_dir": tmp_path / "duckdb-spill"},
    )
    from lakehouse_engine.engine import LakehouseEngine

    with LakehouseEngine(settings) as engine:
        plan = CompactionPlan(
            input_paths=(),
            input_rows=999,
            input_bytes=100,
            snapshot_id=engine._catalog.snapshot_id() or 0,
        )
        with pytest.raises(CompactionError, match="Row count mismatch"):
            engine._compactor.run(plan)


def test_catalog_manager_commit_retry_and_backoff(tmp_path: Path) -> None:
    settings = EngineSettings.load(
        catalog={
            "uri": f"sqlite:///{tmp_path}/catalog.db",
            "warehouse_uri": f"file://{tmp_path}/warehouse",
            "commit_max_attempts": 2,
            "commit_backoff_base_seconds": 0.01,
            "commit_backoff_max_seconds": 0.05,
        },
        ingestion={"dlq_dir": tmp_path / "dlq"},
        query={"duckdb_temp_dir": tmp_path / "duckdb-spill"},
    )
    cat = CatalogManager(settings.catalog, settings.storage)
    cat.open()

    from lakehouse_engine.buffer.buffer import FlushedFile, FlushReason

    ff = FlushedFile(
        path="dummy.parquet",
        flush_id="fid1",
        rows=10,
        arrow_bytes=100,
        file_bytes=100,
        row_groups=1,
        reason=FlushReason.EXPLICIT,
    )

    orig_load_table = cat._catalog.load_table  # type: ignore[union-attr]

    def failing_load_table(identifier: str) -> object:
        tbl = orig_load_table(identifier)
        tbl.add_files = MagicMock(side_effect=CommitFailedException("Concurrent commit conflict"))
        return tbl

    cat._catalog.load_table = failing_load_table  # type: ignore[method-assign,union-attr]

    with pytest.raises(CatalogCommitError, match="Commit exhausted"):
        cat.commit_files([ff], flush_id="fid1")

    cat.close()


def test_polars_adapter_capability_error() -> None:
    adapter = PolarsAdapter()
    batch = pa.RecordBatch.from_arrays(
        [pa.array([10], type=pa.int64())],
        names=["num"],
    )
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])

    import polars as pl
    import polars.io.plugins as pl_plugins

    with pytest.MonkeyPatch.context() as mp:
        mp.delattr(pl, "scan_arrow_c_stream", raising=False)
        mp.delattr(pl_plugins, "register_io_source", raising=False)
        with pytest.raises(
            EngineCapabilityError, match="lacks required streaming scan capabilities"
        ):
            adapter.scan_stream(reader, batch.schema)
