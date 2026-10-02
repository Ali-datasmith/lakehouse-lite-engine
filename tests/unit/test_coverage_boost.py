from pathlib import Path

import pydantic_core
import pytest

from lakehouse_engine.buffer.compaction import CompactionPlan
from lakehouse_engine.config import EngineSettings, IngestionSettings, QuerySettings
from lakehouse_engine.engine import LakehouseEngine
from lakehouse_engine.exceptions import (
    CatalogCommitError,
    CompactionError,
    DeadLetterThresholdExceeded,
)
from lakehouse_engine.ingestion.dlq import NdjsonDeadLetterSink
from lakehouse_engine.ingestion.validator import MicroBatchValidator
from lakehouse_engine.query.duckdb_adapter import DuckDBSession


def test_validator_reject_ratio_breaker(tmp_path: Path) -> None:
    settings = IngestionSettings(
        dlq_dir=tmp_path / "dlq",
        reject_ratio_min_sample=5,
        max_reject_ratio=0.3,
    )
    sink = NdjsonDeadLetterSink(settings)
    validator = MicroBatchValidator(settings, sink)

    # Ingest malformed lines to trip reject ratio breaker
    malformed_ndjson = (
        b'{"event_id": 1}\n{"invalid": }\n{"invalid": }\n{"invalid": }\n{"invalid": }\n'
    )
    with pytest.raises(DeadLetterThresholdExceeded):
        validator.validate(malformed_ndjson, source="test_breaker")


def test_validator_oversize_line_and_drift(tmp_path: Path) -> None:
    settings = IngestionSettings(
        dlq_dir=tmp_path / "dlq",
        max_line_bytes=300,
        dlq_raw_truncate_bytes=256,
    )
    sink = NdjsonDeadLetterSink(settings)
    validator = MicroBatchValidator(settings, sink)

    # Line exceeding max_line_bytes
    long_line = (
        b'{"event_id": 1, "user_id": 2, "event_name": "a", "event_ts": "2026-09-30T12:00:00Z", '
        b'"payload": "' + b"x" * 400 + b'"}\n'
    )
    vbatch = validator.validate(long_line, source="test_line")
    assert vbatch.rejected == 1

    # Extra forbidden schema drift
    drift = (
        b'{"event_id": 1, "user_id": 2, "event_name": "a", "event_ts": "2026-09-30T12:00:00Z", '
        b'"unknown_field": "drift"}\n'
    )
    vbatch_drift = validator.validate(drift, source="test_drift")
    assert vbatch_drift.rejected == 1


def test_duckdb_sql_stream_and_frame(tmp_path: Path) -> None:
    q_settings = QuerySettings(duckdb_temp_dir=tmp_path / "spill")
    session = DuckDBSession(q_settings)

    # Test sql_stream
    reader = session.sql_stream("SELECT 100 as num, 'hello' as str", batch_rows=10)
    batch = reader.read_next_batch()
    assert batch.column("num")[0].as_py() == 100

    # Test sql parameterized stream
    p_reader = session.sql_stream("SELECT $1 as val", params={"p1": 42})
    p_batch = p_reader.read_next_batch()
    assert p_batch.column("val")[0].as_py() == 42

    session.close()


def test_compaction_stale_plan(tmp_path: Path) -> None:
    settings = EngineSettings.load(
        catalog={
            "uri": f"sqlite:///{tmp_path}/catalog.db",
            "warehouse_uri": f"file://{tmp_path}/warehouse",
        },
        ingestion={"dlq_dir": tmp_path / "dlq"},
        query={"duckdb_temp_dir": tmp_path / "duckdb-spill"},
    )

    with LakehouseEngine(settings) as engine:
        events = [
            {"event_id": 1, "user_id": 1, "event_name": "a", "event_ts": "2026-09-30T12:00:00Z"}
        ]
        engine.ingest(pydantic_core.to_json(events), source="s")
        engine.flush()

        compactor = engine._compactor
        plan = compactor.plan()
        if plan is None:
            plan = CompactionPlan(
                input_paths=("nonexistent.parquet",),
                input_rows=1,
                input_bytes=100,
                snapshot_id=-999,
            )

        with pytest.raises(CompactionError, match="Compaction plan is stale"):
            compactor.run(plan)


def test_catalog_manager_locking_and_errors(tmp_path: Path) -> None:
    settings = EngineSettings.load(
        catalog={
            "uri": f"sqlite:///{tmp_path}/catalog.db",
            "warehouse_uri": f"file://{tmp_path}/warehouse",
        },
        ingestion={"dlq_dir": tmp_path / "dlq"},
        query={"duckdb_temp_dir": tmp_path / "duckdb-spill"},
    )
    from lakehouse_engine.catalog.manager import CatalogManager

    cat = CatalogManager(settings.catalog, settings.storage)
    cat.open()

    # Attempting to commit zero files raises CatalogCommitError
    with pytest.raises(CatalogCommitError, match="No files provided"):
        cat.commit_files([], flush_id="empty")

    cat.close()
