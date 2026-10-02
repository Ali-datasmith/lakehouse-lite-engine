import os
import time
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pydantic_core
import pytest
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.types import LongType, NestedField

from lakehouse_engine.catalog.schema_guard import assert_compatible
from lakehouse_engine.config import EngineSettings, IngestionSettings
from lakehouse_engine.engine import LakehouseEngine
from lakehouse_engine.exceptions import CatalogCommitError, SchemaEvolutionError
from lakehouse_engine.governor import Mode, ResourceGovernor
from lakehouse_engine.ingestion.dlq import DeadLetter, NdjsonDeadLetterSink
from lakehouse_engine.query.polars_adapter import PolarsAdapter


def test_schema_guard_field_count_and_nullability_mismatch() -> None:
    iceberg_schema = IcebergSchema(
        NestedField(1, "event_id", LongType(), required=True),
    )
    arrow_schema = pa.schema(
        [
            pa.field("wrong_name", pa.int64(), nullable=True),
            pa.field("extra_field", pa.string(), nullable=True),
        ]
    )

    with pytest.raises(SchemaEvolutionError, match="incompatible with engine schema"):
        assert_compatible(iceberg_schema, arrow_schema)


def test_governor_helpers() -> None:
    gov = ResourceGovernor(pytest.importorskip("lakehouse_engine.config").RuntimeSettings())
    assert gov.arrow_allocated_bytes() >= 0
    res = gov.with_lease(Mode.QUERY, lambda: 42)
    assert res == 42


def test_polars_adapter_empty_files_scan() -> None:
    def _empty_plan(*_args: object, **_kwargs: object) -> list[object]:
        return []

    class FakeTable:
        def schema(self) -> object:
            from pyiceberg.io.pyarrow import pyarrow_to_schema

            from lakehouse_engine.ingestion.schema import EVENTS_ARROW_SCHEMA

            return pyarrow_to_schema(EVENTS_ARROW_SCHEMA)

        def scan(self, snapshot_id: int | None = None) -> object:
            _ = snapshot_id
            return type("Scan", (), {"plan_files": _empty_plan})()

    adapter = PolarsAdapter()
    lf = adapter.scan_iceberg_files(FakeTable())  # type: ignore[arg-type]
    df = lf.collect()
    assert len(df) == 0


def test_catalog_manager_idempotency_replay_and_replace_missing(tmp_path: Path) -> None:
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
            {"event_id": 1, "user_id": 100, "event_name": "a", "event_ts": "2026-09-30T12:00:00Z"}
        ]
        engine.ingest(pydantic_core.to_json(events), source="test_idempotent")
        commit1 = engine.flush()
        assert commit1 is not None

        # Replaying flush with same flush_id via catalog manager
        committer = engine._catalog
        res_replayed = committer._check_idempotency(commit1.flush_id)
        assert res_replayed is not None
        assert res_replayed.replayed is True

        # commit_replace with non-existent delete path raises CatalogCommitError
        with pytest.raises(CatalogCommitError, match="delete paths not found"):
            committer.commit_replace(
                delete=["nonexistent/file.parquet"],
                add_paths=[],
                flush_id="flush_missing",
            )


def test_dlq_retention_deletion(tmp_path: Path) -> None:
    settings = IngestionSettings(
        dlq_dir=tmp_path / "dlq",
        dlq_retention_days=1,
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
        raw="raw_dlq",
        engine_schema_version="1",
    )
    sink.write(dl)
    sink.flush()

    dlq_files = list((tmp_path / "dlq").glob("dlq-*.ndjson"))
    assert len(dlq_files) == 1

    # Modify mtime to simulate expired file
    old_mtime = time.time() - (3 * 86400)
    os.utime(dlq_files[0], (old_mtime, old_mtime))

    sink.enforce_retention_and_limits()
    assert not dlq_files[0].exists()
    sink.close()


def test_compaction_plan_insufficient_files(tmp_path: Path) -> None:
    settings = EngineSettings.load(
        catalog={
            "uri": f"sqlite:///{tmp_path}/catalog.db",
            "warehouse_uri": f"file://{tmp_path}/warehouse",
        },
        ingestion={"dlq_dir": tmp_path / "dlq"},
        query={"duckdb_temp_dir": tmp_path / "duckdb-spill"},
        compaction={"min_input_files": 100},
    )
    with LakehouseEngine(settings) as engine:
        events = [
            {"event_id": 1, "user_id": 100, "event_name": "a", "event_ts": "2026-09-30T12:00:00Z"}
        ]
        engine.ingest(pydantic_core.to_json(events), source="test")
        engine.flush()

        plan = engine._compactor.plan()
        assert plan is None
