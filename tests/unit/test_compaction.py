from pathlib import Path

import pydantic_core

from lakehouse_engine.config import EngineSettings
from lakehouse_engine.engine import LakehouseEngine


def test_compaction_execution_and_plan(tmp_path: Path) -> None:
    settings = EngineSettings.load(
        catalog={
            "uri": f"sqlite:///{tmp_path}/catalog.db",
            "warehouse_uri": f"file://{tmp_path}/warehouse",
        },
        ingestion={"dlq_dir": tmp_path / "dlq"},
        query={"duckdb_temp_dir": tmp_path / "duckdb-spill"},
        compaction={
            "min_input_files": 2,
            "small_file_threshold_bytes": 100 * 1024 * 1024,
            "target_file_bytes": 128 * 1024 * 1024,
        },
    )

    with LakehouseEngine(settings) as engine:
        total_accepted = 0
        # Create multiple small commits
        for i in range(3):
            events = [
                {
                    "event_id": i * 10 + j + 1,
                    "user_id": 100,
                    "event_name": "click",
                    "event_ts": "2026-09-30T12:00:00Z",
                    "payload": {"idx": j},
                }
                for j in range(5)
            ]
            raw = pydantic_core.to_json(events)
            vbatch = engine.ingest(raw, source=f"src_{i}")
            if vbatch:
                total_accepted += vbatch.accepted
            engine.flush()

        res = engine.compact()
        assert res is not None
        assert res.output_rows == total_accepted
        assert len(res.output_paths) == 1
