from pathlib import Path

import pydantic_core

from lakehouse_engine.config import EngineSettings
from lakehouse_engine.engine import LakehouseEngine


def test_readme_quickstart_execution(tmp_path: Path) -> None:
    base = tmp_path / ".lakehouse-lite"
    base.mkdir(parents=True, exist_ok=True)

    settings = EngineSettings.load(
        catalog={
            "uri": f"sqlite:///{(base / 'catalog.db').as_posix()}",
            "warehouse_uri": (base / "warehouse").resolve().as_uri(),
        },
        ingestion={
            "dlq_dir": base / "dlq",
        },
        query={
            "duckdb_temp_dir": base / "duckdb-spill",
        },
    )

    with LakehouseEngine(settings) as engine:
        events = [
            {
                "event_id": 1,
                "user_id": 100,
                "event_name": "purchase",
                "event_ts": "2026-09-30T12:00:00Z",
                "payload": {"item_id": 42, "amount": 99.99},
            }
        ]
        raw_bytes = pydantic_core.to_json(events)

        vbatch = engine.ingest(raw_bytes, source="quickstart_stream", source_offset=1)
        assert vbatch is not None
        assert vbatch.accepted == 1

        commit = engine.flush()
        assert commit is not None
        assert commit.added_rows == 1

        df = engine.query.collect_polars()
        assert len(df) == 1

        tbl = engine.query.query_duckdb(
            "SELECT event_name, COUNT(*) as cnt FROM events GROUP BY event_name"
        )
        assert len(tbl) == 1
