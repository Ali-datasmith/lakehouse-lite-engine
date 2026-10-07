# tests/integration/test_pipeline.py
import pydantic_core

from lakehouse_engine.config import EngineSettings
from lakehouse_engine.engine import LakehouseEngine


def test_full_pipeline_ingest_query_compact(tmp_path) -> None:
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir(parents=True, exist_ok=True)
    db = tmp_path / "catalog.db"

    # Set polars_strategy="parquet_files" to fallback cleanly when Iceberg field IDs are absent
    settings = EngineSettings.load(
        catalog={
            "uri": f"sqlite:///{db}",
            "warehouse_uri": f"file://{warehouse}",
        },
        ingestion={
            "dlq_dir": tmp_path / "dlq",
        },
        query={
            "duckdb_temp_dir": tmp_path / "duckdb-spill",
            "polars_strategy": "parquet_files",
        },
    )

    with LakehouseEngine(settings) as engine:
        events = [
            {
                "event_id": i,
                "user_id": i % 10,
                "event_name": "click" if i % 2 == 0 else "purchase",
                "event_ts": "2026-09-30T12:00:00Z",
                "payload": {"item_id": i},
            }
            for i in range(1, 101)
        ]
        raw = pydantic_core.to_json(events)

        vbatch = engine.ingest(raw, source="integration_test", source_offset=1)
        assert vbatch is not None
        assert vbatch.accepted == 100
        assert vbatch.rejected == 0

        commit = engine.flush()
        assert commit is not None
        assert commit.added_rows == 100

        # Query Polars (governed)
        df = engine.query.collect_polars()
        assert len(df) == 100

        # Query DuckDB
        session = engine.query.duckdb_session()
        rel = session.sql("SELECT COUNT(*) as total FROM events")
        batch = next(rel.to_arrow_reader())
        assert batch["total"][0].as_py() == 100
