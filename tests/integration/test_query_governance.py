from pathlib import Path

import pydantic_core

from lakehouse_engine.catalog.storage import resolve_filesystem
from lakehouse_engine.config import EngineSettings, StorageSettings
from lakehouse_engine.engine import LakehouseEngine


def test_query_governance_apis(tmp_path: Path) -> None:
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
            {
                "event_id": 1,
                "user_id": 100,
                "event_name": "view",
                "event_ts": "2026-09-30T12:00:00Z",
                "payload": {"item": "A"},
            }
        ]
        engine.ingest(pydantic_core.to_json(events), source="test")
        engine.flush()

        # Test Polars dataframe collection
        df = engine.query.collect_polars()
        assert len(df) == 1

        # Test DuckDB query
        res = engine.query.query_duckdb("SELECT event_name FROM events")
        assert res.column("event_name")[0].as_py() == "view"

        # Test Arrow reader
        reader = engine.query.to_arrow_reader()
        table = reader.read_all()
        assert table.num_rows == 1

        # Status check
        status = engine.status()
        assert status["closed"] is False


def test_storage_s3_resolution() -> None:
    s_settings = StorageSettings(
        s3_endpoint="http://localhost:9000",
        s3_region="us-east-1",
        s3_access_key_id="minio",  # type: ignore[arg-type]
        s3_secret_access_key="minio123",  # type: ignore[arg-type] # noqa: S106
    )
    _fs, rel_path = resolve_filesystem("s3://bucket/path/file.parquet", settings=s_settings)
    assert rel_path == "bucket/path/file.parquet"
