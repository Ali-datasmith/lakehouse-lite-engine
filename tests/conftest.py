# tests/conftest.py
import pytest

from lakehouse_engine.config import EngineSettings
from lakehouse_engine.engine import LakehouseEngine


@pytest.fixture
def tmp_warehouse(tmp_path):
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir(parents=True, exist_ok=True)
    return str(warehouse)


@pytest.fixture
def tmp_catalog_db(tmp_path):
    db = tmp_path / "catalog.db"
    return f"sqlite:///{db}"


@pytest.fixture
def test_engine_settings(tmp_path, tmp_warehouse, tmp_catalog_db) -> None:
    dlq_dir = tmp_path / "dlq"
    duckdb_spill = tmp_path / "duckdb-spill"

    return EngineSettings.load(
        catalog={
            "uri": tmp_catalog_db,
            "warehouse_uri": f"file://{tmp_warehouse}",
        },
        ingestion={
            "dlq_dir": dlq_dir,
        },
        query={
            "duckdb_temp_dir": duckdb_spill,
        },
    )


@pytest.fixture
def test_engine(test_engine_settings) -> None:
    with LakehouseEngine(test_engine_settings) as engine:
        yield engine
