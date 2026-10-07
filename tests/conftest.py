from collections.abc import Generator
from pathlib import Path

import pytest

from lakehouse_engine.config import EngineSettings
from lakehouse_engine.engine import LakehouseEngine


@pytest.fixture()
def tmp_warehouse(tmp_path: Path) -> Path:
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir(parents=True, exist_ok=True)
    return warehouse


@pytest.fixture()
def tmp_catalog_db(tmp_path: Path) -> str:
    return f"sqlite:///{tmp_path}/catalog.db"


@pytest.fixture()
def test_engine_settings(
    tmp_path: Path, tmp_warehouse: Path, tmp_catalog_db: str
) -> EngineSettings:
    dlq_dir = tmp_path / "dlq"
    duckdb_spill = tmp_path / "duckdb-spill"
    return EngineSettings.load(
        catalog={
            "uri": tmp_catalog_db,
            "warehouse_uri": f"file://{tmp_warehouse}",
        },
        ingestion={"dlq_dir": dlq_dir},
        query={"duckdb_temp_dir": duckdb_spill},
    )


@pytest.fixture()
def test_engine(
    test_engine_settings: EngineSettings,
) -> Generator[LakehouseEngine]:
    engine = LakehouseEngine(test_engine_settings)
    yield engine
    engine.close()
