from pathlib import Path

import pyarrow as pa
import pytest

from lakehouse_engine.config import EngineSettings
from lakehouse_engine.engine import LakehouseEngine
from lakehouse_engine.exceptions import SchemaMismatchError
from lakehouse_engine.ingestion.schema import EVENTS_ARROW_SCHEMA


def test_engine_append_arrow_batch_public_api(tmp_path: Path) -> None:
    settings = EngineSettings.load(
        catalog={
            "uri": f"sqlite:///{tmp_path}/cat.db",
            "warehouse_uri": f"file://{tmp_path}/wh",
        },
        ingestion={"dlq_dir": tmp_path / "dlq"},
        query={"duckdb_temp_dir": tmp_path / "spill"},
    )

    with LakehouseEngine(settings) as engine:
        batch = pa.RecordBatch.from_arrays(
            [
                pa.array([1], type=pa.int64()),
                pa.array([10], type=pa.int64()),
                pa.array(["purchase"], type=pa.string()),
                pa.array([1000000], type=pa.timestamp("us", tz="UTC")),
                pa.array(["{}"], type=pa.string()),
            ],
            schema=EVENTS_ARROW_SCHEMA,
        )
        res = engine.append_arrow_batch(batch)
        assert res is None or hasattr(res, "snapshot_id")


def test_engine_closed_rejects_append(tmp_path: Path) -> None:
    settings = EngineSettings.load(
        catalog={
            "uri": f"sqlite:///{tmp_path}/cat.db",
            "warehouse_uri": f"file://{tmp_path}/wh",
        },
        ingestion={"dlq_dir": tmp_path / "dlq"},
        query={"duckdb_temp_dir": tmp_path / "spill"},
    )
    engine = LakehouseEngine(settings)
    engine.close()

    batch = pa.RecordBatch.from_arrays(
        [
            pa.array([1], type=pa.int64()),
            pa.array([10], type=pa.int64()),
            pa.array(["purchase"], type=pa.string()),
            pa.array([1000000], type=pa.timestamp("us", tz="UTC")),
            pa.array(["{}"], type=pa.string()),
        ],
        schema=EVENTS_ARROW_SCHEMA,
    )
    with pytest.raises(RuntimeError, match="LakehouseEngine is closed"):
        engine.append_arrow_batch(batch)


def test_engine_schema_mismatch_rejected(tmp_path: Path) -> None:
    settings = EngineSettings.load(
        catalog={
            "uri": f"sqlite:///{tmp_path}/cat.db",
            "warehouse_uri": f"file://{tmp_path}/wh",
        },
        ingestion={"dlq_dir": tmp_path / "dlq"},
        query={"duckdb_temp_dir": tmp_path / "spill"},
    )
    invalid_schema = pa.schema([("wrong_col", pa.int64())])  # type: ignore[arg-type]
    invalid_batch = pa.RecordBatch.from_arrays([pa.array([1])], schema=invalid_schema)

    with LakehouseEngine(settings) as engine, pytest.raises(SchemaMismatchError):
        engine.append_arrow_batch(invalid_batch)
