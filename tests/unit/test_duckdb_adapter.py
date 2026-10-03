from pathlib import Path

import pytest

from lakehouse_engine.config import QuerySettings
from lakehouse_engine.exceptions import UnsafeSqlError
from lakehouse_engine.query.duckdb_adapter import DuckDBSession


def test_duckdb_temp_dir_created_with_permissions(tmp_path: Path) -> None:
    temp_dir = tmp_path / "duckdb_spill_test"
    assert not temp_dir.exists()

    settings = QuerySettings(duckdb_temp_dir=temp_dir)
    with DuckDBSession(settings) as session:
        assert temp_dir.exists()
        assert temp_dir.is_dir()
        assert session._con is not None


def test_duckdb_sql_and_streaming(tmp_path: Path) -> None:
    settings = QuerySettings(duckdb_temp_dir=tmp_path / "spill")
    with DuckDBSession(settings) as session:
        rel = session.sql("SELECT $x::int as val", params={"x": 42})
        reader = rel.to_arrow_reader()
        table = reader.read_all()
        assert table["val"][0].as_py() == 42


def test_duckdb_unsafe_sql_rejection(tmp_path: Path) -> None:
    settings = QuerySettings(duckdb_temp_dir=tmp_path / "spill")
    with DuckDBSession(settings) as session, pytest.raises(UnsafeSqlError):
        session.sql("DROP TABLE events")
