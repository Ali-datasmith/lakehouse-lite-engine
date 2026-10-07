import pyarrow.fs as pafs
import pytest

from lakehouse_engine.engine import LakehouseEngine


@pytest.mark.contract
def test_ct01_ct02_fast_append_and_idempotency(
    test_engine: LakehouseEngine,
) -> None:
    event_raw = (
        b'{"event_id": 101, "user_id": 1, "event_name": "test", '
        b'"event_ts": "2026-09-30T12:00:00Z"}\n'
    )
    test_engine.ingest(event_raw, source="ct01", source_offset=1)
    commit1 = test_engine.flush()
    assert commit1 is not None
    assert commit1.added_files == 1
    commit2 = test_engine._catalog.commit_files([], flush_id=commit1.flush_id)
    assert commit2.replayed is True


@pytest.mark.contract
def test_ct03_compaction_replace_commit(
    test_engine: LakehouseEngine,
) -> None:
    event_raw = (
        b'{"event_id": 102, "user_id": 2, "event_name": "compact_test", '
        b'"event_ts": "2026-09-30T12:00:00Z"}\n'
    )
    test_engine.ingest(event_raw, source="ct03", source_offset=1)
    commit1 = test_engine.flush()
    assert commit1 is not None
    tbl = test_engine._catalog.current_table()
    data_files = [task.file.file_path for task in tbl.scan().plan_files()]
    assert len(data_files) > 0
    data_dir = test_engine._catalog.data_dir()
    new_file_path = f"{data_dir}/compact_output.parquet"
    _fs, rel_path = pafs.FileSystem.from_uri(new_file_path)
    old_fs, old_rel = pafs.FileSystem.from_uri(data_files[0])
    old_fs.copy_file(old_rel, rel_path)
    replace_res = test_engine._catalog.commit_replace(
        delete=data_files,
        add_paths=[new_file_path],
        flush_id="replace_flush_1",
    )
    assert replace_res.added_files == 1


@pytest.mark.contract
def test_ct04_ct05_polars_adapter_capability(
    test_engine: LakehouseEngine,
) -> None:
    event_raw = (
        b'{"event_id": 103, "user_id": 3, "event_name": "polars_test", '
        b'"event_ts": "2026-09-30T12:00:00Z"}\n'
    )
    test_engine.ingest(event_raw, source="ct04", source_offset=1)
    test_engine.flush()
    lf = test_engine.query.polars_lazy()
    df = lf.collect(engine="streaming")
    assert df.shape[0] >= 1


@pytest.mark.contract
def test_ct06_duckdb_stream_registration(
    test_engine: LakehouseEngine,
) -> None:
    event_raw = (
        b'{"event_id": 104, "user_id": 4, "event_name": "duckdb_test", '
        b'"event_ts": "2026-09-30T12:00:00Z"}\n'
    )
    test_engine.ingest(event_raw, source="ct06", source_offset=1)
    test_engine.flush()
    with test_engine.query.duckdb_session() as session:
        rel = session.sql("SELECT count(*) as cnt FROM events")
        assert rel.fetchall()[0][0] >= 1
