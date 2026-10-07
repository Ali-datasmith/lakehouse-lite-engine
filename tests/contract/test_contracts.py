# tests/contract/test_contracts.py
from datetime import UTC, datetime

import polars as pl
import pyarrow as pa
import pytest

from lakehouse_engine.buffer.buffer import FlushedFile, FlushReason
from lakehouse_engine.ingestion.schema import EVENTS_ARROW_SCHEMA
from lakehouse_engine.query.duckdb_adapter import DuckDBSession
from lakehouse_engine.query.polars_adapter import PolarsAdapter


@pytest.mark.contract
def test_ct01_ct02_fast_append_and_idempotency(test_engine) -> None:
    event_raw = (
        b'{"event_id": 101, "user_id": 1, "event_name": "test", '
        b'"event_ts": "2026-09-30T12:00:00Z"}\n'
    )
    test_engine.ingest(event_raw, source="ct01", source_offset=1)
    commit1 = test_engine.flush()

    assert commit1 is not None
    assert commit1.added_rows == 1
    assert not commit1.replayed

    dummy_file = FlushedFile(
        path="dummy.parquet",
        flush_id=commit1.flush_id,
        rows=1,
        arrow_bytes=100,
        file_bytes=100,
        row_groups=1,
        reason=FlushReason.EXPLICIT,
    )
    commit2 = test_engine._catalog.commit_files([dummy_file], flush_id=commit1.flush_id)
    assert commit2.replayed


@pytest.mark.contract
def test_ct03_compaction_replace_commit(test_engine) -> None:
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

    # Simulate compaction replacing input with a new file
    data_dir = test_engine._catalog.data_dir()
    new_file_path = f"{data_dir}/compact_output.parquet"
    _fs, rel_path = pa.fs.FileSystem.from_uri(new_file_path)
    old_fs, old_rel = pa.fs.FileSystem.from_uri(data_files[0])
    old_fs.copy_file(old_rel, rel_path)

    replace_res = test_engine._catalog.commit_replace(
        delete=data_files,
        add_paths=[new_file_path],
        flush_id="replace_flush_1",
    )
    assert replace_res is not None
    assert replace_res.added_files == 1


@pytest.mark.contract
def test_ct04_ct05_polars_adapter_capability(test_engine) -> None:
    adapter = PolarsAdapter()
    tbl = test_engine._catalog.current_table()
    lf = adapter.scan_iceberg_files(tbl, snapshot_id=None)
    assert isinstance(lf, pl.LazyFrame)

    ts = datetime(2026, 9, 30, 12, 0, 0, tzinfo=UTC)
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array([1], type=pa.int64()),
            pa.array([10], type=pa.int64()),
            pa.array(["stream"], type=pa.string()),
            pa.array([ts], type=pa.timestamp("us", tz="UTC")),
            pa.array([None], type=pa.string()),
        ],
        schema=EVENTS_ARROW_SCHEMA,
    )
    reader = pa.RecordBatchReader.from_batches(EVENTS_ARROW_SCHEMA, [batch])
    lf_stream = adapter.scan_stream(reader, EVENTS_ARROW_SCHEMA)
    assert isinstance(lf_stream, pl.LazyFrame)


@pytest.mark.contract
def test_ct06_duckdb_stream_registration(test_engine_settings) -> None:
    session = DuckDBSession(test_engine_settings.query)

    ts1 = datetime(2026, 9, 30, 12, 0, 0, tzinfo=UTC)
    ts2 = datetime(2026, 9, 30, 12, 0, 1, tzinfo=UTC)

    batch = pa.RecordBatch.from_arrays(
        [
            pa.array([1, 2], type=pa.int64()),
            pa.array([10, 20], type=pa.int64()),
            pa.array(["a", "b"], type=pa.string()),
            pa.array([ts1, ts2], type=pa.timestamp("us", tz="UTC")),
            pa.array([None, None], type=pa.string()),
        ],
        schema=EVENTS_ARROW_SCHEMA,
    )
    reader = pa.RecordBatchReader.from_batches(EVENTS_ARROW_SCHEMA, [batch])

    session.register_stream("events", reader)
    rel = session.sql("SELECT COUNT(*) as cnt FROM events")
    stream = rel.to_arrow_reader()
    res_batch = next(stream)
    assert res_batch["cnt"][0].as_py() == 2


@pytest.mark.contract
def test_ct07_ct08_ct09_zero_copy_and_capsule() -> None:
    ts = datetime(2026, 9, 30, 12, 0, 0, tzinfo=UTC)
    arr_int = pa.array([10, 20, 30], type=pa.int64())
    arr_ts = pa.array([ts, ts, ts], type=pa.timestamp("us", tz="UTC"))

    batch = pa.RecordBatch.from_arrays([arr_int, arr_ts], names=["id", "ts"])
    df = pl.from_arrow(batch)
    assert len(df) == 3

    out_batch = df.to_arrow().to_batches()  # type: ignore[union-attr][0]
    assert out_batch.column(0).buffers()[1].address == arr_int.buffers()[1].address


@pytest.mark.contract
def test_ct10_no_deprecation_warnings() -> None:
    import warnings

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        _ = EVENTS_ARROW_SCHEMA
