# tests/unit/test_buffer.py
from dataclasses import dataclass
from datetime import UTC, datetime

import pyarrow as pa
import pytest

from lakehouse_engine.buffer.buffer import BufferPolicy, CompactionBuffer
from lakehouse_engine.buffer.writer import ParquetFlushWriter
from lakehouse_engine.exceptions import SchemaMismatchError
from lakehouse_engine.governor import ResourceGovernor
from lakehouse_engine.ingestion.schema import EVENTS_ARROW_SCHEMA


@dataclass(frozen=True, slots=True)
class DummyResult:
    snapshot_id: int
    flush_id: str
    attempts: int
    added_files: int
    added_rows: int
    replayed: bool


class DummyCommitter:
    def __init__(self):
        self.committed = []

    def data_dir(self) -> str:
        return "test_data"

    def data_dir(self) -> str:
        return "test_data"

    def commit_files(self, files, *, flush_id):
        self.committed.extend(files)
        return DummyResult(
            snapshot_id=1,
            flush_id=flush_id,
            attempts=1,
            added_files=len(files),
            added_rows=sum(f.rows for f in files),
            replayed=False,
        )


def test_buffer_append_and_flush(tmp_path, test_engine_settings) -> None:
    governor = ResourceGovernor(test_engine_settings.runtime)
    writer = ParquetFlushWriter(
        test_engine_settings.buffer,
        EVENTS_ARROW_SCHEMA,
        pa.fs.LocalFileSystem(),
    )
    committer = DummyCommitter()
    policy = BufferPolicy(max_bytes=1000, max_rows=10)

    buffer = CompactionBuffer(
        schema=EVENTS_ARROW_SCHEMA,
        policy=policy,
        writer=writer,
        committer=committer,
        governor=governor,
        data_dir=str(tmp_path),
    )

    ts = datetime(2026, 9, 30, 12, 0, 0, tzinfo=UTC)

    batch = pa.RecordBatch.from_arrays(
        [
            pa.array([1], type=pa.int64()),
            pa.array([10], type=pa.int64()),
            pa.array(["event"], type=pa.string()),
            pa.array([ts], type=pa.timestamp("us", tz="UTC")),
            pa.array([None], type=pa.string()),
        ],
        schema=EVENTS_ARROW_SCHEMA,
    )

    res = buffer.append(batch)
    assert res is None
    assert buffer.rows == 1

    commit_res = buffer.flush()
    assert commit_res is not None
    assert commit_res.added_rows == 1
    assert buffer.rows == 0


def test_buffer_schema_mismatch(tmp_path, test_engine_settings) -> None:
    governor = ResourceGovernor(test_engine_settings.runtime)
    writer = ParquetFlushWriter(
        test_engine_settings.buffer, EVENTS_ARROW_SCHEMA, pa.fs.LocalFileSystem()
    )
    buffer = CompactionBuffer(
        schema=EVENTS_ARROW_SCHEMA,
        policy=BufferPolicy(),
        writer=writer,
        committer=DummyCommitter(),
        governor=governor,
        data_dir=str(tmp_path),
    )

    bad_schema = pa.schema([pa.field("col", pa.int64())])  # type: ignore[arg-type]
    bad_batch = pa.RecordBatch.from_arrays([pa.array([1])], schema=bad_schema)

    with pytest.raises(SchemaMismatchError):
        buffer.append(bad_batch)
