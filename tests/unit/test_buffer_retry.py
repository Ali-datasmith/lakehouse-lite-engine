from collections.abc import Sequence
from dataclasses import dataclass

import pyarrow as pa
import pytest

from lakehouse_engine.buffer.buffer import BufferPolicy, CompactionBuffer, FlushedFile
from lakehouse_engine.catalog.manager import CommitResult
from lakehouse_engine.governor import ResourceGovernor
from lakehouse_engine.ingestion.schema import EVENTS_ARROW_SCHEMA


@dataclass
class DummyWriter:
    def write(self, batches: Sequence[pa.RecordBatch], *, path: str) -> object:
        _ = path
        rows = sum(b.num_rows for b in batches)
        return type("WriteStats", (), {"rows": rows, "row_groups": 1, "file_bytes": 100})()


class DummyCommitter:
    def data_dir(self) -> str:
        return "test_data"

    def __init__(self, fail_first: bool = False) -> None:
        self.fail_first = fail_first
        self.attempts = 0
        self.committed_ids: list[str] = []

    def commit_files(self, files: Sequence[FlushedFile], *, flush_id: str) -> CommitResult:
        self.attempts += 1
        if self.fail_first and self.attempts == 1:
            raise RuntimeError("Simulated catalog commit failure")
        self.committed_ids.append(flush_id)
        return CommitResult(
            snapshot_id=101,
            flush_id=flush_id,
            attempts=self.attempts,
            added_files=len(files),
            added_rows=sum(f.rows for f in files),
            replayed=False,
        )


def test_buffer_flush_id_preserved_on_retry() -> None:
    committer = DummyCommitter(fail_first=True)
    gov = ResourceGovernor(pytest.importorskip("lakehouse_engine.config").RuntimeSettings())
    policy = BufferPolicy(max_bytes=10 * 1024 * 1024, max_rows=100)

    buf = CompactionBuffer(
        schema=EVENTS_ARROW_SCHEMA,
        policy=policy,
        writer=DummyWriter(),  # type: ignore[arg-type]
        committer=committer,
        governor=gov,
        data_dir="test_data",
    )

    batch = pa.RecordBatch.from_arrays(
        [
            pa.array([1], type=pa.int64()),
            pa.array([10], type=pa.int64()),
            pa.array(["evt"], type=pa.string()),
            pa.array([1000000], type=pa.timestamp("us", tz="UTC")),
            pa.array(["{}"], type=pa.string()),
        ],
        schema=EVENTS_ARROW_SCHEMA,
    )

    buf.append(batch)

    # First flush fails during committer
    with pytest.raises(RuntimeError, match="Simulated catalog commit failure"):
        buf.flush()

    assert len(buf.pending_files) == 1
    original_flush_id = buf.pending_files[0].flush_id

    # Replay commit via recover_pending
    res = buf.recover_pending()
    assert res is not None
    assert res.flush_id == original_flush_id
    assert committer.committed_ids == [original_flush_id]


def test_buffer_flush_with_pending_and_new_batches() -> None:
    committer = DummyCommitter(fail_first=True)
    gov = ResourceGovernor(pytest.importorskip("lakehouse_engine.config").RuntimeSettings())
    policy = BufferPolicy(max_bytes=10 * 1024 * 1024, max_rows=100)

    buf = CompactionBuffer(
        schema=EVENTS_ARROW_SCHEMA,
        policy=policy,
        writer=DummyWriter(),  # type: ignore[arg-type]
        committer=committer,
        governor=gov,
        data_dir="test_data",
    )

    batch1 = pa.RecordBatch.from_arrays(
        [
            pa.array([1], type=pa.int64()),
            pa.array([10], type=pa.int64()),
            pa.array(["evt1"], type=pa.string()),
            pa.array([1000000], type=pa.timestamp("us", tz="UTC")),
            pa.array(["{}"], type=pa.string()),
        ],
        schema=EVENTS_ARROW_SCHEMA,
    )

    buf.append(batch1)

    # First flush fails during commit phase
    with pytest.raises(RuntimeError, match="Simulated catalog commit failure"):
        buf.flush()

    assert len(buf.pending_files) == 1
    original_flush_id = buf.pending_files[0].flush_id

    # Append a new batch while pending files exist
    batch2 = pa.RecordBatch.from_arrays(
        [
            pa.array([2], type=pa.int64()),
            pa.array([20], type=pa.int64()),
            pa.array(["evt2"], type=pa.string()),
            pa.array([2000000], type=pa.timestamp("us", tz="UTC")),
            pa.array(["{}"], type=pa.string()),
        ],
        schema=EVENTS_ARROW_SCHEMA,
    )
    buf.append(batch2)

    # Calling flush() now should first commit pending files with original_flush_id,
    # then commit new batch under a new flush_id
    res = buf.flush()
    assert res is not None
    assert len(committer.committed_ids) == 2
    assert committer.committed_ids[0] == original_flush_id
    new_flush_id = committer.committed_ids[1]
    assert new_flush_id != original_flush_id
    assert res.flush_id == new_flush_id
    assert len(buf.pending_files) == 0
