# src/lakehouse_engine/buffer/buffer.py
import threading
import time
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Protocol

import pyarrow as pa

from lakehouse_engine.exceptions import (
    BufferOverflowError,
    SchemaMismatchError,
)
from lakehouse_engine.governor import Mode

if TYPE_CHECKING:
    from lakehouse_engine.buffer.writer import ParquetFlushWriter
    from lakehouse_engine.catalog.manager import CommitResult
    from lakehouse_engine.governor import ResourceGovernor


class FlushReason(StrEnum):
    ROW_LIMIT = "row_limit"
    BYTE_LIMIT = "byte_limit"
    AGE_LIMIT = "age_limit"
    MEMORY_PRESSURE = "memory_pressure"
    EXPLICIT = "explicit"
    CLOSE = "close"


@dataclass(frozen=True, slots=True)
class BufferPolicy:
    max_bytes: int = 128 * 1024 * 1024
    max_rows: int = 500_000
    max_age_seconds: float = 60.0


@dataclass(frozen=True, slots=True)
class FlushedFile:
    path: str
    flush_id: str
    rows: int
    arrow_bytes: int
    file_bytes: int
    row_groups: int
    reason: FlushReason


class FileCommitter(Protocol):
    def commit_files(self, files: Sequence[FlushedFile], *, flush_id: str) -> "CommitResult": ...


class CompactionBuffer:
    def __init__(
        self,
        *,
        schema: pa.Schema,
        policy: BufferPolicy,
        writer: "ParquetFlushWriter",
        committer: FileCommitter,
        governor: "ResourceGovernor",
        data_dir: str,
    ) -> None:
        self._schema = schema
        self._policy = policy
        self._writer = writer
        self._committer = committer
        self._governor = governor
        self._data_dir = data_dir

        self._lock = threading.RLock()
        self._batches: list[pa.RecordBatch] = []
        self._total_rows = 0
        self._total_bytes = 0
        self._oldest_batch_time: float | None = None
        self._pending_files: list[FlushedFile] = []
        self._closed = False

    @property
    def rows(self) -> int:
        with self._lock:
            return self._total_rows

    @property
    def bytes(self) -> int:
        with self._lock:
            return self._total_bytes

    @property
    def pending_files(self) -> tuple[FlushedFile, ...]:
        with self._lock:
            return tuple(self._pending_files)

    def append(self, batch: pa.RecordBatch) -> "CommitResult | None":
        with self._lock:
            if self._closed:
                raise RuntimeError("CompactionBuffer is closed.")

            if not batch.schema.equals(self._schema, check_metadata=False):
                raise SchemaMismatchError("RecordBatch schema does not match buffer schema.")

            incoming_bytes = batch.get_total_buffer_size()
            if incoming_bytes > self._policy.max_bytes:
                msg = (
                    f"Batch size {incoming_bytes} exceeds buffer policy max_bytes "
                    f"{self._policy.max_bytes}"
                )
                raise BufferOverflowError(msg)

            # Pre-flush rule
            commit_result: CommitResult | None = None
            if (
                self._total_bytes + incoming_bytes > self._policy.max_bytes
                or self._total_rows + batch.num_rows > self._policy.max_rows
            ):
                reason = (
                    FlushReason.BYTE_LIMIT
                    if self._total_bytes + incoming_bytes > self._policy.max_bytes
                    else FlushReason.ROW_LIMIT
                )
                commit_result = self.flush(reason=reason)

            self._governor.check(incoming_bytes=incoming_bytes)

            self._batches.append(batch)
            self._total_rows += batch.num_rows
            self._total_bytes += incoming_bytes
            if self._oldest_batch_time is None:
                self._oldest_batch_time = time.monotonic()

            # Post-append checks
            now = time.monotonic()
            age = now - self._oldest_batch_time if self._oldest_batch_time is not None else 0.0

            if self._total_bytes >= self._policy.max_bytes:
                commit_result = self.flush(reason=FlushReason.BYTE_LIMIT)
            elif self._total_rows >= self._policy.max_rows:
                commit_result = self.flush(reason=FlushReason.ROW_LIMIT)
            elif age >= self._policy.max_age_seconds:
                commit_result = self.flush(reason=FlushReason.AGE_LIMIT)
            elif self._governor.over_soft_limit():
                commit_result = self.flush(reason=FlushReason.MEMORY_PRESSURE)

            return commit_result

    def flush(self, reason: FlushReason = FlushReason.EXPLICIT) -> "CommitResult | None":
        with self._lock:
            if not self._batches and not self._pending_files:
                return None

            with self._governor.lease(Mode.FLUSH):
                flush_id = uuid.uuid4().hex
                if self._batches:
                    rel_path = f"{self._data_dir}/{flush_id}.parquet"
                    batches_to_write = list(self._batches)
                    arrow_bytes = self._total_bytes

                    # Write parquet file
                    stats = self._writer.write(batches_to_write, path=rel_path)

                    flushed_file = FlushedFile(
                        path=rel_path,
                        flush_id=flush_id,
                        rows=stats.rows,
                        arrow_bytes=arrow_bytes,
                        file_bytes=stats.file_bytes,
                        row_groups=stats.row_groups,
                        reason=reason,
                    )
                    self._pending_files.append(flushed_file)

                    # Release RAM before commit
                    self._batches.clear()
                    self._total_rows = 0
                    self._total_bytes = 0
                    self._oldest_batch_time = None
                    del batches_to_write
                    self._governor.release_memory()

                # Commit phase
                result = self._committer.commit_files(self._pending_files, flush_id=flush_id)
                self._pending_files.clear()
                return result

    def close(self) -> "CommitResult | None":
        with self._lock:
            if self._closed:
                return None
            result = self.flush(reason=FlushReason.CLOSE)
            self._closed = True
            return result
