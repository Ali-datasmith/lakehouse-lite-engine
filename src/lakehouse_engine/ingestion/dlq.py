# src/lakehouse_engine/ingestion/dlq.py
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Protocol

from pydantic import BaseModel, ConfigDict

from lakehouse_engine.exceptions import DeadLetterWriteError

if TYPE_CHECKING:
    from lakehouse_engine.config import IngestionSettings

logger = logging.getLogger(__name__)


class DeadLetter(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    received_at: datetime  # UTC, tz-aware
    source: str
    line_no: int | None
    source_offset: int | None
    reason: Literal["validation", "schema_drift", "malformed_json", "oversize"]
    errors: list[dict[str, object]]
    raw: str
    engine_schema_version: str


class DeadLetterSink(Protocol):
    def write(self, record: DeadLetter) -> None: ...

    def flush(self) -> None: ...

    def close(self) -> None: ...


class NdjsonDeadLetterSink:
    """Appends one JSON object per line to <dlq_dir>/dlq-<YYYYMMDD>-<seq:05d>.ndjson.

    In-memory staging <= 1 MB; rotates at dlq_rotate_bytes; fsync on flush()/close().
    dlq_dir is created with explicit chmod 0o700. Paths are resolved and MUST stay under dlq_dir.
    Enforces retention days and maximum total disk byte limits.
    """

    def __init__(self, settings: "IngestionSettings") -> None:
        self._settings = settings
        self._dlq_dir = settings.dlq_dir.resolve()
        try:
            self._dlq_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
            self._dlq_dir.chmod(0o700)
        except Exception as exc:
            raise DeadLetterWriteError(
                f"Failed to create DLQ directory {self._dlq_dir}: {exc}"
            ) from exc

        self._seq = 0
        self._buffer: list[bytes] = []
        self._buffer_bytes = 0
        self._current_file: Path | None = None
        self._current_file_bytes = 0

    def _get_file_path(self, today_str: str) -> Path:
        while True:
            file_name = f"dlq-{today_str}-{self._seq:05d}.ndjson"
            target = (self._dlq_dir / file_name).resolve()
            try:
                target.relative_to(self._dlq_dir)
            except ValueError as exc:
                raise DeadLetterWriteError(
                    f"Path traversal detected for target path: {target}"
                ) from exc

            if not target.exists() or target.stat().st_size < self._settings.dlq_rotate_bytes:
                return target
            self._seq += 1

    def enforce_retention_and_limits(self) -> None:
        """Deletes files older than retention days and enforces max total bytes usage."""
        try:
            files = sorted(
                [f for f in self._dlq_dir.glob("dlq-*.ndjson") if f.is_file()],
                key=lambda p: p.stat().st_mtime,
            )
            now = datetime.now(UTC)
            retention_cutoff = now - timedelta(days=self._settings.dlq_retention_days)

            remaining_files: list[tuple[Path, int]] = []
            for f in files:
                mtime = datetime.fromtimestamp(f.stat().st_mtime, tz=UTC)
                if mtime < retention_cutoff:
                    logger.info("Deleting expired DLQ file: %s", f)
                    f.unlink(missing_ok=True)
                else:
                    remaining_files.append((f, f.stat().st_size))

            total_bytes = sum(sz for _, sz in remaining_files)
            max_bytes = self._settings.dlq_max_total_bytes

            while total_bytes > max_bytes and remaining_files:
                oldest_file, size = remaining_files.pop(0)
                logger.info(
                    "Deleting oldest DLQ file to enforce max total bytes limit (%d > %d): %s",
                    total_bytes,
                    max_bytes,
                    oldest_file,
                )
                oldest_file.unlink(missing_ok=True)
                total_bytes -= size
        except Exception as exc:
            logger.error("Failed to enforce DLQ retention limits: %s", exc, exc_info=True)

    def write(self, record: DeadLetter) -> None:
        try:
            line_bytes = record.model_dump_json().encode("utf-8") + b"\n"
            self._buffer.append(line_bytes)
            self._buffer_bytes += len(line_bytes)

            if self._buffer_bytes >= 1_024 * 1_024:  # 1 MB staging
                self.flush()
        except Exception as exc:
            if isinstance(exc, DeadLetterWriteError):
                raise
            raise DeadLetterWriteError(f"Failed to write DLQ record: {exc}") from exc

    def flush(self) -> None:
        if not self._buffer:
            return

        today_str = datetime.now(UTC).strftime("%Y%m%d")
        file_path = self._get_file_path(today_str)

        try:
            with file_path.open("ab") as f:
                for chunk in self._buffer:
                    f.write(chunk)
                f.flush()
                import os

                os.fsync(f.fileno())

            self._buffer.clear()
            self._buffer_bytes = 0

            if file_path.exists() and file_path.stat().st_size >= self._settings.dlq_rotate_bytes:
                self._seq += 1

            self.enforce_retention_and_limits()
        except Exception as exc:
            raise DeadLetterWriteError(f"Failed to flush DLQ to {file_path}: {exc}") from exc

    def close(self) -> None:
        self.flush()
