# src/lakehouse_engine/ingestion/validator.py
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal

import pyarrow as pa
import pydantic_core
from pydantic import ValidationError

from lakehouse_engine.config import SCHEMA_VERSION
from lakehouse_engine.exceptions import (
    DeadLetterThresholdExceeded,
    OversizeBatchError,
)
from lakehouse_engine.ingestion.dlq import DeadLetter, DeadLetterSink
from lakehouse_engine.ingestion.models import EVENT_ADAPTER, EVENT_LIST_ADAPTER, Event
from lakehouse_engine.ingestion.schema import EVENTS_ARROW_SCHEMA

if TYPE_CHECKING:
    from lakehouse_engine.config import IngestionSettings

DlqReason = Literal["validation", "schema_drift", "malformed_json", "oversize"]


@dataclass(frozen=True, slots=True)
class BatchContext:
    now_utc: datetime
    source: str
    source_offset: int | None


@dataclass(frozen=True, slots=True)
class ValidatedBatch:
    batch: pa.RecordBatch | None
    accepted: int
    rejected: int
    source: str
    source_offset: int | None
    raw_bytes: int


class MicroBatchValidator:
    def __init__(self, settings: "IngestionSettings", sink: DeadLetterSink) -> None:
        self._settings = settings
        self._sink = sink
        self._total_evaluated = 0
        self._total_rejected = 0

    def validate(
        self,
        raw: bytes | bytearray | memoryview,
        *,
        source: str,
        source_offset: int | None = None,
    ) -> ValidatedBatch:
        raw_bytes_len = len(raw)
        if raw_bytes_len > self._settings.max_batch_bytes:
            msg = (
                f"Batch size {raw_bytes_len} bytes exceeds max_batch_bytes "
                f"({self._settings.max_batch_bytes})"
            )
            raise OversizeBatchError(msg)

        raw_bytes = bytes(raw)
        stripped = raw_bytes.strip()
        if not stripped:
            return ValidatedBatch(
                batch=None,
                accepted=0,
                rejected=0,
                source=source,
                source_offset=source_offset,
                raw_bytes=raw_bytes_len,
            )

        is_json_array = stripped.startswith(b"[")
        lines: list[bytes] = []
        if is_json_array:
            payload = raw_bytes
        else:
            lines = [line for line in raw_bytes.splitlines() if line.strip()]
            if len(lines) > self._settings.max_batch_rows:
                msg = (
                    f"Line count {len(lines)} exceeds max_batch_rows "
                    f"({self._settings.max_batch_rows})"
                )
                raise OversizeBatchError(msg)
            payload = b"[" + b",".join(lines) + b"]"

        accepted_models: list[Event] = []
        rejected_records: list[DeadLetter] = []
        ctx = BatchContext(now_utc=datetime.now(UTC), source=source, source_offset=source_offset)

        # Fast path
        try:
            models = EVENT_LIST_ADAPTER.validate_json(payload)
            if len(models) > self._settings.max_batch_rows:
                msg = (
                    f"Parsed row count {len(models)} exceeds max_batch_rows "
                    f"({self._settings.max_batch_rows})"
                )
                raise OversizeBatchError(msg)
            accepted_models = models
        except ValidationError:
            # Isolation path
            items_bytes = self._parse_isolation_items(
                raw_bytes, is_json_array, lines, rejected_records, ctx
            )
            self._validate_isolation_items(items_bytes, accepted_models, rejected_records, ctx)

        for dl in rejected_records:
            self._sink.write(dl)
        if rejected_records:
            self._sink.flush()

        accepted_count = len(accepted_models)
        rejected_count = len(rejected_records)
        total_rows = accepted_count + rejected_count

        self._total_evaluated += total_rows
        self._total_rejected += rejected_count

        if self._total_evaluated >= self._settings.reject_ratio_min_sample:
            ratio = self._total_rejected / self._total_evaluated
            if ratio > self._settings.max_reject_ratio:
                self._sink.flush()
                msg = (
                    f"Dead letter threshold exceeded: reject ratio {ratio:.2f} "
                    f"> max_reject_ratio {self._settings.max_reject_ratio:.2f}"
                )
                raise DeadLetterThresholdExceeded(msg)

        batch = self._columnarize(accepted_models) if accepted_count > 0 else None

        return ValidatedBatch(
            batch=batch,
            accepted=accepted_count,
            rejected=rejected_count,
            source=source,
            source_offset=source_offset,
            raw_bytes=raw_bytes_len,
        )

    def _truncate_raw(self, item_bytes: bytes) -> str:
        return item_bytes.decode("utf-8", errors="replace")[: self._settings.dlq_raw_truncate_bytes]

    def _parse_isolation_items(
        self,
        raw_bytes: bytes,
        is_json_array: bool,
        lines: list[bytes],
        rejected_records: list[DeadLetter],
        ctx: BatchContext,
    ) -> list[bytes]:
        if not is_json_array:
            return lines

        try:
            parsed_array = pydantic_core.from_json(raw_bytes, allow_partial=False)
            if isinstance(parsed_array, list):
                if len(parsed_array) > self._settings.max_batch_rows:
                    msg = (
                        f"Parsed array count {len(parsed_array)} exceeds "
                        f"max_batch_rows ({self._settings.max_batch_rows})"
                    )
                    raise OversizeBatchError(msg)  # noqa: TRY301
                return [pydantic_core.to_json(elem) for elem in parsed_array]
        except OversizeBatchError:
            raise
        except (ValueError, TypeError, pydantic_core.ValidationError):
            raw_str = self._truncate_raw(raw_bytes)
            rejected_records.append(
                DeadLetter(
                    received_at=ctx.now_utc,
                    source=ctx.source,
                    line_no=1,
                    source_offset=ctx.source_offset,
                    reason="malformed_json",
                    errors=[{"type": "json_invalid", "msg": "Invalid JSON array"}],
                    raw=raw_str,
                    engine_schema_version=SCHEMA_VERSION,
                )
            )
        return []

    def _validate_isolation_items(
        self,
        items_bytes: list[bytes],
        accepted_models: list[Event],
        rejected_records: list[DeadLetter],
        ctx: BatchContext,
    ) -> None:
        for idx, item_bytes in enumerate(items_bytes, start=1):
            if len(item_bytes) > self._settings.max_line_bytes:
                raw_str = self._truncate_raw(item_bytes)
                rejected_records.append(
                    DeadLetter(
                        received_at=ctx.now_utc,
                        source=ctx.source,
                        line_no=idx,
                        source_offset=ctx.source_offset,
                        reason="oversize",
                        errors=[{"type": "line_oversize", "msg": "Line exceeds max_line_bytes"}],
                        raw=raw_str,
                        engine_schema_version=SCHEMA_VERSION,
                    )
                )
                continue

            try:
                m = EVENT_ADAPTER.validate_json(item_bytes)
                accepted_models.append(m)
            except ValidationError as exc:
                err_dicts = exc.errors(
                    include_url=False,
                    include_context=False,
                    include_input=False,
                )
                clean_errors: list[dict[str, Any]] = [dict(e) for e in err_dicts]
                reason_str: DlqReason = "validation"
                for e in clean_errors:
                    err_type = str(e.get("type", ""))
                    if err_type == "extra_forbidden":
                        reason_str = "schema_drift"
                        break
                    if "json" in err_type or err_type == "json_invalid":
                        reason_str = "malformed_json"

                raw_str = self._truncate_raw(item_bytes)
                rejected_records.append(
                    DeadLetter(
                        received_at=ctx.now_utc,
                        source=ctx.source,
                        line_no=idx,
                        source_offset=ctx.source_offset,
                        reason=reason_str,
                        errors=clean_errors,
                        raw=raw_str,
                        engine_schema_version=SCHEMA_VERSION,
                    )
                )
            except (ValueError, TypeError) as exc:
                raw_str = self._truncate_raw(item_bytes)
                rejected_records.append(
                    DeadLetter(
                        received_at=ctx.now_utc,
                        source=ctx.source,
                        line_no=idx,
                        source_offset=ctx.source_offset,
                        reason="malformed_json",
                        errors=[{"type": "json_invalid", "msg": str(exc)}],
                        raw=raw_str,
                        engine_schema_version=SCHEMA_VERSION,
                    )
                )

    def _columnarize(self, accepted_models: list[Event]) -> pa.RecordBatch:
        event_ids = [m.event_id for m in accepted_models]
        user_ids = [m.user_id for m in accepted_models]
        event_names = [m.event_name for m in accepted_models]
        event_tss = [m.event_ts for m in accepted_models]
        payloads = [
            pydantic_core.to_json(m.payload).decode("utf-8") if m.payload is not None else None
            for m in accepted_models
        ]

        del accepted_models

        return pa.RecordBatch.from_arrays(
            [
                pa.array(event_ids, type=pa.int64()),
                pa.array(user_ids, type=pa.int64()),
                pa.array(event_names, type=pa.string()),
                pa.array(event_tss, type=pa.timestamp("us", tz="UTC")),
                pa.array(payloads, type=pa.string()),
            ],
            schema=EVENTS_ARROW_SCHEMA,
        )
