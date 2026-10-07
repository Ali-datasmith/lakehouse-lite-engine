from pathlib import Path

import pytest

from lakehouse_engine.config import IngestionSettings
from lakehouse_engine.exceptions import OversizeBatchError
from lakehouse_engine.ingestion.dlq import NdjsonDeadLetterSink
from lakehouse_engine.ingestion.validator import MicroBatchValidator


def test_validator_fast_path(tmp_path: Path) -> None:
    sink = NdjsonDeadLetterSink(IngestionSettings(dlq_dir=tmp_path / "dlq"))
    validator = MicroBatchValidator(IngestionSettings(), sink)
    valid_events = (
        b'[{"event_id": 1, "user_id": 100, "event_name": "click", '
        b'"event_ts": "2026-09-30T12:00:00Z"}]'
    )
    vbatch = validator.validate(valid_events, source="test_fast")
    assert vbatch.accepted == 1
    assert vbatch.rejected == 0


def test_validator_oversize_batch(tmp_path: Path) -> None:
    settings = IngestionSettings(dlq_dir=tmp_path / "dlq", max_batch_bytes=1024)
    sink = NdjsonDeadLetterSink(settings)
    validator = MicroBatchValidator(settings, sink)
    with pytest.raises(OversizeBatchError):
        validator.validate(b"x" * 2048, source="test_oversize")


def test_validator_isolation_path(tmp_path: Path) -> None:
    sink = NdjsonDeadLetterSink(IngestionSettings(dlq_dir=tmp_path / "dlq"))
    validator = MicroBatchValidator(IngestionSettings(), sink)
    valid_event = (
        '{"event_id": 1, "user_id": 100, "event_name": "click", "event_ts": "2026-09-30T12:00:00Z"}'
    )
    invalid_event = '{"event_id": "NOT_INT", "user_id": 100}'
    ndjson = f"{valid_event}\n{invalid_event}".encode()
    vbatch = validator.validate(ndjson, source="test_stream", source_offset=2)
    assert vbatch.accepted == 1
    assert vbatch.rejected == 1
