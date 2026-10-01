# tests/unit/test_ingestion.py
import pydantic_core
import pytest

from lakehouse_engine.config import IngestionSettings
from lakehouse_engine.exceptions import OversizeBatchError
from lakehouse_engine.ingestion.dlq import NdjsonDeadLetterSink
from lakehouse_engine.ingestion.validator import MicroBatchValidator


def test_validator_fast_path(tmp_path):
    sink = NdjsonDeadLetterSink(IngestionSettings(dlq_dir=tmp_path / "dlq"))
    validator = MicroBatchValidator(IngestionSettings(), sink)

    event = {
        "event_id": 1,
        "user_id": 100,
        "event_name": "click",
        "event_ts": "2026-09-30T12:00:00Z",
        "payload": {"page": "/home"},
    }
    raw = pydantic_core.to_json([event])

    vbatch = validator.validate(raw, source="test_stream", source_offset=1)
    assert vbatch.accepted == 1
    assert vbatch.rejected == 0
    assert vbatch.batch is not None
    assert vbatch.batch.num_rows == 1


def test_validator_isolation_path(tmp_path):
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


def test_validator_oversize_batch(tmp_path):
    sink = NdjsonDeadLetterSink(IngestionSettings(dlq_dir=tmp_path / "dlq"))
    settings = IngestionSettings(max_batch_bytes=2048)
    validator = MicroBatchValidator(settings, sink)

    with pytest.raises(OversizeBatchError):
        validator.validate(b"x" * 3000, source="test_stream")
