from pathlib import Path

import pyarrow as pa
import pytest

from lakehouse_engine.config import IngestionSettings, QuerySettings
from lakehouse_engine.exceptions import OversizeBatchError, StreamConsumedError
from lakehouse_engine.ingestion.dlq import NdjsonDeadLetterSink
from lakehouse_engine.ingestion.validator import MicroBatchValidator
from lakehouse_engine.query.duckdb_adapter import DuckDBSession


def test_duckdb_session_stream_and_frame(tmp_path: Path) -> None:
    settings = QuerySettings(duckdb_temp_dir=tmp_path / "duckdb-spill")
    session = DuckDBSession(settings)
    batch = pa.RecordBatch.from_arrays([pa.array([1, 2], type=pa.int64())], names=["id"])
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    session.register_stream("test_stream", reader)
    with pytest.raises(StreamConsumedError, match="already been registered and consumed"):
        session.register_stream("test_stream", reader)


def test_duckdb_sql_stream_and_frame(tmp_path: Path) -> None:
    q_settings = QuerySettings(duckdb_temp_dir=tmp_path / "spill")
    session = DuckDBSession(q_settings)
    reader = session.sql_stream("SELECT 100 as num, 'hello' as str", batch_rows=10)
    batches = list(reader)
    assert len(batches) >= 1


def test_validator_isolation_path_and_oversize(tmp_path: Path) -> None:
    settings = IngestionSettings(
        dlq_dir=tmp_path / "dlq",
        max_batch_bytes=1024,
        max_batch_rows=10,
        max_line_bytes=256,
    )
    sink = NdjsonDeadLetterSink(settings)
    validator = MicroBatchValidator(settings, sink)
    with pytest.raises(OversizeBatchError):
        validator.validate(b"x" * 2000, source="test")
    raw_ndjson = b'{"event_id": 1}\n{"invalid_json": }\n'
    vbatch = validator.validate(raw_ndjson, source="test")
    assert vbatch.rejected >= 1
