from pathlib import Path

import pyarrow as pa
import pytest

from lakehouse_engine.config import IngestionSettings, QuerySettings
from lakehouse_engine.exceptions import OversizeBatchError, StreamConsumedError, UnsafeSqlError
from lakehouse_engine.ingestion.dlq import NdjsonDeadLetterSink
from lakehouse_engine.ingestion.validator import MicroBatchValidator
from lakehouse_engine.query.duckdb_adapter import DuckDBSession
from lakehouse_engine.query.polars_adapter import PolarsAdapter


def test_duckdb_session_stream_and_frame(tmp_path: Path) -> None:
    settings = QuerySettings(duckdb_temp_dir=tmp_path / "duckdb-spill")
    session = DuckDBSession(settings)

    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2], type=pa.int64())],
        names=["id"],
    )
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])

    # Register stream
    session.register_stream("test_stream", reader)

    # Scanning second time should raise StreamConsumedError
    with pytest.raises(StreamConsumedError, match="already been registered and consumed"):
        session.register_stream("test_stream", reader)

    # Invalid relation name
    with pytest.raises(UnsafeSqlError, match="Invalid relation name"):
        session.register_stream("invalid-name!", reader)

    session.close()


def test_validator_isolation_path_and_oversize(tmp_path: Path) -> None:
    settings = IngestionSettings(
        dlq_dir=tmp_path / "dlq",
        max_batch_bytes=1024,
        max_batch_rows=10,
        max_line_bytes=256,
    )
    sink = NdjsonDeadLetterSink(settings)
    validator = MicroBatchValidator(settings, sink)

    # 1. Oversize batch bytes
    with pytest.raises(OversizeBatchError):
        validator.validate(b"x" * 2000, source="test")

    # 2. Malformed NDJSON isolation
    raw_ndjson = b'{"event_id": 1}\n{"invalid_json": }\n'
    vbatch = validator.validate(raw_ndjson, source="test")
    assert vbatch.rejected >= 1


def test_polars_adapter_stream_fallback() -> None:
    adapter = PolarsAdapter()
    batch = pa.RecordBatch.from_arrays(
        [pa.array([10], type=pa.int64())],
        names=["num"],
    )
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])

    lf = adapter.scan_stream(reader, batch.schema)
    df = lf.collect()
    assert len(df) == 1
