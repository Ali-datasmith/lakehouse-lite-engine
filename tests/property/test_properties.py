# tests/property/test_properties.py
from datetime import UTC, datetime

import pyarrow as pa
from hypothesis import given
from hypothesis import strategies as st

from lakehouse_engine.ingestion.schema import EVENTS_ARROW_SCHEMA


@given(st.lists(st.integers(min_value=1, max_value=2**62), min_size=1, max_size=100))
def test_arrow_conversion_property(ids) -> None:  # type: ignore[no-untyped-def]
    event_ids = pa.array(ids, type=pa.int64())
    user_ids = pa.array([10] * len(ids), type=pa.int64())
    names = pa.array(["event"] * len(ids), type=pa.string())
    ts = datetime(2026, 9, 30, 12, 0, 0, tzinfo=UTC)
    tss = pa.array([ts] * len(ids), type=pa.timestamp("us", tz="UTC"))
    payloads = pa.array([None] * len(ids), type=pa.string())

    batch = pa.RecordBatch.from_arrays(
        [event_ids, user_ids, names, tss, payloads],
        schema=EVENTS_ARROW_SCHEMA,
    )
    assert batch.num_rows == len(ids)
    assert batch.schema.equals(EVENTS_ARROW_SCHEMA, check_metadata=False)
