# src/lakehouse_engine/ingestion/schema.py
from typing import Any, Final, cast

import pyarrow as pa

_fields = [
    pa.field("event_id", pa.int64(), nullable=False, metadata={"PARQUET:field_id": "1"}),
    pa.field("user_id", pa.int64(), nullable=False, metadata={"PARQUET:field_id": "2"}),
    pa.field("event_name", pa.string(), nullable=False, metadata={"PARQUET:field_id": "3"}),
    pa.field(
        "event_ts", pa.timestamp("us", tz="UTC"), nullable=False, metadata={"PARQUET:field_id": "4"}
    ),
    pa.field(
        "payload", pa.string(), nullable=True, metadata={"PARQUET:field_id": "5"}
    ),  # JSON text
]

EVENTS_ARROW_SCHEMA: Final[pa.Schema] = pa.schema(cast("list[pa.Field[Any]]", _fields))
