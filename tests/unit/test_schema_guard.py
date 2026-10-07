import pyarrow as pa
import pytest
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.types import IntegerType, LongType, NestedField, StringType, TimestamptzType

from lakehouse_engine.catalog.schema_guard import assert_compatible
from lakehouse_engine.catalog.storage import resolve_filesystem
from lakehouse_engine.exceptions import ConfigurationError, SchemaEvolutionError


def test_schema_guard_exact_match() -> None:
    iceberg_schema = IcebergSchema(
        NestedField(1, "event_id", LongType(), required=True),
        NestedField(2, "user_id", LongType(), required=True),
        NestedField(3, "event_name", StringType(), required=True),
        NestedField(4, "event_ts", TimestamptzType(), required=True),
        NestedField(5, "payload", StringType(), required=False),
    )
    arrow_schema = pa.schema(
        [  # type: ignore[arg-type]
            pa.field("event_id", pa.int64(), nullable=False),
            pa.field("user_id", pa.int64(), nullable=False),
            pa.field("event_name", pa.string(), nullable=False),
            pa.field("event_ts", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("payload", pa.string(), nullable=True),
        ]
    )

    # Should pass without exception
    assert_compatible(iceberg_schema, arrow_schema)


def test_schema_guard_type_mismatch() -> None:
    iceberg_schema = IcebergSchema(
        NestedField(1, "event_id", IntegerType(), required=True),  # Integer instead of Long
        NestedField(2, "user_id", LongType(), required=True),
        NestedField(3, "event_name", StringType(), required=True),
        NestedField(4, "event_ts", TimestamptzType(), required=True),
        NestedField(5, "payload", StringType(), required=False),
    )
    arrow_schema = pa.schema(
        [  # type: ignore[arg-type]
            pa.field("event_id", pa.int64(), nullable=False),
            pa.field("user_id", pa.int64(), nullable=False),
            pa.field("event_name", pa.string(), nullable=False),
            pa.field("event_ts", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("payload", pa.string(), nullable=True),
        ]
    )

    with pytest.raises(SchemaEvolutionError, match="incompatible with engine schema"):
        assert_compatible(iceberg_schema, arrow_schema)


def test_storage_unsupported_scheme() -> None:
    with pytest.raises(ConfigurationError, match="Unsupported storage scheme"):
        resolve_filesystem("ftp://invalid-server/data")
