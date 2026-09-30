# src/lakehouse_engine/catalog/schema_guard.py
import pyarrow as pa
from pyiceberg.schema import Schema as IcebergSchema

from lakehouse_engine.exceptions import SchemaEvolutionError


def assert_compatible(table_schema: IcebergSchema, expected_arrow_schema: pa.Schema) -> None:
    """Strictly checks table schema compatibility with expected_arrow_schema.

    Compares field names, types, order, and nullability.
    Raises SchemaEvolutionError on any mismatch.
    """
    iceberg_fields = table_schema.fields
    arrow_fields = list(expected_arrow_schema)

    diffs: list[str] = []

    if len(iceberg_fields) != len(arrow_fields):
        diffs.append(
            f"Field count mismatch: table has {len(iceberg_fields)}, expected {len(arrow_fields)}"
        )

    for idx, (i_field, a_field) in enumerate(zip(iceberg_fields, arrow_fields, strict=False)):
        if i_field.name != a_field.name:
            msg = (
                f"Field position {idx} name mismatch: table has '{i_field.name}', "
                f"expected '{a_field.name}'"
            )
            diffs.append(msg)
        if i_field.required == a_field.nullable:
            msg = (
                f"Field '{a_field.name}' nullability mismatch: table required={i_field.required}, "
                f"expected nullable={a_field.nullable}"
            )
            diffs.append(msg)

    if diffs:
        raise SchemaEvolutionError(
            "Catalog table schema is incompatible with engine schema.",
            context={"diff": diffs},
        )
