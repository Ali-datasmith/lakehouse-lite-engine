# src/lakehouse_engine/catalog/schema_guard.py
import pyarrow as pa
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.schema import Schema as IcebergSchema

from lakehouse_engine.exceptions import SchemaEvolutionError


def _normalize_arrow_type(t: pa.DataType) -> pa.DataType:
    """Normalizes large_string to string for PyIceberg compatibility."""
    if t == pa.large_string():
        return pa.string()
    return t


def assert_compatible(table_schema: IcebergSchema, expected_arrow_schema: pa.Schema) -> None:
    """Strictly checks table schema compatibility with expected_arrow_schema.

    Compares field names, types, order, and nullability by converting Iceberg schema to Arrow.
    Raises SchemaEvolutionError on any mismatch.
    """
    table_arrow_schema = schema_to_pyarrow(table_schema)
    diffs: list[str] = []

    if len(table_arrow_schema) != len(expected_arrow_schema):
        msg = (
            f"Field count mismatch: table has {len(table_arrow_schema)}, "
            f"expected {len(expected_arrow_schema)}"
        )
        diffs.append(msg)

    for idx, (t_field, e_field) in enumerate(
        zip(table_arrow_schema, expected_arrow_schema, strict=False)
    ):
        if t_field.name != e_field.name:
            msg = (
                f"Field position {idx} name mismatch: table has '{t_field.name}', "
                f"expected '{e_field.name}'"
            )
            diffs.append(msg)

        t_type = _normalize_arrow_type(t_field.type)
        e_type = _normalize_arrow_type(e_field.type)
        if t_type != e_type:
            msg = (
                f"Field '{e_field.name}' type mismatch: table type '{t_type}', expected '{e_type}'"
            )
            diffs.append(msg)

        if t_field.nullable != e_field.nullable:
            msg = (
                f"Field '{e_field.name}' nullability mismatch: "
                f"table nullable={t_field.nullable}, expected={e_field.nullable}"
            )
            diffs.append(msg)

    if diffs:
        raise SchemaEvolutionError(
            "Catalog table schema is incompatible with engine schema.",
            context={"diff": diffs},
        )
