# src/lakehouse_engine/query/protocols.py
from typing import Protocol, runtime_checkable


@runtime_checkable
class ArrowStreamExportable(Protocol):
    """Arrow C Stream Interface (PyCapsule). Consumed exactly once per capsule."""

    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object: ...


@runtime_checkable
class ArrowArrayExportable(Protocol):
    """Arrow C Data Interface (PyCapsule): returns (schema_capsule, array_capsule)."""

    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> tuple[object, object]: ...
