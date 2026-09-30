# src/lakehouse_engine/exceptions.py
from collections.abc import Mapping
from types import MappingProxyType
from typing import ClassVar


class LakehouseError(Exception):
    """Root of all engine errors. Never raised directly."""

    code: ClassVar[str] = "LHE-0000"
    retryable: ClassVar[bool] = False

    def __init__(self, message: str, *, context: Mapping[str, object] | None = None) -> None:
        super().__init__(message)
        self.message: str = message
        self.context: Mapping[str, object] = MappingProxyType(dict(context or {}))

    def __str__(self) -> str:
        return f"[{self.code}] {self.message}"


# ---- configuration / runtime -------------------------------------------------
class ConfigurationError(LakehouseError):
    code = "LHE-1001"


class MemoryBudgetExceededError(LakehouseError):
    """RSS crossed the hard limit. Caller should flush/back off; never auto-retried."""

    code = "LHE-1002"


class ResourceBusyError(LakehouseError):
    """A mutually exclusive mode lease (flush/compact vs query) is held."""

    code = "LHE-1003"
    retryable = True


# ---- ingestion ---------------------------------------------------------------
class IngestionError(LakehouseError):
    code = "LHE-2000"


class SchemaValidationError(IngestionError):
    """Raised only for whole-batch failures that cannot be isolated to rows."""

    code = "LHE-2001"


class OversizeBatchError(IngestionError):
    code = "LHE-2002"


class DeadLetterThresholdExceeded(IngestionError):  # noqa: N818
    """Reject ratio breaker tripped: probable upstream schema drift."""

    code = "LHE-2003"


class DeadLetterWriteError(IngestionError):
    code = "LHE-2004"


# ---- buffer / storage --------------------------------------------------------
class BufferOverflowError(LakehouseError):
    code = "LHE-3001"


class SchemaMismatchError(LakehouseError):
    """Arrow batch schema differs from EVENTS_ARROW_SCHEMA."""

    code = "LHE-3002"


class FlushError(LakehouseError):
    code = "LHE-3003"


class ParquetWriteError(FlushError):
    code = "LHE-3004"
    retryable = True


class CompactionError(LakehouseError):
    code = "LHE-3005"


# ---- catalog -----------------------------------------------------------------
class CatalogError(LakehouseError):
    code = "LHE-4000"


class CatalogConnectionError(CatalogError):
    code = "LHE-4001"
    retryable = True


class SchemaEvolutionError(CatalogError):
    """Table schema != engine schema. Fatal; requires operator action."""

    code = "LHE-4002"


class CatalogCommitError(CatalogError):
    """Commit failed or exhausted retries.

    context['pending_files'] lists durable, uncommitted files.
    """

    code = "LHE-4003"
    retryable = True


# ---- query -------------------------------------------------------------------
class QueryError(LakehouseError):
    code = "LHE-5000"


class EngineCapabilityError(QueryError):
    """Pinned engine version lacks a required capability (see capability probes)."""

    code = "LHE-5001"


class StreamConsumedError(QueryError):
    code = "LHE-5002"


class UnsafeSqlError(QueryError):
    code = "LHE-5003"


# ---- benchmarks --------------------------------------------------------------
class BenchmarkError(LakehouseError):
    code = "LHE-6000"


class BenchmarkBudgetExceeded(BenchmarkError):  # noqa: N818
    code = "LHE-6001"
