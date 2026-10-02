# src/lakehouse_engine/engine.py
import logging
from types import TracebackType
from typing import TYPE_CHECKING, Any, Self

from lakehouse_engine.config import EngineSettings
from lakehouse_engine.runtime import configure_runtime

if TYPE_CHECKING:
    from lakehouse_engine.buffer.compaction import CompactionResult
    from lakehouse_engine.catalog.manager import CommitResult
    from lakehouse_engine.ingestion.validator import ValidatedBatch
    from lakehouse_engine.query.service import QueryService

logger = logging.getLogger(__name__)


class MetricsCollector:
    """No-op default metrics abstraction."""

    def increment(self, metric: str, value: int = 1, tags: dict[str, str] | None = None) -> None:
        pass

    def gauge(self, metric: str, value: float, tags: dict[str, str] | None = None) -> None:
        pass

    def histogram(self, metric: str, value: float, tags: dict[str, str] | None = None) -> None:
        pass


class LakehouseEngine:
    def __init__(
        self,
        settings: EngineSettings | None = None,
        metrics: MetricsCollector | None = None,
    ) -> None:
        self._settings = settings if settings is not None else EngineSettings.load()
        configure_runtime(self._settings.runtime)

        self._metrics = metrics or MetricsCollector()
        logger.info("Initializing LakehouseEngine (env: %s)", self._settings.runtime.env)

        # Defer imports of subsystem classes to prevent eager top-level heavy loads
        from lakehouse_engine.buffer.buffer import BufferPolicy, CompactionBuffer
        from lakehouse_engine.buffer.compaction import Compactor
        from lakehouse_engine.buffer.writer import ParquetFlushWriter
        from lakehouse_engine.catalog.manager import CatalogManager
        from lakehouse_engine.catalog.storage import resolve_filesystem
        from lakehouse_engine.governor import ResourceGovernor
        from lakehouse_engine.ingestion.dlq import NdjsonDeadLetterSink
        from lakehouse_engine.ingestion.schema import EVENTS_ARROW_SCHEMA
        from lakehouse_engine.ingestion.validator import MicroBatchValidator
        from lakehouse_engine.query.service import QueryService

        self._governor = ResourceGovernor(self._settings.runtime)
        self._dlq_sink = NdjsonDeadLetterSink(self._settings.ingestion)
        self._validator = MicroBatchValidator(self._settings.ingestion, self._dlq_sink)

        self._catalog = CatalogManager(self._settings.catalog, self._settings.storage)
        self._catalog.open()

        data_dir = self._catalog.data_dir()
        fs, rel_data_path = resolve_filesystem(data_dir, self._settings.storage)

        self._writer = ParquetFlushWriter(
            self._settings.buffer,
            EVENTS_ARROW_SCHEMA,
            fs,
        )

        buffer_policy = BufferPolicy(
            max_bytes=self._settings.buffer.max_bytes,
            max_rows=self._settings.buffer.max_rows,
            max_age_seconds=self._settings.buffer.max_age_seconds,
        )

        self._buffer = CompactionBuffer(
            schema=EVENTS_ARROW_SCHEMA,
            policy=buffer_policy,
            writer=self._writer,
            committer=self._catalog,
            governor=self._governor,
            data_dir=rel_data_path,
        )

        self._compactor = Compactor(
            self._settings.compaction,
            self._settings.buffer,
            self._catalog,
            self._governor,
        )

        self._query_service = QueryService(
            self._catalog,
            self._settings.query,
            self._governor,
        )

        self._closed = False
        self._metrics.increment("engine_initialized")

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        et: type[BaseException] | None,
        ev: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def ingest(
        self,
        raw: bytes | bytearray | memoryview,
        *,
        source: str,
        source_offset: int | None = None,
    ) -> "ValidatedBatch | None":
        if self._closed:
            raise RuntimeError("LakehouseEngine is closed.")

        vbatch = self._validator.validate(raw, source=source, source_offset=source_offset)
        if vbatch.batch is not None and vbatch.batch.num_rows > 0:
            self._buffer.append(vbatch.batch)
            self._metrics.increment("ingest_accepted_rows", vbatch.accepted)
        if vbatch.rejected > 0:
            self._metrics.increment("ingest_rejected_rows", vbatch.rejected)
        return vbatch

    def flush(self) -> "CommitResult | None":
        if self._closed:
            raise RuntimeError("LakehouseEngine is closed.")
        logger.info("Explicit flush requested")
        res = self._buffer.flush()
        if res is not None:
            self._metrics.increment("flush_success")
        return res

    def compact(self) -> "CompactionResult | None":
        if self._closed:
            raise RuntimeError("LakehouseEngine is closed.")
        logger.info("Compaction run requested")
        plan = self._compactor.plan()
        if plan is None:
            logger.info("No compaction plan generated")
            return None
        res = self._compactor.run(plan)
        if res is not None:
            self._metrics.increment("compaction_success")
        return res

    @property
    def query(self) -> "QueryService":
        return self._query_service

    def status(self) -> dict[str, Any]:
        """Introspection helper returning health and resource status."""
        return {
            "closed": self._closed,
            "rss_bytes": self._governor.rss_bytes(),
            "buffer_bytes": self._buffer.total_bytes,
            "buffer_rows": self._buffer.total_rows,
            "active_leases": [str(m) for m in self._governor.active_leases],
        }

    def close(self) -> None:
        if not self._closed:
            logger.info("Closing LakehouseEngine")
            for sub, name in (
                (self._buffer, "buffer"),
                (self._dlq_sink, "dlq_sink"),
                (self._catalog, "catalog"),
            ):
                try:
                    sub.close()
                except Exception as exc:
                    logger.error("Error closing %s: %s", name, exc, exc_info=True)
            self._closed = True
            self._metrics.increment("engine_closed")
