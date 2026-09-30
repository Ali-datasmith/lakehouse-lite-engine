# src/lakehouse_engine/engine.py
import contextlib
from types import TracebackType
from typing import Self

from lakehouse_engine.buffer.buffer import BufferPolicy, CompactionBuffer
from lakehouse_engine.buffer.compaction import Compactor
from lakehouse_engine.buffer.writer import ParquetFlushWriter
from lakehouse_engine.catalog.manager import CatalogManager
from lakehouse_engine.catalog.storage import resolve_filesystem
from lakehouse_engine.config import EngineSettings
from lakehouse_engine.governor import ResourceGovernor
from lakehouse_engine.ingestion.dlq import NdjsonDeadLetterSink
from lakehouse_engine.ingestion.schema import EVENTS_ARROW_SCHEMA
from lakehouse_engine.ingestion.validator import MicroBatchValidator, ValidatedBatch
from lakehouse_engine.query.service import QueryService
from lakehouse_engine.runtime import configure_runtime


class LakehouseEngine:
    def __init__(self, settings: EngineSettings | None = None) -> None:
        self._settings = settings if settings is not None else EngineSettings.load()
        configure_runtime(self._settings.runtime)

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
    ) -> ValidatedBatch | None:
        if self._closed:
            raise RuntimeError("LakehouseEngine is closed.")

        vbatch = self._validator.validate(raw, source=source, source_offset=source_offset)
        if vbatch.batch is not None and vbatch.batch.num_rows > 0:
            self._buffer.append(vbatch.batch)
        return vbatch

    def flush(self) -> object:
        if self._closed:
            raise RuntimeError("LakehouseEngine is closed.")
        return self._buffer.flush()

    def compact(self) -> object:
        if self._closed:
            raise RuntimeError("LakehouseEngine is closed.")
        plan = self._compactor.plan()
        if plan is None:
            return None
        return self._compactor.run(plan)

    @property
    def query(self) -> QueryService:
        return self._query_service

    def close(self) -> None:
        if not self._closed:
            with contextlib.suppress(Exception):
                self._buffer.close()
            with contextlib.suppress(Exception):
                self._dlq_sink.close()
            with contextlib.suppress(Exception):
                self._catalog.close()
            self._closed = True
