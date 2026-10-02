# src/lakehouse_engine/buffer/compaction.py
import contextlib
import logging
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq

from lakehouse_engine.catalog.storage import resolve_filesystem
from lakehouse_engine.exceptions import CompactionError
from lakehouse_engine.governor import Mode
from lakehouse_engine.ingestion.schema import EVENTS_ARROW_SCHEMA

if TYPE_CHECKING:
    from lakehouse_engine.catalog.manager import CatalogManager, CommitResult
    from lakehouse_engine.config import BufferSettings, CompactionSettings
    from lakehouse_engine.governor import ResourceGovernor

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class CompactionPlan:
    input_paths: tuple[str, ...]
    input_rows: int
    input_bytes: int
    snapshot_id: int


@dataclass(frozen=True, slots=True)
class CompactionResult:
    plan: CompactionPlan
    output_paths: tuple[str, ...]
    output_rows: int
    commit: "CommitResult"


class Compactor:
    def __init__(
        self,
        settings: "CompactionSettings",
        buffer_settings: "BufferSettings",
        catalog: "CatalogManager",
        governor: "ResourceGovernor",
    ) -> None:
        self._settings = settings
        self._buffer_settings = buffer_settings
        self._catalog = catalog
        self._governor = governor

    def plan(self) -> CompactionPlan | None:
        tbl = self._catalog.current_table()
        current_snap = tbl.current_snapshot()
        if current_snap is None:
            return None

        small_files: list[tuple[str, int, int]] = []
        for task in tbl.scan().plan_files():
            file_path = task.file.file_path
            file_bytes = task.file.file_size_in_bytes
            file_rows = task.file.record_count
            if file_bytes < self._settings.small_file_threshold_bytes:
                small_files.append((file_path, file_bytes, file_rows))

        if len(small_files) < self._settings.min_input_files:
            return None

        # Sort candidate small files deterministically by size, then path
        small_files.sort(key=lambda f: (f[1], f[0]))
        small_files = small_files[: self._settings.max_input_files_per_run]

        input_paths = tuple(f[0] for f in small_files)
        input_bytes = sum(f[1] for f in small_files)
        input_rows = sum(f[2] for f in small_files)

        return CompactionPlan(
            input_paths=input_paths,
            input_rows=input_rows,
            input_bytes=input_bytes,
            snapshot_id=current_snap.snapshot_id,
        )

    def run(self, plan: CompactionPlan) -> CompactionResult:
        with self._governor.lease(Mode.COMPACT):
            self._verify_plan_freshness(plan)

            flush_id = uuid.uuid4().hex
            data_dir = self._catalog.data_dir()
            output_paths: list[str] = []

            accumulator: list[pa.RecordBatch] = []
            accumulated_bytes = 0
            accumulated_rows = 0

            arrow_schema = EVENTS_ARROW_SCHEMA

            def flush_accumulator(writer: pq.ParquetWriter) -> None:
                nonlocal accumulator, accumulated_bytes, accumulated_rows
                if not accumulator:
                    return
                tbl_chunk = pa.Table.from_batches(accumulator, schema=arrow_schema)
                rg_rows = self._buffer_settings.row_group_max_rows
                writer.write_table(tbl_chunk, row_group_size=rg_rows)
                accumulator.clear()
                accumulated_bytes = 0
                accumulated_rows = 0
                self._governor.release_memory()

            current_output_rel_path: str | None = None
            current_writer: pq.ParquetWriter | None = None
            current_fs: pafs.FileSystem | None = None

            try:
                for input_path in plan.input_paths:
                    fs, rel_path = resolve_filesystem(input_path)
                    parquet_file = pq.ParquetFile(rel_path, filesystem=fs)

                    for batch in parquet_file.iter_batches(
                        batch_size=self._settings.read_batch_rows,
                        use_threads=self._settings.use_threads,
                    ):
                        b_bytes = batch.get_total_buffer_size()
                        self._governor.check(incoming_bytes=b_bytes)

                        accumulator.append(batch)
                        accumulated_bytes += b_bytes
                        accumulated_rows += batch.num_rows

                        if current_writer is None:
                            fn = f"compact-{uuid.uuid4().hex}.parquet"
                            current_output_rel_path = f"{data_dir}/{fn}"
                            current_fs, rel_out = resolve_filesystem(current_output_rel_path)
                            output_paths.append(current_output_rel_path)
                            current_writer = pq.ParquetWriter(
                                rel_out,
                                arrow_schema,
                                filesystem=current_fs,
                                compression=self._buffer_settings.compression,
                                compression_level=self._buffer_settings.compression_level,
                                use_dictionary=True,
                                write_statistics=True,
                                write_page_index=True,
                                data_page_size=self._buffer_settings.data_page_bytes,
                                version="2.6",
                            )

                        if (
                            accumulated_bytes >= self._buffer_settings.max_bytes
                            or accumulated_rows >= self._buffer_settings.max_rows
                        ):
                            flush_accumulator(current_writer)

                        if current_output_rel_path is not None and current_fs is not None:
                            _, rel_out = resolve_filesystem(current_output_rel_path)
                            target_sz = self._settings.target_file_bytes
                            if current_fs.get_file_info(rel_out).size >= target_sz:
                                flush_accumulator(current_writer)
                                current_writer.close()
                                current_writer = None

                if current_writer is not None:
                    flush_accumulator(current_writer)
                    current_writer.close()
                    current_writer = None

                # Verify row count invariance by inspecting written Parquet file footers
                output_rows_total = 0
                for out_path in output_paths:
                    out_fs, out_rel = resolve_filesystem(out_path)
                    pf = pq.ParquetFile(out_rel, filesystem=out_fs)
                    output_rows_total += pf.metadata.num_rows

                if output_rows_total != plan.input_rows:
                    msg = (
                        f"Row count mismatch during compaction: expected "
                        f"{plan.input_rows}, got {output_rows_total}"
                    )
                    raise CompactionError(msg)  # noqa: TRY301

                commit_res = self._catalog.commit_replace(
                    delete=plan.input_paths,
                    add_paths=output_paths,
                    flush_id=flush_id,
                )

                logger.info(
                    "Compaction finished: merged %d files (%d rows) into %d files",
                    len(plan.input_paths),
                    output_rows_total,
                    len(output_paths),
                )

                return CompactionResult(
                    plan=plan,
                    output_paths=tuple(output_paths),
                    output_rows=output_rows_total,
                    commit=commit_res,
                )
            except Exception as exc:
                if current_writer is not None:
                    with contextlib.suppress(Exception):
                        current_writer.close()
                for out_p in output_paths:
                    with contextlib.suppress(Exception):
                        out_fs, rel_p = resolve_filesystem(out_p)
                        out_fs.delete_file(rel_p)
                if isinstance(exc, CompactionError):
                    raise
                raise CompactionError(f"Compaction execution failed: {exc}") from exc

    def _verify_plan_freshness(self, plan: CompactionPlan) -> None:
        current_snap_id = self._catalog.snapshot_id()
        if current_snap_id != plan.snapshot_id:
            tbl = self._catalog.current_table()
            existing_files = {task.file.file_path for task in tbl.scan().plan_files()}
            for path in plan.input_paths:
                if path not in existing_files:
                    raise CompactionError(
                        f"Compaction plan is stale: input file {path} no longer exists."
                    )
