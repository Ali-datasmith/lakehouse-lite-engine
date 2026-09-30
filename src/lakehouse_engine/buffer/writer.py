# src/lakehouse_engine/buffer/writer.py
import contextlib
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq

from lakehouse_engine.exceptions import ParquetWriteError

if TYPE_CHECKING:
    from lakehouse_engine.config import BufferSettings


@dataclass(frozen=True, slots=True)
class WriteStats:
    rows: int
    row_groups: int
    file_bytes: int


class ParquetFlushWriter:
    def __init__(
        self,
        settings: "BufferSettings",
        schema: pa.Schema,
        filesystem: pafs.FileSystem,
    ) -> None:
        self._settings = settings
        self._schema = schema
        self._fs = filesystem

    def write(self, batches: Sequence[pa.RecordBatch], *, path: str) -> WriteStats:
        """Writes one file. `path` is a filesystem-relative path (from resolve_filesystem).

        Raises ParquetWriteError on any I/O or encoding failure.
        A partially written file is deleted best-effort.
        """
        if not batches:
            raise ValueError("No batches provided to write.")

        try:
            # Ensure parent directory exists on local filesystem
            parent_dir = path.rsplit("/", 1)[0] if "/" in path else ""
            if parent_dir:
                with contextlib.suppress(Exception):
                    self._fs.create_dir(parent_dir, recursive=True)

            table = pa.Table.from_batches(list(batches), schema=self._schema)
            with pq.ParquetWriter(
                path,
                self._schema,
                filesystem=self._fs,
                compression=self._settings.compression,
                compression_level=self._settings.compression_level,
                use_dictionary=True,
                write_statistics=True,
                write_page_index=True,
                data_page_size=self._settings.data_page_bytes,
                version="2.6",
            ) as writer:
                writer.write_table(table, row_group_size=self._settings.row_group_max_rows)

            file_info = self._fs.get_file_info(path)
            file_bytes = file_info.size
            total_rows = table.num_rows

            parquet_file = pq.ParquetFile(path, filesystem=self._fs)
            num_row_groups = parquet_file.metadata.num_row_groups

            return WriteStats(
                rows=total_rows,
                row_groups=num_row_groups,
                file_bytes=file_bytes,
            )
        except Exception as exc:
            with contextlib.suppress(Exception):
                self._fs.delete_file(path)
            raise ParquetWriteError(f"Failed to write Parquet file at {path}: {exc}") from exc
