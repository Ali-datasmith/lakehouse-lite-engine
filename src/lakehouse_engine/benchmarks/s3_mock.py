# src/lakehouse_engine/benchmarks/s3_mock.py
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.fs as pafs

if TYPE_CHECKING:
    from lakehouse_engine.config import BenchmarkSettings


@dataclass(slots=True)
class S3RequestCounts:
    get_count: int = 0
    put_count: int = 0
    head_count: int = 0
    list_count: int = 0


class CountingFileSystemHandler(pafs.FileSystemHandler):
    """Wraps a LocalFileSystem and counts calls, mapped to S3 request classes:

    open_input_file / read range   -> GET
    get_file_info(path)            -> HEAD
    get_file_info(FileSelector)    -> LIST
    open_output_stream / close     -> PUT
    """

    def __init__(self, local_fs: pafs.LocalFileSystem) -> None:
        self._local_fs = local_fs
        self.counts = S3RequestCounts()

    def get_type_name(self) -> str:
        return "counting_s3_mock"

    def get_file_info(
        self,
        paths_or_selector: pafs.FileSelector | list[str] | str,
    ) -> list[pafs.FileInfo] | pafs.FileInfo:
        if isinstance(paths_or_selector, pafs.FileSelector):
            self.counts.list_count += 1
            return self._local_fs.get_file_info(paths_or_selector)
        self.counts.head_count += 1
        return self._local_fs.get_file_info(paths_or_selector)

    def create_dir(self, path: str, recursive: bool = True) -> None:
        self._local_fs.create_dir(path, recursive=recursive)

    def delete_dir(self, path: str) -> None:
        self._local_fs.delete_dir(path)

    def delete_file(self, path: str) -> None:
        self._local_fs.delete_file(path)

    def move(self, src: str, dest: str) -> None:
        self._local_fs.move(src, dest)

    def copy_file(self, src: str, dest: str) -> None:
        self._local_fs.copy_file(src, dest)

    def open_input_stream(self, path: str) -> pa.NativeFile:
        self.counts.get_count += 1
        res: pa.NativeFile = self._local_fs.open_input_stream(path)
        return res

    def open_input_file(self, path: str) -> pa.NativeFile:
        self.counts.get_count += 1
        res: pa.NativeFile = self._local_fs.open_input_file(path)
        return res

    def open_output_stream(self, path: str, metadata: object = None) -> pa.NativeFile:
        self.counts.put_count += 1
        res: pa.NativeFile = self._local_fs.open_output_stream(path, metadata=metadata)  # type: ignore[arg-type]
        return res

    def open_append_stream(self, path: str, metadata: object = None) -> pa.NativeFile:
        self.counts.put_count += 1
        res: pa.NativeFile = self._local_fs.open_append_stream(path, metadata=metadata)  # type: ignore[arg-type]
        return res


@dataclass(frozen=True, slots=True)
class S3CostModel:
    get_per_1k_usd: float
    put_list_per_1k_usd: float
    head_per_1k_usd: float

    @classmethod
    def from_settings(cls, settings: "BenchmarkSettings") -> "S3CostModel":
        return cls(
            get_per_1k_usd=settings.s3_get_per_1k_usd,
            put_list_per_1k_usd=settings.s3_put_list_per_1k_usd,
            head_per_1k_usd=settings.s3_head_per_1k_usd,
        )

    def cost(self, counts: S3RequestCounts) -> float:
        get_cost = (counts.get_count / 1000.0) * self.get_per_1k_usd
        put_list_cost = ((counts.put_count + counts.list_count) / 1000.0) * self.put_list_per_1k_usd
        head_cost = (counts.head_count / 1000.0) * self.head_per_1k_usd
        return get_cost + put_list_cost + head_cost
