# src/lakehouse_engine/benchmarks/memory.py
import resource
import sys
import threading
import time
from dataclasses import dataclass
from types import TracebackType

import psutil
import pyarrow as pa


@dataclass(frozen=True, slots=True)
class MemoryReport:
    peak_rss_bytes: int
    peak_arrow_bytes: int
    peak_python_heap_bytes: int
    ru_maxrss_bytes: int


class RssSampler:
    """Background thread sampling psutil RSS every 10 ms. .peak_bytes valid after stop()."""

    def __init__(self, interval_seconds: float = 0.01) -> None:
        self._interval = interval_seconds
        self._process = psutil.Process()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._peak_rss = 0
        self._peak_arrow = 0

    def _sample_loop(self) -> None:
        while not self._stop_event.is_set():
            rss = int(self._process.memory_info().rss)
            self._peak_rss = max(self._peak_rss, rss)
            arrow = int(pa.total_allocated_bytes())
            self._peak_arrow = max(self._peak_arrow, arrow)
            time.sleep(self._interval)

    def __enter__(self) -> "RssSampler":
        self._peak_rss = int(self._process.memory_info().rss)
        self._peak_arrow = int(pa.total_allocated_bytes())
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._sample_loop, daemon=True)
        self._thread.start()
        return self

    def __exit__(
        self,
        et: type[BaseException] | None,
        ev: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join()

    @property
    def peak_bytes(self) -> int:
        return self._peak_rss

    @property
    def peak_arrow_bytes(self) -> int:
        return self._peak_arrow

    def get_report(self, python_heap_peak: int = 0) -> MemoryReport:
        ru_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # Normalise ru_maxrss: KB on Linux, bytes on macOS
        ru_bytes = ru_rss * 1024 if sys.platform != "darwin" else ru_rss

        return MemoryReport(
            peak_rss_bytes=self._peak_rss,
            peak_arrow_bytes=self._peak_arrow,
            peak_python_heap_bytes=python_heap_peak,
            ru_maxrss_bytes=ru_bytes,
        )
