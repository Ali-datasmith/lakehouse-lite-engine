# src/lakehouse_engine/governor.py
import gc
import threading
from enum import StrEnum
from types import TracebackType
from typing import TYPE_CHECKING

import psutil
import pyarrow as pa

from lakehouse_engine.exceptions import MemoryBudgetExceededError, ResourceBusyError

if TYPE_CHECKING:
    from lakehouse_engine.config import RuntimeSettings


class Mode(StrEnum):
    FLUSH = "flush"
    COMPACT = "compact"
    QUERY = "query"


class ModeLease:
    def __init__(self, governor: "ResourceGovernor", mode: Mode) -> None:
        self._governor = governor
        self._mode = mode

    def __enter__(self) -> "ModeLease":
        return self

    def __exit__(
        self,
        et: type[BaseException] | None,
        ev: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._governor._release_lease(self._mode)


class ResourceGovernor:
    def __init__(self, settings: "RuntimeSettings") -> None:
        self._settings = settings
        self._process = psutil.Process()
        self._lock = threading.RLock()
        self._active_leases: set[Mode] = set()

    def rss_bytes(self) -> int:
        """Returns physical process RSS bytes."""
        return int(self._process.memory_info().rss)

    def arrow_allocated_bytes(self) -> int:
        """Returns pyarrow.total_allocated_bytes()."""
        return int(pa.total_allocated_bytes())

    def check(self, *, incoming_bytes: int = 0) -> None:
        """Raises MemoryBudgetExceededError if hard limit or ceiling would be crossed."""
        rss = self.rss_bytes()
        hard_limit = self._settings.memory_hard_limit_bytes
        if rss + incoming_bytes >= hard_limit:
            msg = (
                f"Memory hard limit exceeded: RSS {rss} + incoming {incoming_bytes} >= {hard_limit}"
            )
            raise MemoryBudgetExceededError(
                msg,
                context={
                    "rss_bytes": rss,
                    "incoming_bytes": incoming_bytes,
                    "hard_limit_bytes": hard_limit,
                },
            )

    def over_soft_limit(self) -> bool:
        """Returns True if current RSS exceeds soft limit."""
        return bool(self.rss_bytes() >= self._settings.memory_soft_limit_bytes)

    def lease(self, mode: Mode) -> ModeLease:
        """Acquires a mode lease. Raises ResourceBusyError on conflict."""
        with self._lock:
            allow_concurrent = self._settings.allow_concurrent_query_during_flush
            if mode in (Mode.FLUSH, Mode.COMPACT):
                if Mode.QUERY in self._active_leases and not allow_concurrent:
                    raise ResourceBusyError(
                        f"Cannot start {mode} while query lease is held.",
                        context={"active_leases": [str(m) for m in self._active_leases]},
                    )
                if Mode.FLUSH in self._active_leases or Mode.COMPACT in self._active_leases:
                    raise ResourceBusyError(
                        f"Cannot start {mode} while write/compact lease is held.",
                        context={"active_leases": [str(m) for m in self._active_leases]},
                    )
            elif mode == Mode.QUERY:
                has_writer = (
                    Mode.FLUSH in self._active_leases or Mode.COMPACT in self._active_leases
                )
                if has_writer and not allow_concurrent:
                    raise ResourceBusyError(
                        "Cannot start query while flush/compact lease is held.",
                        context={"active_leases": [str(m) for m in self._active_leases]},
                    )

            self._active_leases.add(mode)
            return ModeLease(self, mode)

    def _release_lease(self, mode: Mode) -> None:
        with self._lock:
            self._active_leases.discard(mode)

    def release_memory(self) -> None:
        """Triggers garbage collection and releases Arrow memory pool buffers."""
        gc.collect()
        pool = pa.default_memory_pool()
        pool.release_unused()
