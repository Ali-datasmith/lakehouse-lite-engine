# src/lakehouse_engine/governor.py
import gc
import logging
import threading
from collections import defaultdict
from collections.abc import Callable
from enum import StrEnum
from types import TracebackType
from typing import TYPE_CHECKING, TypeVar

import psutil
import pyarrow as pa

from lakehouse_engine.exceptions import MemoryBudgetExceededError, ResourceBusyError

if TYPE_CHECKING:
    from lakehouse_engine.config import RuntimeSettings

logger = logging.getLogger(__name__)

T = TypeVar("T")


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
        self._lease_counts: dict[Mode, int] = defaultdict(int)

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
            logger.warning(msg)
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

    @property
    def active_leases(self) -> set[Mode]:
        with self._lock:
            return {m for m, count in self._lease_counts.items() if count > 0}

    def lease(self, mode: Mode) -> ModeLease:
        """Acquires a reentrant mode lease. Raises ResourceBusyError on conflict."""
        with self._lock:
            allow_concurrent = self._settings.allow_concurrent_query_during_flush
            active = self.active_leases

            # If mode is already held by current thread/process, reentrant acquisition is allowed
            if self._lease_counts[mode] > 0:
                self._lease_counts[mode] += 1
                return ModeLease(self, mode)

            if mode in (Mode.FLUSH, Mode.COMPACT):
                if Mode.QUERY in active and not allow_concurrent:
                    raise ResourceBusyError(
                        f"Cannot start {mode} while query lease is held.",
                        context={"active_leases": [str(m) for m in active]},
                    )
                if Mode.FLUSH in active or Mode.COMPACT in active:
                    raise ResourceBusyError(
                        f"Cannot start {mode} while write/compact lease is held.",
                        context={"active_leases": [str(m) for m in active]},
                    )
            elif mode == Mode.QUERY:
                has_writer = Mode.FLUSH in active or Mode.COMPACT in active
                if has_writer and not allow_concurrent:
                    raise ResourceBusyError(
                        "Cannot start query while flush/compact lease is held.",
                        context={"active_leases": [str(m) for m in active]},
                    )

            self._lease_counts[mode] += 1
            logger.debug("Acquired lease %s (count: %d)", mode, self._lease_counts[mode])
            return ModeLease(self, mode)

    def _release_lease(self, mode: Mode) -> None:
        with self._lock:
            if self._lease_counts[mode] > 0:
                self._lease_counts[mode] -= 1
                logger.debug("Released lease %s (count: %d)", mode, self._lease_counts[mode])

    def with_lease(self, mode: Mode, fn: Callable[[], T]) -> T:
        """Executes a callable under a lease."""
        with self.lease(mode):
            return fn()

    def release_memory(self) -> None:
        """Triggers garbage collection and releases Arrow memory pool buffers."""
        gc.collect()
        pool = pa.default_memory_pool()
        pool.release_unused()
