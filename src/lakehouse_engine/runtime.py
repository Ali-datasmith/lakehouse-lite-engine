# src/lakehouse_engine/runtime.py
import os
import sys
from typing import TYPE_CHECKING

from lakehouse_engine.exceptions import ConfigurationError

if TYPE_CHECKING:
    from lakehouse_engine.config import RuntimeSettings

_RUNTIME_CONFIGURED: bool = False


def is_runtime_configured() -> bool:
    """Check if configure_runtime has been executed in the current process."""
    return _RUNTIME_CONFIGURED


def configure_runtime(settings: "RuntimeSettings") -> None:
    """MUST be called before importing pyarrow, polars or duckdb in the process.

    Sets ARROW_DEFAULT_MEMORY_POOL, POLARS_MAX_THREADS, PYICEBERG_MAX_WORKERS via
    os.environ.setdefault, then (after import) calls pyarrow.set_cpu_count(n) and
    pyarrow.set_io_thread_count(n).
    Raises ConfigurationError if polars/duckdb were imported before pyarrow.
    """
    global _RUNTIME_CONFIGURED

    os.environ.setdefault("ARROW_DEFAULT_MEMORY_POOL", settings.arrow_memory_pool)
    os.environ.setdefault("POLARS_MAX_THREADS", str(settings.cpu_threads))
    os.environ.setdefault("PYICEBERG_MAX_WORKERS", str(settings.cpu_threads))

    if "pyarrow" in sys.modules:
        import pyarrow as pa

        pa.set_cpu_count(settings.cpu_threads)
        pa.set_io_thread_count(settings.io_threads)
    else:
        for mod in ("polars", "duckdb"):
            if mod in sys.modules:
                raise ConfigurationError(
                    f"Module {mod} was imported before configure_runtime was called."
                )

        import pyarrow as pa

        pa.set_cpu_count(settings.cpu_threads)
        pa.set_io_thread_count(settings.io_threads)

    _RUNTIME_CONFIGURED = True


def configure_runtime_or_fail(settings: "RuntimeSettings") -> None:
    """Convenience wrapper around configure_runtime."""
    configure_runtime(settings)
