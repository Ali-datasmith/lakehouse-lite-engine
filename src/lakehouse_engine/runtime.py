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


def _guarantee_pyarrow_memory_pool(requested_pool: str) -> None:
    import pyarrow as pa

    requested = requested_pool.lower().strip()
    current = pa.default_memory_pool().backend_name.lower()
    if current == requested:
        return

    pool_factory = getattr(pa, f"{requested}_memory_pool", None)
    if pool_factory is not None:
        try:
            pool = pool_factory()
            pa.set_memory_pool(pool)
            if pa.default_memory_pool().backend_name.lower() == requested:
                return
        except Exception as exc:
            raise ConfigurationError(
                f"Failed to set PyArrow memory pool to '{requested}': {exc}"
            ) from exc

    raise ConfigurationError(
        f"Unable to configure requested PyArrow memory pool '{requested}'. "
        f"Current memory pool is '{current}' and available backends are "
        f"{pa.supported_memory_backends()}."
    )


def configure_runtime(settings: "RuntimeSettings") -> None:
    """Configures environment and PyArrow runtime settings.

    Sets ARROW_DEFAULT_MEMORY_POOL, POLARS_MAX_THREADS, PYICEBERG_MAX_WORKERS.
    Guarantees PyArrow memory pool configuration or raises ConfigurationError.
    Idempotent across multiple calls.
    """
    global _RUNTIME_CONFIGURED

    if _RUNTIME_CONFIGURED:
        if "pyarrow" in sys.modules:
            import pyarrow as pa

            pa.set_cpu_count(settings.cpu_threads)
            pa.set_io_thread_count(settings.io_threads)
            _guarantee_pyarrow_memory_pool(settings.arrow_memory_pool)
        return

    os.environ.setdefault("ARROW_DEFAULT_MEMORY_POOL", settings.arrow_memory_pool)
    os.environ.setdefault("POLARS_MAX_THREADS", str(settings.cpu_threads))
    os.environ.setdefault("PYICEBERG_MAX_WORKERS", str(settings.cpu_threads))

    if "pyarrow" in sys.modules:
        import pyarrow as pa

        pa.set_cpu_count(settings.cpu_threads)
        pa.set_io_thread_count(settings.io_threads)
        _guarantee_pyarrow_memory_pool(settings.arrow_memory_pool)
    else:
        for mod in ("polars", "duckdb"):
            if mod in sys.modules:
                raise ConfigurationError(
                    f"Module {mod} was imported before configure_runtime was called."
                )

        import pyarrow as pa

        pa.set_cpu_count(settings.cpu_threads)
        pa.set_io_thread_count(settings.io_threads)
        _guarantee_pyarrow_memory_pool(settings.arrow_memory_pool)

    _RUNTIME_CONFIGURED = True


def configure_runtime_or_fail(settings: "RuntimeSettings") -> None:
    """Convenience wrapper around configure_runtime."""
    configure_runtime(settings)
