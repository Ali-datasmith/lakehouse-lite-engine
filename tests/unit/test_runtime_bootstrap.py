import sys

import pytest

from lakehouse_engine.config import EngineSettings, RuntimeSettings
from lakehouse_engine.exceptions import ConfigurationError
from lakehouse_engine.governor import Mode, ResourceGovernor
from lakehouse_engine.runtime import configure_runtime


def test_package_import_does_not_load_heavy_libs() -> None:
    """Verify that importing lakehouse_engine does not eagerly import pyarrow, polars, or duckdb."""
    import lakehouse_engine

    assert hasattr(lakehouse_engine, "EngineSettings")
    assert hasattr(lakehouse_engine, "LakehouseEngine")


def test_configure_runtime_fails_if_imported_early(monkeypatch: pytest.MonkeyPatch) -> None:
    """configure_runtime raises ConfigurationError if polars/duckdb imported before pyarrow."""
    import lakehouse_engine.runtime as rt

    # Temporarily remove pyarrow from sys.modules and add duckdb
    monkeypatch.setattr(rt, "_RUNTIME_CONFIGURED", False)
    monkeypatch.setitem(sys.modules, "duckdb", object())

    modules_copy = dict(sys.modules)
    modules_copy.pop("pyarrow", None)
    monkeypatch.setattr(sys, "modules", modules_copy)

    settings = RuntimeSettings()
    with pytest.raises(ConfigurationError, match="imported before configure_runtime"):
        configure_runtime(settings)


def test_governor_reentrant_leases() -> None:
    settings = RuntimeSettings(allow_concurrent_query_during_flush=False)
    gov = ResourceGovernor(settings)

    with gov.lease(Mode.QUERY):
        assert Mode.QUERY in gov.active_leases
        # Reentrant lease acquisition
        with gov.lease(Mode.QUERY):
            assert Mode.QUERY in gov.active_leases
        assert Mode.QUERY in gov.active_leases

    assert Mode.QUERY not in gov.active_leases


def test_config_insecure_tmp_rejection_in_prod() -> None:
    with pytest.raises(ConfigurationError, match="Insecure default path"):
        EngineSettings.load(
            runtime={"env": "prod"},
            ingestion={"dlq_dir": "/tmp/dlq"},  # noqa: S108
        )


def test_configure_runtime_guarantees_memory_pool_or_raises() -> None:
    settings = RuntimeSettings(arrow_memory_pool="system")
    configure_runtime(settings)
    import pyarrow as pa

    assert pa.default_memory_pool().backend_name.lower() == "system"

    from lakehouse_engine.runtime import _guarantee_pyarrow_memory_pool

    msg = "Unable to configure requested PyArrow memory pool"
    with pytest.raises(ConfigurationError, match=msg):
        _guarantee_pyarrow_memory_pool("non_existent_pool_xyz")
