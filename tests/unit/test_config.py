# tests/unit/test_config.py
import pytest

from lakehouse_engine.config import EngineSettings
from lakehouse_engine.exceptions import ConfigurationError


def test_config_defaults():
    settings = EngineSettings.load()
    assert settings.runtime.process_ceiling_bytes == 500 * 1024 * 1024
    assert settings.buffer.max_bytes == 128 * 1024 * 1024
    assert settings.ingestion.max_batch_rows == 20000


def test_config_invalid_invariant():
    with pytest.raises(ConfigurationError):
        EngineSettings.load(
            runtime={
                "memory_hard_limit_bytes": 100 * 1024 * 1024,
                "memory_soft_limit_bytes": 200 * 1024 * 1024,
            }
        )


def test_secret_redaction():
    settings = EngineSettings.load(storage={"s3_access_key_id": "SUPER_SECRET_KEY"})
    assert "SUPER_SECRET_KEY" not in repr(settings.storage)
