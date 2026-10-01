# tests/memory/test_memory.py
import pyarrow as pa
import pydantic_core
import pytest

from lakehouse_engine.benchmarks.memory import RssSampler
from lakehouse_engine.engine import LakehouseEngine


@pytest.mark.memory
def test_mt01_mt02_ingest_rss_and_arrow_memory(test_engine_settings):
    with RssSampler() as sampler:
        with LakehouseEngine(test_engine_settings) as engine:
            events = [
                {
                    "event_id": i,
                    "user_id": i % 100,
                    "event_name": "test_event",
                    "event_ts": "2026-09-30T12:00:00Z",
                    "payload": {"data": "x" * 100},
                }
                for i in range(1, 1001)
            ]
            raw = pydantic_core.to_json(events)

            for _ in range(5):
                engine.ingest(raw, source="mt_test")
                engine.flush()

                # MT-02: After each flush, pyarrow allocated bytes <= 32 MB
                assert pa.total_allocated_bytes() <= 32 * 1024 * 1024

        # MT-01: Peak RSS <= 500 MB
        assert sampler.peak_bytes <= 500 * 1024 * 1024


@pytest.mark.memory
def test_mt05_governor_memory_pressure_flush(test_engine_settings):
    # Lower soft limit to force memory pressure
    low_settings = test_engine_settings.model_copy(
        update={
            "runtime": test_engine_settings.runtime.model_copy(
                update={"memory_soft_limit_bytes": 1024 * 1024}  # 1 MB soft limit
            )
        }
    )
    with LakehouseEngine(low_settings) as engine:
        events = [
            {
                "event_id": i,
                "user_id": i,
                "event_name": "evt",
                "event_ts": "2026-09-30T12:00:00Z",
            }
            for i in range(1, 100)
        ]
        raw = pydantic_core.to_json(events)
        res = engine.ingest(raw, source="pressure_test")
        assert res is not None
