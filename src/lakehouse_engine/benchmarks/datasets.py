# src/lakehouse_engine/benchmarks/datasets.py
import random
from collections.abc import Generator
from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pydantic_core

from lakehouse_engine.ingestion.schema import EVENTS_ARROW_SCHEMA


def generate_streaming_batches(
    total_rows: int,
    *,
    batch_rows: int = 100_000,
    seed: int = 20260930,
) -> Generator[pa.RecordBatch]:
    """Generates synthetic Event RecordBatches in a streaming fashion."""
    rng = random.Random(seed)  # noqa: S311
    base_ts = datetime(2026, 9, 30, 0, 0, 0, tzinfo=UTC)

    event_names = ["click", "page_view", "purchase", "add_to_cart", "search"]

    generated = 0
    while generated < total_rows:
        current_batch_sz = min(batch_rows, total_rows - generated)

        event_ids = list(range(generated + 1, generated + current_batch_sz + 1))
        user_ids = [rng.randint(1, 100_000) for _ in range(current_batch_sz)]
        names = [rng.choice(event_names) for _ in range(current_batch_sz)]
        tss = [
            base_ts + timedelta(seconds=rng.randint(0, 86400 * 30)) for _ in range(current_batch_sz)
        ]

        payloads: list[str | None] = []
        for _ in range(current_batch_sz):
            if rng.random() < 0.2:  # noqa: PLR2004
                payloads.append(None)
            else:
                p_dict = {"page": "/item", "price": round(rng.uniform(1.0, 500.0), 2)}
                payloads.append(pydantic_core.to_json(p_dict).decode("utf-8"))

        batch = pa.RecordBatch.from_arrays(
            [
                pa.array(event_ids, type=pa.int64()),
                pa.array(user_ids, type=pa.int64()),
                pa.array(names, type=pa.string()),
                pa.array(tss, type=pa.timestamp("us", tz="UTC")),
                pa.array(payloads, type=pa.string()),
            ],
            schema=EVENTS_ARROW_SCHEMA,
        )

        generated += current_batch_sz
        yield batch
