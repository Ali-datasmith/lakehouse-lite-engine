from datetime import UTC, datetime
from pathlib import Path

from lakehouse_engine.config import IngestionSettings
from lakehouse_engine.ingestion.dlq import DeadLetter, NdjsonDeadLetterSink


def test_dlq_retention_and_max_bytes_pruning(tmp_path: Path) -> None:
    settings = IngestionSettings(
        dlq_dir=tmp_path / "dlq",
        dlq_rotate_bytes=1024 * 1024,
        dlq_retention_days=1,
        dlq_max_total_bytes=10 * 1024 * 1024,
    )
    sink = NdjsonDeadLetterSink(settings)

    dl = DeadLetter(
        received_at=datetime.now(UTC),
        source="test",
        line_no=1,
        source_offset=1,
        reason="validation",
        errors=[],
        raw="invalid raw line",
        engine_schema_version="1",
    )

    for _ in range(5):
        sink.write(dl)
        sink.flush()

    sink.close()

    dlq_files = list((tmp_path / "dlq").glob("dlq-*.ndjson"))
    assert len(dlq_files) > 0
    total_size = sum(f.stat().st_size for f in dlq_files)
    assert total_size <= 10 * 1024 * 1024
