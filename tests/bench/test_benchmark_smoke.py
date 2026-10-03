from pathlib import Path

from lakehouse_engine.benchmarks.harness import BenchmarkHarness
from lakehouse_engine.benchmarks.report import BenchmarkReportWriter
from lakehouse_engine.config import BenchmarkSettings


def test_benchmark_smoke_and_report_generation(tmp_path: Path) -> None:
    settings = BenchmarkSettings(
        rows=100_000,
        small_file_rows=1_000,
        warmups=0,
        runs=1,
        output_dir=tmp_path / "bench-out",
    )
    harness = BenchmarkHarness(settings)

    # Smoke test cell
    cell_res = harness.run_cell("raw_parquet", "polars", "Q1_point")
    assert cell_res.layout == "raw_parquet"
    assert cell_res.engine == "polars"
    assert cell_res.query == "Q1_point"

    writer = BenchmarkReportWriter(settings.output_dir, seed=settings.seed)
    writer.write([cell_res])

    assert (tmp_path / "bench-out" / "results.json").exists()
    assert (tmp_path / "bench-out" / "report.md").exists()


def test_append_arrow_batch_public_api(tmp_path: Path) -> None:
    import pyarrow as pa

    from lakehouse_engine.config import EngineSettings
    from lakehouse_engine.engine import LakehouseEngine
    from lakehouse_engine.ingestion.schema import EVENTS_ARROW_SCHEMA

    settings = EngineSettings.load(
        catalog={
            "uri": f"sqlite:///{tmp_path}/cat.db",
            "warehouse_uri": f"file://{tmp_path}/wh",
        },
        ingestion={"dlq_dir": tmp_path / "dlq"},
        query={"duckdb_temp_dir": tmp_path / "spill"},
    )
    with LakehouseEngine(settings) as engine:
        batch = pa.RecordBatch.from_arrays(
            [
                pa.array([1], type=pa.int64()),
                pa.array([10], type=pa.int64()),
                pa.array(["evt"], type=pa.string()),
                pa.array([1000000], type=pa.timestamp("us", tz="UTC")),
                pa.array(["{}"], type=pa.string()),
            ],
            schema=EVENTS_ARROW_SCHEMA,
        )
        res = engine.append_arrow_batch(batch)
        assert res is None or hasattr(res, "snapshot_id")
