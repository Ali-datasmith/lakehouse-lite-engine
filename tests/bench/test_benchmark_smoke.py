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
