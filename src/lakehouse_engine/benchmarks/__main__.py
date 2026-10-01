# src/lakehouse_engine/benchmarks/__main__.py
import argparse
from pathlib import Path

from lakehouse_engine.benchmarks.harness import BenchmarkHarness
from lakehouse_engine.benchmarks.report import BenchmarkReportWriter
from lakehouse_engine.config import BenchmarkSettings


def main() -> None:
    parser = argparse.ArgumentParser(description="Lakehouse Engine Benchmarks")
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--out", type=str, default="./bench-out")
    args = parser.parse_args()

    settings = BenchmarkSettings(
        rows=args.rows,
        runs=args.runs,
        output_dir=Path(args.out),
    )

    harness = BenchmarkHarness(settings)
    results = []

    for layout in ("csv", "raw_parquet", "iceberg_compacted"):
        for engine in ("duckdb", "polars"):
            for query in ("Q1_point", "Q2_range_agg", "Q3_topn"):
                res = harness.run_cell(layout, engine, query)
                results.append(res)

    writer = BenchmarkReportWriter(settings.output_dir, seed=settings.seed)
    writer.write(results)


if __name__ == "__main__":
    main()
