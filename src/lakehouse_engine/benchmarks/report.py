# src/lakehouse_engine/benchmarks/report.py
import json
import os
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import duckdb
import polars as pl
import pyarrow as pa
import pyiceberg

from lakehouse_engine.benchmarks.memory import MemoryReport


@dataclass(frozen=True, slots=True)
class QueryTiming:
    p50_ms: float
    p95_ms: float
    min_ms: float
    max_ms: float


@dataclass(frozen=True, slots=True)
class BenchmarkCellResult:
    layout: str
    engine: str
    query: str
    timing: QueryTiming
    memory: MemoryReport
    s3_requests: dict[str, int]
    est_usd_per_1k: float


@dataclass(frozen=True, slots=True)
class BenchmarkHeader:
    seed: int
    python_version: str
    pyarrow_version: str
    polars_version: str
    duckdb_version: str
    pyiceberg_version: str
    cpu_count: int
    os_name: str


class BenchmarkReportWriter:
    def __init__(self, output_dir: Path, seed: int = 20260930) -> None:
        self._output_dir = output_dir
        self._output_dir.mkdir(parents=True, exist_ok=True)
        self._seed = seed

    def build_header(self) -> BenchmarkHeader:
        return BenchmarkHeader(
            seed=self._seed,
            python_version=sys.version,
            pyarrow_version=pa.__version__,
            polars_version=pl.__version__,
            duckdb_version=duckdb.__version__,
            pyiceberg_version=pyiceberg.__version__,
            cpu_count=os.cpu_count() or 1,
            os_name=sys.platform,
        )

    def write(self, results: list[BenchmarkCellResult]) -> None:
        header = self.build_header()

        data: dict[str, Any] = {
            "header": asdict(header),
            "results": [asdict(r) for r in results],
        }

        json_path = self._output_dir / "results.json"
        with json_path.open("w") as f:
            json.dump(data, f, indent=2)

        md_path = self._output_dir / "report.md"
        with md_path.open("w") as f:
            f.write("# Lakehouse Engine Benchmark Report\n\n")
            f.write(f"**Seed:** {header.seed}\n")
            f.write(f"**OS:** {header.os_name} | **CPUs:** {header.cpu_count}\n\n")
            f.write("## Results\n\n")
            f.write("| Layout | Engine | Query | p50 (ms) | p95 (ms) | Peak RSS (MB) |\n")
            f.write("|---|---|---|---|---|---|\n")
            for r in results:
                rss_mb = r.memory.peak_rss_bytes / (1024 * 1024)
                f.write(
                    f"| {r.layout} | {r.engine} | {r.query} | "
                    f"{r.timing.p50_ms:.2f} | {r.timing.p95_ms:.2f} | {rss_mb:.1f} |\n"
                )
