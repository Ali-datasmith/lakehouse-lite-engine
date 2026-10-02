# src/lakehouse_engine/benchmarks/harness.py
import multiprocessing as mp
import tempfile
import time
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import csv

from lakehouse_engine.benchmarks.datasets import generate_streaming_batches
from lakehouse_engine.benchmarks.memory import MemoryReport, RssSampler
from lakehouse_engine.benchmarks.report import BenchmarkCellResult, QueryTiming
from lakehouse_engine.benchmarks.scenarios import (
    run_q1_point_duckdb,
    run_q1_point_polars,
    run_q2_range_agg_duckdb,
    run_q2_range_agg_polars,
    run_q3_topn_duckdb,
    run_q3_topn_polars,
)
from lakehouse_engine.config import BenchmarkSettings, EngineSettings
from lakehouse_engine.engine import LakehouseEngine


def _subprocess_worker(
    layout: str,
    engine: str,
    query: str,
    settings: BenchmarkSettings,
    queue: mp.Queue,  # type: ignore[type-arg]
) -> None:
    timings: list[float] = []

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        # Prepare dataset based on layout
        if layout in ("csv", "raw_parquet"):
            for file_idx, batch in enumerate(
                generate_streaming_batches(
                    settings.rows, batch_rows=settings.small_file_rows, seed=settings.seed
                )
            ):
                if layout == "csv":
                    fp = data_dir / f"file_{file_idx:05d}.csv"
                    csv.write_csv(batch, fp)
                else:
                    fp = data_dir / f"file_{file_idx:05d}.parquet"
                    pq.write_table(pa_table_from_batch(batch), fp, compression="zstd")
        elif layout == "iceberg_compacted":
            e_settings = EngineSettings.load(
                catalog={
                    "uri": f"sqlite:///{tmp_path}/catalog.db",
                    "warehouse_uri": f"file://{tmp_path}/warehouse",
                },
                ingestion={"dlq_dir": tmp_path / "dlq"},
                query={
                    "duckdb_temp_dir": tmp_path / "duckdb-spill",
                    "polars_strategy": "parquet_files",
                },
            )
            with LakehouseEngine(e_settings) as lhe:
                for batch in generate_streaming_batches(
                    settings.rows, batch_rows=20000, seed=settings.seed
                ):
                    lhe._buffer.append(batch)
                lhe.flush()

        with RssSampler() as sampler:
            # Warmups
            for _ in range(settings.warmups):
                _execute_scenario(layout, engine, query, tmp_path, data_dir)

            # Runs
            for _ in range(settings.runs):
                t0 = time.perf_counter_ns()
                _execute_scenario(layout, engine, query, tmp_path, data_dir)
                t1 = time.perf_counter_ns()
                timings.append((t1 - t0) / 1e6)

            mem_report = sampler.get_report()

    timings.sort()
    p50 = timings[len(timings) // 2]
    p95 = timings[int(len(timings) * 0.95)]

    queue.put(
        {
            "p50_ms": p50,
            "p95_ms": p95,
            "min_ms": min(timings),
            "max_ms": max(timings),
            "memory": mem_report,
        }
    )


def pa_table_from_batch(batch: pa.RecordBatch) -> pa.Table:
    return pa.Table.from_batches([batch])


def _execute_csv(engine: str, query: str, data_dir: Path) -> None:
    if engine == "duckdb":
        import duckdb

        con = duckdb.connect(":memory:")
        con.execute(f"CREATE VIEW events AS SELECT * FROM read_csv_auto('{data_dir}/*.csv')")  # noqa: S608
        if query == "Q1_point":
            con.execute("SELECT * FROM events WHERE event_id = 42")
        elif query == "Q2_range_agg":
            con.execute("SELECT event_name, COUNT(*) FROM events GROUP BY event_name")
        elif query == "Q3_topn":
            q = (
                "SELECT user_id, COUNT(*) as cnt FROM events "
                "GROUP BY user_id ORDER BY cnt DESC LIMIT 100"
            )
            con.execute(q)
        con.close()
    elif engine == "polars":
        lf = pl.scan_csv(f"{data_dir}/*.csv")
        if query == "Q1_point":
            run_q1_point_polars(lf)
        elif query == "Q2_range_agg":
            run_q2_range_agg_polars(lf)
        elif query == "Q3_topn":
            run_q3_topn_polars(lf)


def _execute_raw_parquet(engine: str, query: str, data_dir: Path) -> None:
    if engine == "duckdb":
        import duckdb

        con = duckdb.connect(":memory:")
        con.execute(f"CREATE VIEW events AS SELECT * FROM read_parquet('{data_dir}/*.parquet')")  # noqa: S608
        if query == "Q1_point":
            con.execute("SELECT * FROM events WHERE event_id = 42")
        elif query == "Q2_range_agg":
            con.execute("SELECT event_name, COUNT(*) FROM events GROUP BY event_name")
        elif query == "Q3_topn":
            q = (
                "SELECT user_id, COUNT(*) as cnt FROM events "
                "GROUP BY user_id ORDER BY cnt DESC LIMIT 100"
            )
            con.execute(q)
        con.close()
    elif engine == "polars":
        lf = pl.scan_parquet(f"{data_dir}/*.parquet")
        if query == "Q1_point":
            run_q1_point_polars(lf)
        elif query == "Q2_range_agg":
            run_q2_range_agg_polars(lf)
        elif query == "Q3_topn":
            run_q3_topn_polars(lf)


def _execute_iceberg(engine: str, query: str, tmp_path: Path) -> None:
    e_settings = EngineSettings.load(
        catalog={
            "uri": f"sqlite:///{tmp_path}/catalog.db",
            "warehouse_uri": f"file://{tmp_path}/warehouse",
        },
        ingestion={"dlq_dir": tmp_path / "dlq"},
        query={
            "duckdb_temp_dir": tmp_path / "duckdb-spill",
            "polars_strategy": "parquet_files",
        },
    )
    with LakehouseEngine(e_settings) as lhe:
        if engine == "duckdb":
            session = lhe.query.duckdb_session()
            if query == "Q1_point":
                run_q1_point_duckdb(session)
            elif query == "Q2_range_agg":
                run_q2_range_agg_duckdb(session)
            elif query == "Q3_topn":
                run_q3_topn_duckdb(session)
            session.close()
        elif engine == "polars":
            lf_iceberg = lhe.query.polars_lazy()
            if query == "Q1_point":
                run_q1_point_polars(lf_iceberg)
            elif query == "Q2_range_agg":
                run_q2_range_agg_polars(lf_iceberg)
            elif query == "Q3_topn":
                run_q3_topn_polars(lf_iceberg)


def _execute_scenario(layout: str, engine: str, query: str, tmp_path: Path, data_dir: Path) -> None:
    if layout == "csv":
        _execute_csv(engine, query, data_dir)
    elif layout == "raw_parquet":
        _execute_raw_parquet(engine, query, data_dir)
    elif layout == "iceberg_compacted":
        _execute_iceberg(engine, query, tmp_path)


class BenchmarkHarness:
    def __init__(self, settings: BenchmarkSettings) -> None:
        self._settings = settings

    def run_cell(
        self,
        layout: str,
        engine: str,
        query: str,
    ) -> BenchmarkCellResult:
        ctx = mp.get_context("spawn")
        queue = ctx.Queue()

        proc = ctx.Process(
            target=_subprocess_worker,
            args=(layout, engine, query, self._settings, queue),
        )
        proc.start()
        proc.join()

        if proc.exitcode != 0:
            raise RuntimeError(f"Subprocess failed with exit code {proc.exitcode}")

        res = queue.get()
        mem: MemoryReport = res["memory"]

        if mem.peak_rss_bytes > 500 * 1024 * 1024:
            raise RuntimeError(f"Memory budget exceeded: Peak RSS {mem.peak_rss_bytes} > 500MB")

        timing = QueryTiming(
            p50_ms=res["p50_ms"],
            p95_ms=res["p95_ms"],
            min_ms=res["min_ms"],
            max_ms=res["max_ms"],
        )

        return BenchmarkCellResult(
            layout=layout,
            engine=engine,
            query=query,
            timing=timing,
            memory=mem,
            s3_requests={"GET": 0, "PUT": 0, "HEAD": 0, "LIST": 0},
            est_usd_per_1k=0.0,
        )
