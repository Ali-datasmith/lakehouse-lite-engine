<img width="1024" height="318" alt="image" src="https://github.com/user-attachments/assets/81678c6c-bdcf-4d85-a8ba-2b9c4a7a00a9" />

# lakehouse-lite-engine

[![Python 3.13](https://img.shields.io/badge/python-3.13-blue.svg)](https://www.python.org/downloads/release/python-3130/)
[![PyIceberg 0.12+](https://img.shields.io/badge/PyIceberg-0.12.0-blue)](https://pyiceberg.apache.org/)
[![Zero-JVM](https://img.shields.io/badge/JVM-Free-brightgreen.svg)](#1-executive-summary)
[![RAM Ceiling < 500MB](https://img.shields.io/badge/RAM_Ceiling-%3C500MB-green.svg)](#5-performance-memory--cost-benchmark-harness)
[![Ruff](https://img.shields.io/badge/code_style-ruff-000000.svg)](https://github.com/astral-sh/ruff)
[![Mypy Strict](https://img.shields.io/badge/mypy-strict-blue.svg)](https://mypy.readthedocs.io/)
[![CodeQL SAST](https://img.shields.io/badge/security-CodeQL-green.svg)](https://codeql.github.com/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

---

## 1. Executive Summary

`lakehouse-lite-engine` is an embedded, zero-JVM transactional lakehouse engine written in pure **Python 3.13**. It provides high-throughput ingestion of JSON/NDJSON micro-batches into ACID-compliant Apache Iceberg tables, serving out-of-core analytical queries concurrently via **Polars** and **DuckDB**.

By replacing JVM-bound infrastructure (PySpark, Py4J, Java runtimes) with native C-extension memory pools and zero-copy PyCapsule IPC transfers (`__arrow_c_stream__`), the engine operates within a strictly enforced **< 500 MB peak RSS RAM budget**, targeting resource-constrained containers, edge deployments, and serverless microservices on local disk or AWS S3.

---

## 2. System Architecture & Dataflow

```text
 JSON / NDJSON Micro-Batches (<= 8 MB)
                 │
                 ▼
 ┌──────────────────────────────────┐   rejected    ┌─────────────────────────────┐
 │ ingestion.MicroBatchValidator    │─────────────▶│ Ingestion DLQ (NDJSON)      │
 │ Pydantic v2 validate_json        │               │ NdjsonDeadLetterSink        │
 └─────────────────┬────────────────┘               └─────────────────────────────┘
                   │ pa.RecordBatch (<= 16 MB, schema-locked)
                   ▼
 ┌──────────────────────────────────┐
 │ buffer.CompactionBuffer          │  flush triggers: 128 MB Arrow | 500,000 rows
 │ (Arrow batches only)             │  age >= 60s | RSS soft limit | explicit
 └─────────────────┬────────────────┘
                   │ pa.Table.from_batches (zero-copy view)
                   ▼
 ┌──────────────────────────────────┐
 │ buffer.ParquetFlushWriter        │  ZSTD Level 3, 1 row group <= 128 MB
 └─────────────────┬────────────────┘
                   │ data file URI
                   ▼
 ┌──────────────────────────────────┐  Table.add_files(...)
 │ catalog.CatalogManager           │  Fast-append snapshot
 │ PyIceberg (SQL / REST)           │  Idempotent via snapshot property 'lhe.flush-id'
 └─────────────────┬────────────────┘
                   │ snapshot-pinned reads
         ┌─────────┴─────────┐
         ▼                   ▼
  PolarsAdapter         DuckDBSession
  pl.scan_iceberg       DataScan.to_arrow_batch_reader()
  (LazyFrame)           -> con.from_arrow(...) (DuckDB)
         └────── Arrow PyCapsule: __arrow_c_stream__ ──────┘

  Background: buffer.Compactor merges small Parquet files -> commit_replace
```

### Physical Design Trade-offs

| Design Axis | Traditional JVM Lakehouse | `lakehouse-lite-engine` Architecture | Physical Performance Impact |
|---|---|---|---|
| **JVM Footprint** | PySpark / Py4J / JPype dependencies | 100% Zero-JVM native C-extension pipeline | < 120 MB idle RSS baseline; zero JVM warmup delay |
| **Ingestion Pipeline** | Python dict / model object allocation | Fast-pass Rust validation directly to Arrow batches | Eliminates Python object heap overhead |
| **Object Storage Cost** | Frequent uncompacted small Parquet writes | In-memory 128 MB compaction buffer prior to commit | Compacts small batches in-memory prior to Parquet flush to minimize metadata fragmentation and small-file proliferation |
| **Cross-Engine Transfer** | Inter-process IPC or Pandas conversion | In-process Arrow PyCapsule (`__arrow_c_stream__`) | Zero-copy, 0% CPU serialization overhead |

---

## 3. Core Subsystem Deep Dives

### 3.1 Ingestion & Validation (`ingestion`)
* **Schema Authority**: `src/lakehouse_engine/ingestion/schema.py` defines `EVENTS_ARROW_SCHEMA` as the single source of truth.
* **Fast-Path Validation**: `MicroBatchValidator` executes single-pass Rust validation via `EVENT_LIST_ADAPTER.validate_json` directly on raw byte arrays.
* **Isolation Path & DLQ**: On validation error, batch lines are isolated individually. Invalid records route to `NdjsonDeadLetterSink` under `<dlq_dir>/dlq-<YYYYMMDD>-<seq:05d>.ndjson` with reason metadata (`validation`, `schema_drift`, `malformed_json`, `oversize`). Path traversal safety is enforced.
* **Circuit Breaker**: If reject ratio exceeds `max_reject_ratio` (default `0.5`) after `reject_ratio_min_sample` (default `1,000`), a `DeadLetterThresholdExceeded` exception is raised.

### 3.2 Compaction Buffer (`buffer`)
* **Memory Buffer**: `CompactionBuffer` accumulates `pa.RecordBatch` references in memory without intermediate row serialization.
* **Pre-Flush Rule**: Before accepting a batch, the buffer verifies if `current_bytes + incoming > max_bytes` (128 MB) or `current_rows + incoming_rows > max_rows` (500,000). If breached, an immediate flush is executed to guarantee row groups stay under bounds.
* **Parquet Writing**: `ParquetFlushWriter` outputs ZSTD-compressed (level 3) Parquet files with dictionary encoding, page indexes, and statistics enabled.
* **Streaming Compaction**: `Compactor` identifies files below `small_file_threshold_bytes` (64 MB), streams batch iteration, merges them into target 128 MB compressed Parquet files, verifies row count invariance from Parquet footers, and commits an atomic replacement snapshot via `commit_replace`.

### 3.3 PyIceberg Transaction Manager (`catalog`)
* **Catalog Support**: `CatalogManager` supports SQLite SQL catalogs (`sqlite:///catalog.db`) and REST catalogs (`type = "rest"`).
* **SQLite Locking**: Process-level advisory locking on `<catalog.db>.lock` ensures single-writer safety for SQLite backend instances.
* **Fast-Append Commits**: Writes Parquet files directly and registers them using `table.add_files(...)` with snapshot property `lhe.flush-id` for idempotent replay protection.
* **Schema Guard**: `assert_compatible()` enforces strict compatibility checks across field names, types, order, and nullability without automatic schema mutation.

### 3.4 Zero-Copy Serving Layer (`query`)
* **PyCapsule Integration**: `ArrowStreamExportable` protocol enables zero-copy C Data Interface stream handoffs between engines.
* **Polars Adapter**: `PolarsAdapter` uses `pl.scan_iceberg` and capability-probing adapters (`pl.scan_arrow_c_stream` and `polars.io.plugins.register_io_source`) to yield out-of-core `LazyFrame`s.
* **DuckDB Session**: Connection-scoped `DuckDBSession` enforces `memory_limit`, thread limits, spill directories, and executes parameterized queries using `con.from_arrow(source)` views. Banned SQL statements outside `SELECT`, `WITH`, `DESCRIBE`, and `EXPLAIN` trigger `UnsafeSqlError`.

---

## 4. Installation & Quickstart Guide

### System Prerequisites
* **Python**: `>= 3.13, < 3.14`
* **Package Manager**: `uv`

### Installation Commands

```bash
# Clone repository
git clone https://github.com/Ali-datasmith/lakehouse-lite-engine.git
cd lakehouse-lite-engine

# Create virtual environment and sync dependencies using uv
uv venv --python 3.13
uv sync
```

### Minimal Runnable Example

```python
from pathlib import Path
import pydantic_core
from lakehouse_engine.config import EngineSettings
from lakehouse_engine.engine import LakehouseEngine

base = Path(".lakehouse-lite").resolve()
base.mkdir(parents=True, exist_ok=True)

# 1. Initialize Engine Configuration
settings = EngineSettings.load(
    catalog={
        "uri": f"sqlite:///{(base / 'catalog.db').as_posix()}",
        "warehouse_uri": (base / "warehouse").resolve().as_uri(),
    },
    ingestion={
        "dlq_dir": base / "dlq",
    },
    query={
        "duckdb_temp_dir": base / "duckdb-spill",
    },
)

# 2. Execute Ingest, Flush, Commit, and Query Cycle
with LakehouseEngine(settings) as engine:
    events = [
        {
            "event_id": 1,
            "user_id": 100,
            "event_name": "purchase",
            "event_ts": "2026-09-30T12:00:00Z",
            "payload": {"item_id": 42, "amount": 99.99},
        }
    ]
    raw_bytes = pydantic_core.to_json(events)

    # Ingest micro-batch
    vbatch = engine.ingest(raw_bytes, source="quickstart_stream", source_offset=1)
    if vbatch is not None:
        print(f"Accepted: {vbatch.accepted}, Rejected: {vbatch.rejected}")

    # Flush buffer to Parquet and commit Iceberg snapshot
    commit = engine.flush()
    if commit is not None:
        print(f"Committed snapshot {commit.snapshot_id} with {commit.added_rows} rows")

    # Query with Polars (DataFrame collected under lease governance)
    df = engine.query.collect_polars()
    print("Polars Query Result:")
    print(df)

    # Query with DuckDB
    session = engine.query.duckdb_session()
    rel = session.sql("SELECT event_name, COUNT(*) as cnt FROM events GROUP BY event_name")
    print("DuckDB Query Result:")
    print(rel.fetch_arrow_table())
    session.close()
```

---

## 5. Performance, Memory & Cost Benchmark Harness

### Benchmarking Methodology
The benchmark harness (`python -m lakehouse_engine.benchmarks`) uses process isolation (`multiprocessing.get_context("spawn")`) to measure query latency and memory consumption across three physical layouts: `csv` (sharded CSVs), `raw_parquet` (small 10k-row Parquet files), and `iceberg_compacted` (compacted Parquet files registered in Iceberg).

A background sampler (`RssSampler`) records physical process RSS every 10 ms to enforce the **500 MB peak RAM limit**.

### Verified Benchmark Results

All performance numbers are generated directly from the repository benchmark harness (`uv run python -m lakehouse_engine.benchmarks --rows 100000 --runs 2 --out ./bench-out`).

#### Benchmark Metadata
- **Git Commit SHA**: `59ffaebcbe1f0df20ae861d92af3da7d13ad6d01`
- **Timestamp (UTC)**: `2026-10-02T18:42:00.000000+00:00`
- **Command Used**: `uv run python -m lakehouse_engine.benchmarks --rows 100000 --runs 2 --out ./bench-out`
- **OS**: `linux` | **CPU Count**: `4`
- **Python Version**: `3.13.12`
- **PyArrow Version**: `25.0.1`
- **Polars Version**: `1.44.2`
- **DuckDB Version**: `1.5.6`
- **PyIceberg Version**: `0.12.0`
- **Seed**: `20260930`
- **Rows**: `100,000`
- **Runs**: `2`

#### Verified Performance Results Table

| Layout | Engine | Query | p50 (ms) | p95 (ms) | Min (ms) | Max (ms) | Peak RSS (MB) |
|---|---|---|---|---|---|---|---|
| `csv` | `duckdb` | `Q1_point` | 1435.76 | 1435.76 | 1417.37 | 1435.76 | 211.6 |
| `csv` | `duckdb` | `Q2_range_agg` | 1449.70 | 1449.70 | 1416.71 | 1449.70 | 205.3 |
| `csv` | `duckdb` | `Q3_topn` | 1417.50 | 1417.50 | 1412.01 | 1417.50 | 212.9 |
| `csv` | `polars` | `Q1_point` | 20.37 | 20.37 | 13.91 | 20.37 | 211.1 |
| `csv` | `polars` | `Q2_range_agg` | 14.78 | 14.78 | 13.88 | 14.78 | 201.0 |
| `csv` | `polars` | `Q3_topn` | 16.90 | 16.90 | 16.29 | 16.90 | 207.9 |
| `raw_parquet` | `duckdb` | `Q1_point` | 20.61 | 20.61 | 20.57 | 20.61 | 181.1 |
| `raw_parquet` | `duckdb` | `Q2_range_agg` | 21.30 | 21.30 | 20.73 | 21.30 | 182.2 |
| `raw_parquet` | `duckdb` | `Q3_topn` | 28.21 | 28.21 | 26.96 | 28.21 | 191.7 |
| `raw_parquet` | `polars` | `Q1_point` | 10.53 | 10.53 | 4.23 | 10.53 | 199.7 |
| `raw_parquet` | `polars` | `Q2_range_agg` | 6.04 | 6.04 | 5.25 | 6.04 | 197.8 |
| `raw_parquet` | `polars` | `Q3_topn` | 11.21 | 11.21 | 11.16 | 11.21 | 206.9 |
| `iceberg_compacted` | `duckdb` | `Q1_point` | 94.37 | 94.37 | 93.92 | 94.37 | 240.3 |
| `iceberg_compacted` | `duckdb` | `Q2_range_agg` | 71.79 | 71.79 | 66.50 | 71.79 | 234.3 |
| `iceberg_compacted` | `duckdb` | `Q3_topn` | 170.44 | 170.44 | 83.08 | 170.44 | 253.5 |
| `iceberg_compacted` | `polars` | `Q1_point` | 31.29 | 31.29 | 30.68 | 31.29 | 216.9 |
| `iceberg_compacted` | `polars` | `Q2_range_agg` | 28.48 | 28.48 | 28.26 | 28.48 | 208.8 |
| `iceberg_compacted` | `polars` | `Q3_topn` | 34.40 | 34.40 | 31.11 | 34.40 | 219.3 |

For full benchmark artifact details and raw output, refer to [bench-out/report.md](bench-out/report.md) and [bench-out/results.json](bench-out/results.json).

---

## 6. Developer Experience & Quality Assurance

### Local Quality Commands

```bash
# Run Mypy strict type checking
uv run mypy src/lakehouse_engine

# Run Ruff linter and format checker
uv run ruff check src tests
uv run ruff format --check src tests

# Run full Pytest test suite with coverage
uv run pytest

# Execute Benchmark Harness
uv run python -m lakehouse_engine.benchmarks --rows 100000 --runs 2 --out ./bench-out
```

### Test Suite Architecture Overview

* `tests/unit/test_config.py`: Invariant validation, default checking, secret redaction.
* `tests/unit/test_ingestion.py`: MicroBatchValidator fast path, isolation path, and DLQ output.
* `tests/unit/test_buffer.py`: CompactionBuffer append, flush triggers, pre-flush rule, schema mismatch.
* `tests/contract/test_contracts.py`: Pinning third-party behavior (`CT-01` through `CT-10`), fast-append commits, zero-copy PyCapsule address equality, DuckDB stream registration.
* `tests/property/test_properties.py`: Hypothesis property tests for Arrow schema and conversion invariants.
* `tests/memory/test_memory.py`: RSS-gated memory limit tests (`MT-01` through `MT-05`) verifying <500 MB peak RAM.
* `tests/integration/test_pipeline.py`: Full end-to-end ingest -> flush -> commit -> query -> compact cycle.
* `tests/test_no_legacy.py`: AST scanner enforcing `G-LEGACY` zero-legacy rules.

---

## 7. CI/CD, Security & Governance

### GitHub Actions Workflows
1. **CI Pipeline** (`.github/workflows/ci.yml`):
   * Runs on Python 3.13.
   * Executes Ruff linting, Mypy strict type checking, Pytest suite with XML coverage reporting, and a smoke benchmark run.
2. **CodeQL SAST Security Workflow** (`.github/workflows/codeql-sast.yml`):
   * Runs automated static security analysis for the Python language stack on every push and PR to `main`.

### Security Policy
Vulnerabilities are managed according to [`SECURITY.md`](SECURITY.md). Reports receive an initial response within 48 hours.

---

## 8. License & Author Contacts

### License
This project is licensed under the [Apache-2.0 License](LICENSE).

### Maintainer Profile & Contacts

* **Author**: Ali Datasmith
* **LinkedIn**: [https://www.linkedin.com/in/ali-datasmith/](https://www.linkedin.com/in/ali-datasmith/)
* **Primary Security Contact**: `rajputmuhammadali979@gmail.com`
* **Secondary Contact**: `rjptmhmmd@gmail.com`
