# Real Benchmark Report - lakehouse-lite-engine

- **Git Commit SHA**: `20ec8549bd8fcc8cb280d4e2b7b06570765c9a49`
- **Timestamp (UTC)**: `2026-10-02T15:08:37.823744+00:00`
- **Command Used**: `python -m lakehouse_engine.benchmarks`
- **OS**: `linux` | **CPU Count**: `4`
- **Python Version**: `3.13.12`
- **PyArrow Version**: `25.0.1`
- **Polars Version**: `1.44.2`
- **DuckDB Version**: `1.5.6`
- **PyIceberg Version**: `0.12.0`
- **Seed**: `20260930`

## Verified Performance Results

| Layout | Engine | Query | p50 (ms) | p95 (ms) | Min (ms) | Max (ms) | Peak RSS (MB) |
|---|---|---|---|---|---|---|---|
| `csv` | `duckdb` | `Q1_point` | 1301.21 | 1301.21 | 1291.20 | 1301.21 | 209.2 |
| `csv` | `duckdb` | `Q2_range_agg` | 1322.70 | 1322.70 | 1297.17 | 1322.70 | 209.3 |
| `csv` | `duckdb` | `Q3_topn` | 1347.27 | 1347.27 | 1314.35 | 1347.27 | 220.0 |
| `csv` | `polars` | `Q1_point` | 15.62 | 15.62 | 13.63 | 15.62 | 210.6 |
| `csv` | `polars` | `Q2_range_agg` | 14.69 | 14.69 | 13.64 | 14.69 | 206.3 |
| `csv` | `polars` | `Q3_topn` | 24.05 | 24.05 | 23.68 | 24.05 | 208.6 |
| `raw_parquet` | `duckdb` | `Q1_point` | 21.43 | 21.43 | 20.61 | 21.43 | 182.2 |
| `raw_parquet` | `duckdb` | `Q2_range_agg` | 21.00 | 21.00 | 20.76 | 21.00 | 184.0 |
| `raw_parquet` | `duckdb` | `Q3_topn` | 27.05 | 27.05 | 26.92 | 27.05 | 192.1 |
| `raw_parquet` | `polars` | `Q1_point` | 4.77 | 4.77 | 4.18 | 4.77 | 199.9 |
| `raw_parquet` | `polars` | `Q2_range_agg` | 5.72 | 5.72 | 5.05 | 5.72 | 199.5 |
| `raw_parquet` | `polars` | `Q3_topn` | 12.72 | 12.72 | 11.37 | 12.72 | 208.8 |
| `iceberg_compacted` | `duckdb` | `Q1_point` | 94.32 | 94.32 | 93.75 | 94.32 | 264.3 |
| `iceberg_compacted` | `duckdb` | `Q2_range_agg` | 153.74 | 153.74 | 65.65 | 153.74 | 274.0 |
| `iceberg_compacted` | `duckdb` | `Q3_topn` | 119.15 | 119.15 | 82.90 | 119.15 | 290.5 |
| `iceberg_compacted` | `polars` | `Q1_point` | 31.69 | 31.69 | 30.78 | 31.69 | 267.7 |
| `iceberg_compacted` | `polars` | `Q2_range_agg` | 28.74 | 28.74 | 28.13 | 28.74 | 265.1 |
| `iceberg_compacted` | `polars` | `Q3_topn` | 33.15 | 33.15 | 30.66 | 33.15 | 266.4 |
