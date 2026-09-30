# Lakehouse Engine Benchmark Report

**Seed:** 20260930
**OS:** linux | **CPUs:** 4

## Results

| Layout | Engine | Query | p50 (ms) | p95 (ms) | Peak RSS (MB) |
|---|---|---|---|---|---|
| csv | duckdb | Q1_point | 1.10 | 1.10 | 150.2 |
| csv | duckdb | Q2_range_agg | 1.14 | 1.14 | 150.1 |
| csv | duckdb | Q3_topn | 1.13 | 1.13 | 150.0 |
| csv | polars | Q1_point | 1.13 | 1.13 | 150.6 |
| csv | polars | Q2_range_agg | 1.11 | 1.11 | 149.7 |
| csv | polars | Q3_topn | 1.17 | 1.17 | 150.4 |
| raw_parquet | duckdb | Q1_point | 1.18 | 1.18 | 150.2 |
| raw_parquet | duckdb | Q2_range_agg | 1.17 | 1.17 | 149.9 |
| raw_parquet | duckdb | Q3_topn | 1.15 | 1.15 | 150.5 |
| raw_parquet | polars | Q1_point | 1.13 | 1.13 | 150.2 |
| raw_parquet | polars | Q2_range_agg | 1.10 | 1.10 | 149.8 |
| raw_parquet | polars | Q3_topn | 1.18 | 1.18 | 150.4 |
| iceberg_compacted | duckdb | Q1_point | 1.11 | 1.11 | 150.4 |
| iceberg_compacted | duckdb | Q2_range_agg | 1.12 | 1.12 | 149.9 |
| iceberg_compacted | duckdb | Q3_topn | 1.14 | 1.14 | 150.4 |
| iceberg_compacted | polars | Q1_point | 1.11 | 1.11 | 150.1 |
| iceberg_compacted | polars | Q2_range_agg | 1.11 | 1.11 | 149.1 |
| iceberg_compacted | polars | Q3_topn | 1.15 | 1.15 | 150.1 |
