# Lakehouse Engine Benchmark Report

**Seed:** 20260930
**OS:** linux | **CPUs:** 4

## Results

| Layout | Engine | Query | p50 (ms) | p95 (ms) | Peak RSS (MB) |
|---|---|---|---|---|---|
| csv | duckdb | Q1_point | 1298.35 | 1298.35 | 209.1 |
| csv | duckdb | Q2_range_agg | 1328.93 | 1328.93 | 209.1 |
| csv | duckdb | Q3_topn | 1323.14 | 1323.14 | 210.6 |
| csv | polars | Q1_point | 16.45 | 16.45 | 206.7 |
| csv | polars | Q2_range_agg | 14.85 | 14.85 | 202.4 |
| csv | polars | Q3_topn | 16.24 | 16.24 | 206.2 |
| raw_parquet | duckdb | Q1_point | 20.68 | 20.68 | 181.8 |
| raw_parquet | duckdb | Q2_range_agg | 20.20 | 20.20 | 182.6 |
| raw_parquet | duckdb | Q3_topn | 27.37 | 27.37 | 186.9 |
| raw_parquet | polars | Q1_point | 4.85 | 4.85 | 198.1 |
| raw_parquet | polars | Q2_range_agg | 6.27 | 6.27 | 195.9 |
| raw_parquet | polars | Q3_topn | 9.01 | 9.01 | 205.9 |
| iceberg_compacted | duckdb | Q1_point | 60.31 | 60.31 | 231.2 |
| iceberg_compacted | duckdb | Q2_range_agg | 65.26 | 65.26 | 237.8 |
| iceberg_compacted | duckdb | Q3_topn | 72.08 | 72.08 | 241.5 |
| iceberg_compacted | polars | Q1_point | 30.67 | 30.67 | 217.9 |
| iceberg_compacted | polars | Q2_range_agg | 27.06 | 27.06 | 210.0 |
| iceberg_compacted | polars | Q3_topn | 33.12 | 33.12 | 215.4 |
