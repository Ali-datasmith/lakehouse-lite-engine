# Lakehouse Engine Benchmark Report

**Seed:** 20260930
**OS:** linux | **CPUs:** 4

## Results

| Layout | Engine | Query | p50 (ms) | p95 (ms) | Peak RSS (MB) |
|---|---|---|---|---|---|
| csv | duckdb | Q1_point | 1407.52 | 1407.52 | 210.7 |
| csv | duckdb | Q2_range_agg | 1294.84 | 1294.84 | 211.0 |
| csv | duckdb | Q3_topn | 1303.68 | 1303.68 | 214.4 |
| csv | polars | Q1_point | 16.24 | 16.24 | 210.8 |
| csv | polars | Q2_range_agg | 13.89 | 13.89 | 203.6 |
| csv | polars | Q3_topn | 24.50 | 24.50 | 208.9 |
| raw_parquet | duckdb | Q1_point | 21.25 | 21.25 | 181.6 |
| raw_parquet | duckdb | Q2_range_agg | 20.99 | 20.99 | 183.8 |
| raw_parquet | duckdb | Q3_topn | 27.50 | 27.50 | 193.0 |
| raw_parquet | polars | Q1_point | 5.99 | 5.99 | 200.1 |
| raw_parquet | polars | Q2_range_agg | 6.92 | 6.92 | 200.2 |
| raw_parquet | polars | Q3_topn | 10.87 | 10.87 | 209.3 |
| iceberg_compacted | duckdb | Q1_point | 117.73 | 117.73 | 243.2 |
| iceberg_compacted | duckdb | Q2_range_agg | 262.79 | 262.79 | 255.0 |
| iceberg_compacted | duckdb | Q3_topn | 76.34 | 76.34 | 248.8 |
| iceberg_compacted | polars | Q1_point | 30.48 | 30.48 | 216.1 |
| iceberg_compacted | polars | Q2_range_agg | 27.59 | 27.59 | 212.5 |
| iceberg_compacted | polars | Q3_topn | 33.24 | 33.24 | 220.2 |
