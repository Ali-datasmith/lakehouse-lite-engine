# Lakehouse Engine Benchmark Report

**Seed:** 20260930
**OS:** linux | **CPUs:** 4

## Results

| Layout | Engine | Query | p50 (ms) | p95 (ms) | Peak RSS (MB) |
|---|---|---|---|---|---|
| csv | duckdb | Q1_point | 1325.07 | 1325.07 | 210.7 |
| csv | duckdb | Q2_range_agg | 1295.64 | 1295.64 | 208.3 |
| csv | duckdb | Q3_topn | 1300.03 | 1300.03 | 219.8 |
| csv | polars | Q1_point | 19.77 | 19.77 | 208.9 |
| csv | polars | Q2_range_agg | 15.27 | 15.27 | 201.6 |
| csv | polars | Q3_topn | 17.84 | 17.84 | 204.7 |
| raw_parquet | duckdb | Q1_point | 20.89 | 20.89 | 181.0 |
| raw_parquet | duckdb | Q2_range_agg | 22.21 | 22.21 | 181.9 |
| raw_parquet | duckdb | Q3_topn | 27.53 | 27.53 | 187.8 |
| raw_parquet | polars | Q1_point | 4.96 | 4.96 | 196.5 |
| raw_parquet | polars | Q2_range_agg | 7.06 | 7.06 | 198.5 |
| raw_parquet | polars | Q3_topn | 9.19 | 9.19 | 204.9 |
| iceberg_compacted | duckdb | Q1_point | 62.53 | 62.53 | 236.2 |
| iceberg_compacted | duckdb | Q2_range_agg | 64.32 | 64.32 | 237.7 |
| iceberg_compacted | duckdb | Q3_topn | 70.09 | 70.09 | 242.3 |
| iceberg_compacted | polars | Q1_point | 30.46 | 30.46 | 214.8 |
| iceberg_compacted | polars | Q2_range_agg | 25.98 | 25.98 | 208.4 |
| iceberg_compacted | polars | Q3_topn | 33.27 | 33.27 | 217.3 |
