# Lakehouse Engine Architecture

## Overview

`lakehouse-lite-engine` is an embedded zero-JVM lakehouse engine operating in Python 3.13.

## Components

1. **Ingestion & Validation**: Pydantic v2 fast-path validation + isolation routing invalid batches to DLQ.
2. **Buffer & Compaction**: In-memory PyArrow batch buffering, pre-flush threshold enforcement, ZSTD Parquet file creation, and background compaction.
3. **Catalog Management**: PyIceberg catalog manager with SQLite/REST support and process advisory locking.
4. **Query Governance**: Lease-governed DuckDB and Polars query execution using zero-copy Arrow streams (`__arrow_c_stream__`).
