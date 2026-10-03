# Changelog

All notable changes to `lakehouse-lite-engine` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Public `LakehouseEngine.append_arrow_batch()` API for appending Arrow record batches directly through governor and compaction buffer paths.
- Guaranteed runtime memory pool configuration verification in `configure_runtime()`.
- DuckDB temporary/spill directory auto-creation with secure `0o700` permissions.

### Fixed
- Fixed `CompactionBuffer` retry semantics to retain original `flush_id` for pending files and commit pending files prior to new batch flushes.
- Replaced synthetic benchmark claims and estimated S3 request figures in README with real, harness-generated benchmark results and metadata.
- Updated README quickstart examples to use secure local `.lakehouse-lite` defaults instead of `/tmp`.
- Standardized DuckDB `sql()` query execution on streaming relations without materializing full tables via `fetch_arrow_table()`.
- Governed Polars query execution through `QueryService.collect_polars()` under `QUERY` leases.
- Expanded AST scanner in `tests/test_no_legacy.py` to ban `os.system`, `subprocess.run(shell=True)`, `pickle.loads`, `eval`, and `exec`.

## [1.0.0-remediation] - 2026-10-02

### Added
- GitHub Actions CI workflow (`ci.yml`) for Python 3.13 linting, typing, security scanning, pytest, and smoke benchmarking.
- CodeQL SAST workflow (`codeql-sast.yml`).
- Architectural and operational documentation in `docs/` directory (`architecture.md`, `configuration.md`, `operations.md`, `troubleshooting.md`, `benchmarks/real-benchmark-report.md`).
- `CONTRIBUTING.md` guide.
- DLQ retention enforcement and max total disk usage pruning.
- Windowed reject ratio circuit breaker in ingestion validator.
- Reentrant reference-counted query/flush governor leases.
- Full schema comparison in `SchemaGuard` checking field names, order, Arrow types, and nullability.
- Structured logging and metrics hooks across core engine modules.
- Explicit pending commit recovery API in `CompactionBuffer`.

### Fixed
- Lightweight `lakehouse_engine` package initialization with lazy imports to prevent eager `pyarrow`/`polars`/`duckdb` loading.
- Enforced pre-import runtime bootstrap checks in `configure_runtime()`.
- Removed insecure `/tmp` default file paths; defaulted to isolated local directory structures with `0o700` file permissions.
- Hardened DuckDB query execution with strict SQL statement validation, session parameter binding, and query timeouts.
- Preserved original `flush_id` across pending commit retries for crash-safe idempotent replay semantics.
- Fixed compaction file selection, target size rotation, missing delete path detection, and PyIceberg replace commit safety.
