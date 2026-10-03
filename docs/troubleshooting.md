# Troubleshooting Guide

## Common Issues & Remediation

### 1. `ConfigurationError`: Heavy library imported before runtime configuration
**Cause**: `pyarrow`, `polars`, or `duckdb` was imported before `EngineSettings` or `configure_runtime()` was initialized.
**Solution**: Ensure `configure_runtime(settings.runtime)` is executed at process entry before importing heavy libraries or use `LakehouseEngine`.

### 2. `UnsafeSqlError`: Unsafe query statement
**Cause**: DuckDB query contained semicolons, comments, or disallowed non-SELECT statements.
**Solution**: Sanitize queries to standard SELECT/WITH statements without multi-statement semicolons.
