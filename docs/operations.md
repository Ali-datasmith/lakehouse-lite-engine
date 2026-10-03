# Operational Guide

## Monitoring & Health Check

The `LakehouseEngine` provides structured logging and metric introspection hooks via `engine.status()`.

## Storage Directory Permissions

All local directories created by `lakehouse-lite-engine` (catalog database lock, DLQ output, DuckDB spill directory) are automatically created with secure `0o700` permissions.
