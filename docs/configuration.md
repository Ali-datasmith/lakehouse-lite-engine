# Engine Configuration Reference

`lakehouse-lite-engine` uses Pydantic Settings for type-safe configuration management.

## Environment Variables

- `LHE_ENV`: Runtime environment (`dev` or `prod`).
- `LHE_CATALOG__URI`: Catalog URI (e.g. `sqlite:///.lakehouse-lite/catalog.db`).
- `LHE_CATALOG__WAREHOUSE_URI`: Warehouse root (e.g. `file:///.lakehouse-lite/warehouse`).
- `LHE_INGESTION__DLQ_DIR`: DLQ directory path.
- `LHE_INGESTION__DLQ_RETENTION_DAYS`: DLQ file retention in days (default: 30).
- `LHE_INGESTION__DLQ_MAX_TOTAL_BYTES`: Maximum DLQ storage bytes (default: 1 GB).
