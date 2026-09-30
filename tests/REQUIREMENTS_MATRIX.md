# tests/REQUIREMENTS_MATRIX.md

| Requirement ID | Module | Description | Test Mapping |
|---|---|---|---|
| `ING-1` | Ingestion | Fast path uses `validate_json` on bytes | `tests/unit/test_ingestion.py::test_validator_fast_path` |
| `ING-2` | Ingestion | Module-level `Final` `TypeAdapter` instances | `tests/test_no_legacy.py` |
| `ING-3` | Ingestion | Row cap enforcement | `tests/unit/test_ingestion.py` |
| `ING-4` | Ingestion | Rejected rows produce DLQ records | `tests/unit/test_ingestion.py::test_validator_isolation_path` |
| `ING-5` | Ingestion | Schema drift maps to extra_forbidden | `tests/unit/test_ingestion.py` |
| `ING-6` | Ingestion | DLQ record truncation and secret safety | `tests/unit/test_ingestion.py` |
| `ING-7` | Ingestion | Valid Arrow batch schema equality | `tests/contract/test_contracts.py` |
| `BUF-1` | Buffer | Buffer flush triggers | `tests/unit/test_buffer.py` |
| `BUF-2` | Buffer | Buffer holds Arrow RecordBatches only | `tests/unit/test_buffer.py` |
| `BUF-3` | Buffer | Pre-flush size/row limits | `tests/unit/test_buffer.py` |
| `BUF-4` | Buffer | Single row group per flush file | `tests/contract/test_contracts.py` |
| `CAT-1` | Catalog | Zero JVM bridge dependencies | `tests/test_no_legacy.py` |
| `CAT-2` | Catalog | Atomic single snapshot commits | `tests/contract/test_contracts.py` |
| `CAT-3` | Catalog | Idempotency on `lhe.flush-id` | `tests/contract/test_contracts.py::test_ct01_ct02_fast_append_and_idempotency` |
| `QRY-1` | Query | Snapshot pinning | `tests/contract/test_contracts.py` |
| `QRY-2` | Query | DuckDB memory limit & thread bounds | `tests/contract/test_contracts.py` |
| `QRY-3` | Query | Zero fetchall/to_pandas usage in `src/` | `tests/test_no_legacy.py` |
| `CFG-1` | Config | Frozen Pydantic settings | `tests/unit/test_config.py::test_config_defaults` |
| `CFG-2` | Config | SecretStr redaction | `tests/unit/test_config.py::test_secret_redaction` |
