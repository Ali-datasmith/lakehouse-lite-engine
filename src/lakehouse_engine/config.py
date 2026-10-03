# src/lakehouse_engine/config.py
from pathlib import Path
from typing import Annotated, Final, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, SecretStr, ValidationError, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from lakehouse_engine.exceptions import ConfigurationError

MiB: Final[int] = 1024 * 1024
SCHEMA_VERSION: Final[str] = "1"


class _Section(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=False)


class RuntimeSettings(_Section):
    env: Literal["dev", "prod"] = "dev"
    memory_hard_limit_bytes: Annotated[int, Field(ge=64 * MiB, le=4096 * MiB)] = 480 * MiB
    memory_soft_limit_bytes: Annotated[int, Field(ge=32 * MiB)] = 440 * MiB
    process_ceiling_bytes: Annotated[int, Field(ge=64 * MiB)] = 500 * MiB  # CI/benchmark gate
    arrow_memory_pool: Literal["system", "mimalloc", "jemalloc"] = "mimalloc"
    cpu_threads: Annotated[int, Field(ge=1, le=64)] = 2
    io_threads: Annotated[int, Field(ge=1, le=64)] = 4
    allow_concurrent_query_during_flush: bool = False
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"


class IngestionSettings(_Section):
    max_batch_rows: Annotated[int, Field(ge=1, le=100_000)] = 20_000
    max_batch_bytes: Annotated[int, Field(ge=1024, le=64 * MiB)] = 8 * MiB
    max_line_bytes: Annotated[int, Field(ge=256, le=8 * MiB)] = 1 * MiB
    dlq_dir: Path = Path(".lakehouse-lite/dlq")
    dlq_rotate_bytes: Annotated[int, Field(ge=1 * MiB)] = 16 * MiB
    dlq_raw_truncate_bytes: Annotated[int, Field(ge=256, le=1 * MiB)] = 64 * 1024
    dlq_retention_days: Annotated[int, Field(ge=1, le=365)] = 30
    dlq_max_total_bytes: Annotated[int, Field(ge=10 * MiB)] = 1024 * MiB
    max_reject_ratio: Annotated[float, Field(ge=0.0, le=1.0)] = 0.5
    reject_ratio_min_sample: Annotated[int, Field(ge=1)] = 1_000


class BufferSettings(_Section):
    max_bytes: Annotated[int, Field(ge=1 * MiB, le=128 * MiB)] = 128 * MiB  # hard-capped at 128 MB
    max_rows: Annotated[int, Field(ge=100_000, le=500_000)] = 500_000
    max_age_seconds: Annotated[float, Field(gt=0.0)] = 60.0
    compression: Literal["zstd"] = "zstd"
    compression_level: Annotated[int, Field(ge=1, le=19)] = 3
    data_page_bytes: Annotated[int, Field(ge=64 * 1024, le=8 * MiB)] = 1 * MiB
    row_group_max_rows: Annotated[int, Field(ge=100_000, le=500_000)] = 500_000


class CompactionSettings(_Section):
    min_input_files: Annotated[int, Field(ge=2)] = 8
    max_input_files_per_run: Annotated[int, Field(ge=2, le=256)] = 64
    small_file_threshold_bytes: Annotated[int, Field(ge=1 * MiB)] = 64 * MiB
    target_file_bytes: Annotated[int, Field(ge=8 * MiB, le=512 * MiB)] = 128 * MiB
    read_batch_rows: Annotated[int, Field(ge=10_000, le=500_000)] = 100_000
    use_threads: bool = False


class StorageSettings(_Section):
    s3_endpoint: str | None = None
    s3_region: str | None = None
    s3_access_key_id: SecretStr | None = None
    s3_secret_access_key: SecretStr | None = None


class CatalogSettings(_Section):
    name: str = "local"
    kind: Literal["sql", "rest"] = "sql"
    uri: str = "sqlite:///.lakehouse-lite/catalog.db"
    warehouse_uri: str = "file:///.lakehouse-lite/warehouse"
    rest_token: SecretStr | None = None
    namespace: Annotated[str, Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")] = "default"
    table_name: Annotated[str, Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")] = "events"
    commit_max_attempts: Annotated[int, Field(ge=1, le=20)] = 5
    commit_backoff_base_seconds: Annotated[float, Field(gt=0.0)] = 0.2
    commit_backoff_max_seconds: Annotated[float, Field(gt=0.0)] = 5.0


class QuerySettings(_Section):
    duckdb_memory_limit_bytes: Annotated[int, Field(ge=32 * MiB)] = 192 * MiB
    duckdb_threads: Annotated[int, Field(ge=1, le=16)] = 2
    duckdb_temp_dir: Path = Path(".lakehouse-lite/duckdb-spill")
    duckdb_timeout_seconds: Annotated[float, Field(gt=0.0)] = 30.0
    polars_threads: Annotated[int, Field(ge=1, le=16)] = 2
    polars_strategy: Literal["iceberg", "parquet_files"] = "iceberg"
    arrow_batch_rows: Annotated[int, Field(ge=10_000, le=500_000)] = 100_000


class BenchmarkSettings(_Section):
    rows: Annotated[int, Field(ge=100_000)] = 5_000_000
    small_file_rows: Annotated[int, Field(ge=1_000)] = 10_000
    warmups: Annotated[int, Field(ge=0)] = 1
    runs: Annotated[int, Field(ge=1)] = 5
    seed: int = 20260930
    output_dir: Path = Path("./bench-out")


class EngineSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="LHE_",
        env_nested_delimiter="__",
        env_file=".env",
        extra="forbid",
        frozen=True,
    )
    runtime: RuntimeSettings = RuntimeSettings()
    ingestion: IngestionSettings = IngestionSettings()
    buffer: BufferSettings = BufferSettings()
    compaction: CompactionSettings = CompactionSettings()
    storage: StorageSettings = StorageSettings()
    catalog: CatalogSettings = CatalogSettings()
    query: QuerySettings = QuerySettings()
    benchmarks: BenchmarkSettings = BenchmarkSettings()

    @model_validator(mode="after")
    def _check_invariants(self) -> Self:
        r, b, c = self.runtime, self.buffer, self.compaction
        if not (r.memory_soft_limit_bytes < r.memory_hard_limit_bytes <= r.process_ceiling_bytes):
            raise ConfigurationError(
                "require soft < hard <= ceiling",
                context={
                    "soft": r.memory_soft_limit_bytes,
                    "hard": r.memory_hard_limit_bytes,
                    "ceiling": r.process_ceiling_bytes,
                },
            )
        baseline, in_hand, slack = 120 * MiB, 16 * MiB, 40 * MiB
        flush_peak = baseline + in_hand + b.max_bytes + int(b.max_bytes * 1.25) + slack
        if flush_peak > r.process_ceiling_bytes:
            raise ConfigurationError(
                "buffer.max_bytes cannot fit the flush memory budget",
                context={
                    "estimated_flush_peak": flush_peak,
                    "ceiling": r.process_ceiling_bytes,
                },
            )
        if c.target_file_bytes < b.max_bytes // 8:
            raise ConfigurationError(
                "compaction.target_file_bytes is implausibly small for buffer.max_bytes"
            )
        if self.catalog.commit_backoff_base_seconds > self.catalog.commit_backoff_max_seconds:
            raise ConfigurationError(
                "commit_backoff_base_seconds must be <= commit_backoff_max_seconds"
            )

        # Insecure /tmp validation in prod
        if r.env == "prod":
            for path_name, path_val in (
                ("dlq_dir", self.ingestion.dlq_dir),
                ("duckdb_temp_dir", self.query.duckdb_temp_dir),
            ):
                resolved = path_val.resolve()
                if resolved == Path("/tmp") or resolved.parent == Path("/tmp"):  # noqa: S108
                    raise ConfigurationError(
                        f"Insecure default path '{path_name}' in production environment: {path_val}"
                    )
        return self

    @classmethod
    def load(cls, **kwargs: object) -> "EngineSettings":
        """Loads and validates settings, mapping Pydantic ValidationError to ConfigurationError."""
        try:
            return cls(**kwargs)  # type: ignore[arg-type]
        except ValidationError as exc:
            raise ConfigurationError(
                f"Invalid engine configuration: {exc}",
                context={"errors": exc.errors()},
            ) from exc
