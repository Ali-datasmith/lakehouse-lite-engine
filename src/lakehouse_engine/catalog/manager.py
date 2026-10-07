import contextlib
import logging
import random
import time
from collections.abc import Sequence
from dataclasses import dataclass
from fcntl import LOCK_EX, LOCK_NB, flock
from pathlib import Path
from typing import TYPE_CHECKING, TextIO

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.io.pyarrow import parquet_file_to_data_file
from pyiceberg.table import Table

from lakehouse_engine.catalog.schema_guard import assert_compatible
from lakehouse_engine.exceptions import (
    CatalogCommitError,
    CatalogConnectionError,
)
from lakehouse_engine.ingestion.schema import EVENTS_ARROW_SCHEMA

if TYPE_CHECKING:
    from lakehouse_engine.buffer.buffer import FlushedFile
    from lakehouse_engine.config import CatalogSettings, StorageSettings

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class CommitResult:
    snapshot_id: int
    flush_id: str
    attempts: int
    added_files: int
    added_rows: int
    replayed: bool


class CatalogManager:
    def __init__(self, settings: "CatalogSettings", storage: "StorageSettings") -> None:
        self._settings = settings
        self._storage = storage
        self._catalog: Catalog | None = None
        self._table: Table | None = None
        self._lock_file: TextIO | None = None

    @property
    def storage_settings(self) -> "StorageSettings":
        return self._storage

    def open(self) -> None:
        if self._settings.uri.startswith("sqlite:"):
            db_path_str = self._settings.uri.replace("sqlite:///", "").replace("sqlite://", "")
            lock_path = Path(db_path_str + ".lock").resolve()
            lock_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            try:
                lf = lock_path.open("w")
                flock(lf, LOCK_EX | LOCK_NB)
                self._lock_file = lf
            except OSError as exc:
                raise CatalogConnectionError(f"Failed to acquire writer lock: {exc}") from exc

        props: dict[str, str] = {
            "type": self._settings.kind,
            "uri": self._settings.uri,
            "warehouse": self._settings.warehouse_uri,
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        }
        if self._storage.s3_endpoint:
            props["s3.endpoint"] = self._storage.s3_endpoint
        if self._storage.s3_region:
            props["s3.region"] = self._storage.s3_region
        if self._storage.s3_access_key_id:
            props["s3.access-key-id"] = self._storage.s3_access_key_id.get_secret_value()
        if self._storage.s3_secret_access_key:
            props["s3.secret-access-key"] = self._storage.s3_secret_access_key.get_secret_value()
        if self._settings.rest_token:
            props["token"] = self._settings.rest_token.get_secret_value()

        try:
            self._catalog = load_catalog(self._settings.name, **props)
            self._catalog.create_namespace_if_not_exists(self._settings.namespace)
            identifier = f"{self._settings.namespace}.{self._settings.table_name}"
            self._table = self._catalog.create_table_if_not_exists(
                identifier,
                schema=EVENTS_ARROW_SCHEMA,
                properties={
                    "format-version": "2",
                    "write.format.default": "parquet",
                    "write.parquet.compression-codec": "zstd",
                    "write.parquet.compression-level": "3",
                    "write.parquet.row-group-limit": "500000",
                    "write.target-file-size-bytes": str(128 * 1024 * 1024),
                    "lhe.schema-version": "1",
                },
            )
        except Exception as exc:
            raise CatalogConnectionError(f"Failed to initialize catalog: {exc}") from exc
        assert_compatible(self._table.schema(), EVENTS_ARROW_SCHEMA)

    @property
    def table(self) -> Table:
        return self.current_table()

    def current_table(self) -> Table:
        if self._table is None:
            raise CatalogConnectionError("CatalogManager is not open.")
        self._table.refresh()
        return self._table

    def snapshot_id(self) -> int | None:
        current = self.current_table().current_snapshot()
        return current.snapshot_id if current is not None else None

    def data_dir(self) -> str:
        return f"{self.current_table().location()}/data"

    def _check_idempotency(self, flush_id: str) -> CommitResult | None:
        for snap in self.current_table().metadata.snapshots:
            if snap.summary and snap.summary.additional_properties.get("lhe.flush-id") == flush_id:
                return CommitResult(snap.snapshot_id, flush_id, 1, 0, 0, True)
        return None

    def commit_files(self, files: Sequence["FlushedFile"], *, flush_id: str) -> CommitResult:
        if not files:
            raise CatalogCommitError("No files provided.")
        replayed_res = self._check_idempotency(flush_id)
        if replayed_res:
            return replayed_res
        identifier = f"{self._settings.namespace}.{self._settings.table_name}"
        file_paths = [f.path for f in files]
        added_rows = sum(f.rows for f in files)
        attempts = 0
        backoff_base = self._settings.commit_backoff_base_seconds
        backoff_max = self._settings.commit_backoff_max_seconds
        for attempt in range(1, self._settings.commit_max_attempts + 1):
            attempts = attempt
            replayed = self._check_idempotency(flush_id)
            if replayed:
                return replayed
            try:
                if self._catalog is None:
                    raise CatalogConnectionError("Catalog not open.")  # noqa: TRY301
                fresh_table = self._catalog.load_table(identifier)
                fresh_table.add_files(
                    file_paths=file_paths,
                    snapshot_properties={"lhe.flush-id": flush_id},
                )
                self._table = fresh_table
                snap = fresh_table.current_snapshot()
                snap_id = snap.snapshot_id if snap is not None else -1
                return CommitResult(
                    snap_id,
                    flush_id,
                    attempts,
                    len(files),
                    added_rows,
                    False,
                )
            except CommitFailedException:
                if attempt == self._settings.commit_max_attempts:
                    break
                jitter = random.uniform(0.5, 1.5)  # noqa: S311
                time.sleep(
                    min(
                        backoff_max,
                        backoff_base * (2 ** (attempt - 1)),
                    )
                    * jitter
                )
            except CatalogConnectionError:
                raise
            except Exception as exc:
                raise CatalogCommitError(
                    f"Commit failed: {exc}",
                    context={"pending_files": file_paths},
                ) from exc
        raise CatalogCommitError(
            "Commit exhausted attempts.",
            context={"pending_files": file_paths},
        )

    def commit_replace(
        self,
        *,
        delete: Sequence[str],
        add_paths: Sequence[str],
        flush_id: str,
    ) -> CommitResult:
        replayed_res = self._check_idempotency(flush_id)
        if replayed_res:
            return replayed_res
        tbl = self.current_table()
        delete_set = set(delete)
        matching_data_files = [
            task.file for task in tbl.scan().plan_files() if task.file.file_path in delete_set
        ]
        if len(matching_data_files) != len(delete_set):
            missing = delete_set - {df.file_path for df in matching_data_files}
            raise CatalogCommitError(f"delete paths not found: {missing}")
        with tbl.transaction() as tx:
            overwrite = tx.update_snapshot(
                snapshot_properties={"lhe.flush-id": flush_id}
            ).overwrite()
            for df in matching_data_files:
                overwrite.delete_data_file(df)
            added_rows = 0
            for path in add_paths:
                data_file = parquet_file_to_data_file(tbl.io, tbl.metadata, path)
                added_rows += data_file.record_count
                overwrite.append_data_file(data_file)
        identifier = f"{self._settings.namespace}.{self._settings.table_name}"
        fresh_table = self._catalog.load_table(identifier) if self._catalog else tbl
        self._table = fresh_table
        snap = fresh_table.current_snapshot()
        snap_id = snap.snapshot_id if snap is not None else -1
        return CommitResult(snap_id, flush_id, 1, len(add_paths), added_rows, False)

    def close(self) -> None:
        if self._lock_file is not None:
            with contextlib.suppress(OSError):
                self._lock_file.close()
            self._lock_file = None
