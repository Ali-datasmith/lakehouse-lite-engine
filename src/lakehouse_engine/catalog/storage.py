# src/lakehouse_engine/catalog/storage.py
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import pyarrow.fs as pafs

from lakehouse_engine.exceptions import ConfigurationError

if TYPE_CHECKING:
    from lakehouse_engine.config import StorageSettings


def resolve_filesystem(
    uri: str, settings: "StorageSettings | None" = None
) -> tuple[pafs.FileSystem, str]:
    """Resolves a URI (file://, s3://, or local path) to a PyArrow FileSystem and relative path.

    Returns:
        (filesystem, relative_path)
    """
    parsed = urlparse(uri)
    scheme = parsed.scheme

    if scheme in ("", "file"):
        path_str = parsed.path if scheme == "file" else uri
        p = Path(path_str).resolve()
        return pafs.LocalFileSystem(), str(p)

    if scheme == "s3":
        endpoint_override = settings.s3_endpoint if settings else None
        region = settings.s3_region if settings else None
        access_key = (
            settings.s3_access_key_id.get_secret_value()
            if settings and settings.s3_access_key_id
            else None
        )
        secret_key = (
            settings.s3_secret_access_key.get_secret_value()
            if settings and settings.s3_secret_access_key
            else None
        )

        s3_fs = pafs.S3FileSystem(
            endpoint_override=endpoint_override,
            region=region,
            access_key=access_key,
            secret_key=secret_key,
        )
        rel_path = f"{parsed.netloc}{parsed.path}"
        return s3_fs, rel_path

    raise ConfigurationError(f"Unsupported storage scheme in URI: {uri}")
