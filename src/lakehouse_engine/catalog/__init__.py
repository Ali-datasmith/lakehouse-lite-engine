# src/lakehouse_engine/catalog/__init__.py
from lakehouse_engine.catalog.manager import CatalogManager, CommitResult
from lakehouse_engine.catalog.schema_guard import assert_compatible
from lakehouse_engine.catalog.storage import resolve_filesystem

__all__ = [
    "CatalogManager",
    "CommitResult",
    "assert_compatible",
    "resolve_filesystem",
]
