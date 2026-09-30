# src/lakehouse_engine/query/__init__.py
from lakehouse_engine.query.duckdb_adapter import DuckDBSession
from lakehouse_engine.query.polars_adapter import PolarsAdapter
from lakehouse_engine.query.protocols import ArrowArrayExportable, ArrowStreamExportable
from lakehouse_engine.query.service import QueryService

__all__ = [
    "ArrowArrayExportable",
    "ArrowStreamExportable",
    "DuckDBSession",
    "PolarsAdapter",
    "QueryService",
]
