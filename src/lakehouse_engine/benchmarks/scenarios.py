# src/lakehouse_engine/benchmarks/scenarios.py
import polars as pl

from lakehouse_engine.query.duckdb_adapter import DuckDBSession


def run_q1_point_duckdb(session: DuckDBSession, target_id: int = 42) -> int:
    rel = session.sql("SELECT * FROM events WHERE event_id = $1", params={"target_id": target_id})
    reader = rel.to_arrow_reader()
    total = sum(b.num_rows for b in reader)
    return total


def run_q1_point_polars(lf: pl.LazyFrame, target_id: int = 42) -> int:
    filtered = lf.filter(pl.col("event_id") == target_id)
    df = filtered.collect(engine="streaming")
    return len(df)


def run_q2_range_agg_duckdb(session: DuckDBSession) -> int:
    query = """
        SELECT event_name, COUNT(*) as cnt
        FROM events
        GROUP BY event_name
    """
    rel = session.sql(query)
    reader = rel.to_arrow_reader()
    total = sum(b.num_rows for b in reader)
    return total


def run_q2_range_agg_polars(lf: pl.LazyFrame) -> int:
    agg = lf.group_by("event_name").agg(pl.len().alias("cnt"))
    df = agg.collect(engine="streaming")
    return len(df)


def run_q3_topn_duckdb(session: DuckDBSession) -> int:
    query = """
        SELECT user_id, COUNT(*) as cnt
        FROM events
        GROUP BY user_id
        ORDER BY cnt DESC
        LIMIT 100
    """
    rel = session.sql(query)
    reader = rel.to_arrow_reader()
    total = sum(b.num_rows for b in reader)
    return total


def run_q3_topn_polars(lf: pl.LazyFrame) -> int:
    topn = lf.group_by("user_id").agg(pl.len().alias("cnt")).sort("cnt", descending=True).limit(100)
    df = topn.collect(engine="streaming")
    return len(df)
