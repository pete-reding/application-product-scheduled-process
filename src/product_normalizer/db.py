"""
db.py
-----
MotherDuck connection factory and low-level query helpers.

All modules should obtain a connection via ``get_conn()`` rather than
calling duckdb directly, so the auth token is injected from one place.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, Generator

import duckdb

from .config import settings

logger = logging.getLogger(__name__)

_CONN: duckdb.DuckDBPyConnection | None = None


def get_conn(readonly: bool = False) -> duckdb.DuckDBPyConnection:
    """
    Return a persistent MotherDuck connection.

    Parameters
    ----------
    readonly:
        If True, opens a read-only connection (useful for source queries).
        The shared singleton is always read-write.
    """
    global _CONN  # noqa: PLW0603
    if _CONN is None:
        md_path = f"md:?motherduck_token={settings.motherduck_token}"
        logger.debug("Opening MotherDuck connection…")
        _CONN = duckdb.connect(md_path)
        logger.info("MotherDuck connection established.")
    return _CONN


def close_conn() -> None:
    """Close and reset the singleton connection."""
    global _CONN  # noqa: PLW0603
    if _CONN is not None:
        _CONN.close()
        _CONN = None
        logger.debug("MotherDuck connection closed.")


@contextmanager
def transaction() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """
    Context manager that wraps work in a DuckDB transaction.
    Rolls back on any exception.
    """
    conn = get_conn()
    conn.execute("BEGIN")
    try:
        yield conn
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def execute(sql: str, params: list[Any] | None = None) -> duckdb.DuckDBPyRelation:
    """Execute a write statement (INSERT / UPDATE / CREATE …)."""
    conn = get_conn()
    logger.debug("SQL ▶ %s", sql[:120])
    return conn.execute(sql, params or [])


def query(sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    """Run a SELECT and return rows as a list of dicts."""
    conn = get_conn()
    logger.debug("QUERY ▶ %s", sql[:120])
    rel = conn.execute(sql, params or [])
    columns = [d[0] for d in rel.description]
    return [dict(zip(columns, row)) for row in rel.fetchall()]


def query_one(sql: str, params: list[Any] | None = None) -> dict[str, Any] | None:
    """Return the first row or None."""
    rows = query(sql, params)
    return rows[0] if rows else None


def scalar(sql: str, params: list[Any] | None = None) -> Any:
    """Return the first column of the first row (scalar result)."""
    row = query_one(sql, params)
    if row is None:
        return None
    return next(iter(row.values()))
