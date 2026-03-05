"""Paginated data reading from a PostgreSQL source."""

from __future__ import annotations

from typing import Generator

import pandas as pd

from database.connector import DBConnector


def read_rows(
    conn: DBConnector,
    table: str,
    schema: str = "public",
    limit: int = 20,
    offset: int = 0,
) -> pd.DataFrame:
    """Return up to *limit* rows from *table* starting at *offset*."""
    rows = conn.execute_query(
        f'SELECT * FROM "{schema}"."{table}" LIMIT %s OFFSET %s',
        (limit, offset),
    )
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def read_distinct_values(
    conn: DBConnector,
    table: str,
    column: str,
    schema: str = "public",
    max_values: int = 500,
) -> list[tuple[str, int]]:
    """Return (value, count) pairs for distinct values in *column*.

    Limited to *max_values* most frequent values.
    """
    rows = conn.execute_query(
        f"""
        SELECT "{column}"::text AS val, COUNT(*) AS cnt
        FROM "{schema}"."{table}"
        GROUP BY "{column}"
        ORDER BY cnt DESC
        LIMIT %s
        """,
        (max_values,),
    )
    return [(r["val"], r["cnt"]) for r in rows]


def read_all_rows_batched(
    conn: DBConnector,
    table: str,
    schema: str = "public",
    batch_size: int = 100,
    order_by: str | None = None,
) -> Generator[list[dict], None, None]:
    """Yield rows from *table* in batches of *batch_size*.

    Uses server-side cursor (named cursor) to avoid loading all data into memory.
    """
    order_clause = f'ORDER BY "{order_by}"' if order_by else ""
    sql = f'SELECT * FROM "{schema}"."{table}" {order_clause}'

    with conn.connection.cursor(
        name="batch_reader", cursor_factory=__import__("psycopg2.extras", fromlist=["RealDictCursor"]).RealDictCursor
    ) as cur:
        cur.itersize = batch_size
        cur.execute(sql)
        batch: list[dict] = []
        for row in cur:
            batch.append(dict(row))
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch
