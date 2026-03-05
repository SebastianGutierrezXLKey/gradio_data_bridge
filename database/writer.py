"""Insert rows into a PostgreSQL target database."""

from __future__ import annotations

from typing import Any

import psycopg2

from database.connector import DBConnector


def build_insert_sql(
    table: str,
    schema: str,
    columns: list[str],
    returning: str | None = None,
    on_conflict: str | None = None,
) -> str:
    """Build a parameterised INSERT statement.

    Args:
        table: Target table name.
        schema: Target schema name.
        columns: Column names to insert.
        returning: Column name to return (e.g. 'id'), or None.
        on_conflict: Optional ON CONFLICT clause (e.g. 'DO NOTHING').

    Returns:
        SQL string with %s placeholders.
    """
    quoted_cols = ", ".join(f'"{c}"' for c in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    sql = f'INSERT INTO "{schema}"."{table}" ({quoted_cols}) VALUES ({placeholders})'
    if on_conflict:
        sql += f" ON CONFLICT {on_conflict}"
    if returning:
        sql += f' RETURNING "{returning}"'
    return sql


def insert_row(
    conn: DBConnector,
    table: str,
    schema: str,
    data: dict[str, Any],
    returning_col: str | None = None,
    on_conflict: str | None = None,
) -> Any:
    """Insert a single row and return the value of *returning_col* if specified.

    Raises:
        psycopg2.Error on database errors.
    """
    if not data:
        return None

    columns = list(data.keys())
    values = [data[c] for c in columns]
    sql = build_insert_sql(table, schema, columns, returning=returning_col, on_conflict=on_conflict)
    return conn.execute_write(sql, tuple(values))
