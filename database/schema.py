"""Schema discovery using PostgreSQL information_schema queries."""

from __future__ import annotations

from typing import Any

from database.connector import DBConnector


def get_tables(conn: DBConnector, schema: str = "public") -> list[str]:
    """Return all user table names in *schema*, sorted alphabetically."""
    rows = conn.execute_query(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """,
        (schema,),
    )
    return [r["table_name"] for r in rows]


def get_columns(conn: DBConnector, table: str, schema: str = "public") -> list[dict[str, Any]]:
    """Return column metadata for *table*.

    Each dict contains: name, data_type, is_nullable, is_pk.
    """
    rows = conn.execute_query(
        """
        SELECT
            c.column_name          AS name,
            c.data_type            AS data_type,
            c.is_nullable          AS is_nullable,
            c.column_default       AS column_default,
            c.ordinal_position     AS ordinal_position,
            CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END AS is_pk
        FROM information_schema.columns c
        LEFT JOIN (
            SELECT ku.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage ku
              ON tc.constraint_name = ku.constraint_name
             AND tc.table_schema    = ku.table_schema
             AND tc.table_name      = ku.table_name
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_name   = %s
              AND tc.table_schema = %s
        ) pk ON c.column_name = pk.column_name
        WHERE c.table_name   = %s
          AND c.table_schema = %s
        ORDER BY c.ordinal_position
        """,
        (table, schema, table, schema),
    )
    return rows


def get_foreign_keys(
    conn: DBConnector, table: str, schema: str = "public"
) -> list[dict[str, str]]:
    """Return FK definitions for *table*.

    Each dict: column, ref_table, ref_column.
    """
    rows = conn.execute_query(
        """
        SELECT
            kcu.column_name                  AS column_name,
            ccu.table_name                   AS ref_table,
            ccu.column_name                  AS ref_column
        FROM information_schema.table_constraints        tc
        JOIN information_schema.key_column_usage         kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema    = kcu.table_schema
        JOIN information_schema.constraint_column_usage  ccu
          ON ccu.constraint_name = tc.constraint_name
         AND ccu.table_schema    = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_name   = %s
          AND tc.table_schema = %s
        """,
        (table, schema),
    )
    return [
        {
            "column": r["column_name"],
            "ref_table": r["ref_table"],
            "ref_column": r["ref_column"],
        }
        for r in rows
    ]


def get_row_count(conn: DBConnector, table: str, schema: str = "public") -> int:
    """Return the approximate row count for *table* using pg_class stats."""
    rows = conn.execute_query(
        """
        SELECT reltuples::bigint AS estimate
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = %s
          AND n.nspname = %s
        """,
        (table, schema),
    )
    if rows:
        estimate = rows[0]["estimate"]
        # Fall back to exact count when estimate is 0 (table not yet analyzed)
        if estimate <= 0:
            exact = conn.execute_query(
                f'SELECT COUNT(*) AS cnt FROM "{schema}"."{table}"'
            )
            return exact[0]["cnt"]
        return estimate
    return 0


def get_primary_key_columns(
    conn: DBConnector, table: str, schema: str = "public"
) -> list[str]:
    """Return the list of PK column names for *table*."""
    cols = get_columns(conn, table, schema)
    return [c["name"] for c in cols if c["is_pk"]]
