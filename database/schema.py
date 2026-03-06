"""Schema discovery using pg_catalog (much faster than information_schema)."""

from __future__ import annotations

from typing import Any

from database.connector import DBConnector


def get_tables(conn: DBConnector, schema: str = "public") -> list[str]:
    """Return all user table names in *schema*, sorted alphabetically."""
    rows = conn.execute_query(
        """
        SELECT c.relname AS table_name
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s
          AND c.relkind = 'r'
        ORDER BY c.relname
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
            a.attname                                        AS name,
            pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
            NOT a.attnotnull                                 AS is_nullable,
            pg_get_expr(d.adbin, d.adrelid)                  AS column_default,
            a.attnum                                         AS ordinal_position,
            COALESCE(pk.is_pk, false)                        AS is_pk
        FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_class c     ON c.oid = a.attrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_catalog.pg_attrdef d
               ON d.adrelid = a.attrelid AND d.adnum = a.attnum
        LEFT JOIN (
            SELECT unnest(ix.indkey) AS attnum, true AS is_pk
            FROM pg_catalog.pg_index ix
            JOIN pg_catalog.pg_class ci ON ci.oid = ix.indrelid
            JOIN pg_catalog.pg_namespace ni ON ni.oid = ci.relnamespace
            WHERE ix.indisprimary
              AND ci.relname = %s
              AND ni.nspname = %s
        ) pk ON pk.attnum = a.attnum
        WHERE c.relname = %s
          AND n.nspname = %s
          AND a.attnum  > 0
          AND NOT a.attisdropped
        ORDER BY a.attnum
        """,
        (table, schema, table, schema),
    )
    # Normalize is_nullable to match previous interface ("YES"/"NO" → bool)
    for r in rows:
        r["is_nullable"] = "YES" if r["is_nullable"] else "NO"
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
            a.attname        AS column_name,
            cf.relname       AS ref_table,
            af.attname       AS ref_column
        FROM pg_catalog.pg_constraint con
        JOIN pg_catalog.pg_class c     ON c.oid = con.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_catalog.pg_class cf    ON cf.oid = con.confrelid
        JOIN pg_catalog.pg_attribute a
               ON a.attrelid = con.conrelid  AND a.attnum = ANY(con.conkey)
        JOIN pg_catalog.pg_attribute af
               ON af.attrelid = con.confrelid AND af.attnum = ANY(con.confkey)
        WHERE con.contype = 'f'
          AND c.relname  = %s
          AND n.nspname  = %s
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
    """Return the approximate row count using pg_class stats."""
    rows = conn.execute_query(
        """
        SELECT c.reltuples::bigint AS estimate
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = %s
          AND n.nspname = %s
        """,
        (table, schema),
    )
    if rows:
        estimate = rows[0]["estimate"]
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
