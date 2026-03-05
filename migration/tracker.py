"""Temporary mapping tables — tracks source_id → target_id per migrated table."""

from __future__ import annotations

from database.connector import DBConnector


_MAPPING_SCHEMA = "public"


def _mapping_table_name(source_table: str) -> str:
    return f"_mapping_{source_table}"


def create_mapping_table(target_conn: DBConnector, source_table: str) -> None:
    """Create (or re-create) the mapping table for *source_table* in the target DB."""
    tname = _mapping_table_name(source_table)
    target_conn.execute_write(
        f"""
        CREATE TABLE IF NOT EXISTS "{_MAPPING_SCHEMA}"."{tname}" (
            id          SERIAL PRIMARY KEY,
            source_id   TEXT NOT NULL,
            target_id   TEXT NOT NULL,
            migrated_at TIMESTAMP DEFAULT NOW()
        )
        """
    )
    target_conn.execute_write(
        f"""
        CREATE INDEX IF NOT EXISTS "{tname}_source_id_idx"
        ON "{_MAPPING_SCHEMA}"."{tname}" (source_id)
        """
    )
    target_conn.commit()


def store_mapping(
    target_conn: DBConnector,
    source_table: str,
    source_id: str,
    target_id: str,
) -> None:
    """Insert a (source_id, target_id) pair into the mapping table."""
    tname = _mapping_table_name(source_table)
    target_conn.execute_write(
        f'INSERT INTO "{_MAPPING_SCHEMA}"."{tname}" (source_id, target_id) VALUES (%s, %s)',
        (str(source_id), str(target_id)),
    )


def lookup_target_id(
    target_conn: DBConnector,
    source_table: str,
    source_id: str,
) -> str | None:
    """Return the target_id for *source_id*, or None if not found."""
    tname = _mapping_table_name(source_table)
    try:
        rows = target_conn.execute_query(
            f'SELECT target_id FROM "{_MAPPING_SCHEMA}"."{tname}" WHERE source_id = %s LIMIT 1',
            (str(source_id),),
        )
        return rows[0]["target_id"] if rows else None
    except Exception:
        return None


def drop_mapping_tables(target_conn: DBConnector, source_tables: list[str]) -> None:
    """Drop all mapping tables created for the given source tables."""
    for table in source_tables:
        tname = _mapping_table_name(table)
        target_conn.execute_write(
            f'DROP TABLE IF EXISTS "{_MAPPING_SCHEMA}"."{tname}"'
        )
    target_conn.commit()


def list_mapping_tables(target_conn: DBConnector) -> list[str]:
    """Return all existing _mapping_* table names in the target DB."""
    rows = target_conn.execute_query(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_name LIKE '_mapping_%%'
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """,
        (_MAPPING_SCHEMA,),
    )
    return [r["table_name"] for r in rows]
