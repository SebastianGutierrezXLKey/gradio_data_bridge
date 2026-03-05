"""Miscellaneous helper utilities."""

from __future__ import annotations

from typing import Any

import pandas as pd


def dataframe_to_display(df: pd.DataFrame, max_rows: int = 100) -> pd.DataFrame:
    """Truncate and convert all values to strings for safe Gradio display."""
    if df is None or df.empty:
        return pd.DataFrame()
    display = df.head(max_rows).copy()
    for col in display.columns:
        display[col] = display[col].astype(str)
    return display


def format_column_info(columns: list[dict[str, Any]]) -> str:
    """Return a human-readable summary of column metadata."""
    lines = []
    for col in columns:
        pk_mark = " [PK]" if col.get("is_pk") else ""
        nullable = "" if col.get("is_nullable") == "YES" else " NOT NULL"
        lines.append(f"  • {col['name']} ({col['data_type']}){nullable}{pk_mark}")
    return "\n".join(lines)


def format_fk_info(fks: list[dict[str, str]]) -> str:
    """Return a human-readable summary of foreign key metadata."""
    if not fks:
        return "  (aucune clé étrangère)"
    return "\n".join(
        f"  • {fk['column']} → {fk['ref_table']}.{fk['ref_column']}" for fk in fks
    )


def connection_badge(success: bool, message: str) -> str:
    """Return an HTML badge indicating connection status."""
    color = "#22c55e" if success else "#ef4444"
    icon = "✓" if success else "✗"
    return (
        f'<span style="background:{color};color:#fff;padding:4px 10px;'
        f'border-radius:4px;font-weight:bold;">{icon} {message}</span>'
    )
