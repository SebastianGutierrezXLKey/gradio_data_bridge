"""Column and value mapping configuration."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class MappingConfig(BaseModel):
    """Holds all mapping rules for a single table migration."""

    # source_col → target_col (None means "skip this column")
    column_map: dict[str, str | None] = Field(default_factory=dict)

    # col_name → { "source_value" : "target_value" }
    value_maps: dict[str, dict[str, str]] = Field(default_factory=dict)

    # source PK column name (used to look up mapping table)
    source_pk: str = "id"

    # target PK column name (used to store new IDs)
    target_pk: str = "id"

    class Config:
        arbitrary_types_allowed = True


def apply_column_mapping(row: dict[str, Any], config: MappingConfig) -> dict[str, Any]:
    """Return a new dict with source columns renamed according to *config.column_map*.

    Columns mapped to None are dropped.
    Columns absent from the map are also dropped (strict mapping).
    """
    result: dict[str, Any] = {}
    for src_col, value in row.items():
        target_col = config.column_map.get(src_col)
        if target_col is not None:
            result[target_col] = value
    return result


def apply_value_mapping(row: dict[str, Any], config: MappingConfig) -> dict[str, Any]:
    """Replace values in *row* according to *config.value_maps*.

    Operates on the **target** column names (after column mapping).
    """
    result = dict(row)
    for col, vmap in config.value_maps.items():
        if col in result and result[col] is not None:
            str_val = str(result[col])
            if str_val in vmap:
                mapped = vmap[str_val]
                # Preserve None sentinel for explicit "no mapping"
                result[col] = None if mapped == "" else mapped
    return result


def validate_mapping(
    config: MappingConfig,
    source_cols: list[str],
    target_cols: list[str],
) -> list[str]:
    """Return a list of warning strings for potential mapping issues."""
    warnings: list[str] = []
    target_col_set = set(target_cols)

    for src, tgt in config.column_map.items():
        if src not in source_cols:
            warnings.append(f"Colonne source inconnue : '{src}'")
        if tgt is not None and tgt not in target_col_set:
            warnings.append(f"Colonne cible inconnue : '{tgt}' (pour source '{src}')")

    unmapped = [c for c in source_cols if c not in config.column_map]
    if unmapped:
        warnings.append(
            f"{len(unmapped)} colonne(s) source non mappée(s) : {', '.join(unmapped)}"
        )

    return warnings
