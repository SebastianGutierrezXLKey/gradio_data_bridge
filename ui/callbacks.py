"""All Gradio event handler functions (pure logic, no layout)."""

from __future__ import annotations

import json
from typing import Any, Generator

import gradio as gr
import pandas as pd

from api.client import ApiClient
from audit.logger import AuditLogger
from config import ApiDefaults, DBDefaults, DEFAULT_PREVIEW_ROWS
from database.connector import DBConnector
from database.reader import read_distinct_values, read_rows
from database.schema import (
    get_columns,
    get_foreign_keys,
    get_row_count,
    get_tables,
)
from migration.engine import MigrationEngine
from migration.mapper import MappingConfig, validate_mapping
from utils.helpers import connection_badge, dataframe_to_display, format_column_info, format_fk_info


# ---------------------------------------------------------------------------
# Tab 1 — Connection
# ---------------------------------------------------------------------------

def handle_connect(
    side: str,  # "source" | "target"
    host: str,
    port: str,
    database: str,
    user: str,
    password: str,
    schema: str,
    state: dict,
) -> tuple[str, dict]:
    """Connect to a database and return (html_badge, updated_state)."""
    conn = DBConnector()
    success, message = conn.connect(host, port, database, user, password, schema)
    badge = connection_badge(success, message)

    if success:
        state = dict(state)
        state[f"{side}_conn"] = conn
        state[f"{side}_schema"] = schema
        state[f"{side}_db"] = database

    return badge, state


# ---------------------------------------------------------------------------
# Tab 2 — Table selection & preview
# ---------------------------------------------------------------------------

def load_tables(side: str, state: dict) -> gr.Dropdown:
    """Return updated Dropdown choices for source or target tables."""
    conn: DBConnector | None = state.get(f"{side}_conn")
    schema: str = state.get(f"{side}_schema", "public")
    if conn is None or not conn.is_connected():
        return gr.Dropdown(choices=[], label=f"Tables {side}", interactive=False)
    tables = get_tables(conn, schema)
    return gr.Dropdown(choices=tables, label=f"Tables {side}", interactive=True)


def preview_table(
    side: str, table: str, state: dict, offset: int = 0
) -> tuple[pd.DataFrame, str]:
    """Return (dataframe_preview, metadata_text) for *table*."""
    conn: DBConnector | None = state.get(f"{side}_conn")
    schema: str = state.get(f"{side}_schema", "public")
    if conn is None or not table:
        return pd.DataFrame(), ""

    df = read_rows(conn, table, schema, limit=DEFAULT_PREVIEW_ROWS, offset=offset)
    cols = get_columns(conn, table, schema)
    fks = get_foreign_keys(conn, table, schema)
    count = get_row_count(conn, table, schema)

    meta = (
        f"**{table}** — {count:,} lignes estimées · {len(cols)} colonnes\n\n"
        f"**Colonnes :**\n{format_column_info(cols)}\n\n"
        f"**Clés étrangères :**\n{format_fk_info(fks)}"
    )
    return dataframe_to_display(df), meta


def load_more_rows(
    side: str, table: str, state: dict, current_offset: int
) -> tuple[pd.DataFrame, int]:
    """Append 50 more rows to the current preview."""
    conn: DBConnector | None = state.get(f"{side}_conn")
    schema: str = state.get(f"{side}_schema", "public")
    if conn is None or not table:
        return pd.DataFrame(), current_offset

    new_offset = current_offset + 50
    df = read_rows(conn, table, schema, limit=50, offset=new_offset)
    return dataframe_to_display(df), new_offset


# ---------------------------------------------------------------------------
# Tab 3 — Mapping
# ---------------------------------------------------------------------------

def build_column_mapping_ui(
    source_table: str,
    target_table: str,
    state: dict,
) -> tuple[list[str], list[str], list[str], list[dict[str, Any]]]:
    """Compute data needed to render column mapping dropdowns.

    Returns:
        (source_col_names, target_col_choices, fk_columns, fk_info_list)
    """
    src_conn: DBConnector | None = state.get("source_conn")
    tgt_conn: DBConnector | None = state.get("target_conn")
    src_schema = state.get("source_schema", "public")
    tgt_schema = state.get("target_schema", "public")

    if src_conn is None or tgt_conn is None or not source_table or not target_table:
        return [], [], [], []

    src_cols = get_columns(src_conn, source_table, src_schema)
    tgt_cols = get_columns(tgt_conn, target_table, tgt_schema)
    fks = get_foreign_keys(src_conn, source_table, src_schema)

    src_names = [c["name"] for c in src_cols]
    tgt_choices = ["— Ne pas migrer —"] + [c["name"] for c in tgt_cols]
    fk_cols = [fk["column"] for fk in fks]

    return src_names, tgt_choices, fk_cols, fks


def save_column_mapping(
    source_cols: list[str],
    selected_targets: list[str],
    state: dict,
) -> dict:
    """Persist the column mapping into session state."""
    state = dict(state)
    col_map: dict[str, str | None] = {}
    for src, tgt in zip(source_cols, selected_targets):
        col_map[src] = None if tgt == "— Ne pas migrer —" else tgt
    state["column_map"] = col_map
    return state


def load_distinct_values(
    source_table: str, column: str, state: dict
) -> list[tuple[str, int]]:
    """Return distinct (value, count) for a FK column in the source table."""
    conn: DBConnector | None = state.get("source_conn")
    schema = state.get("source_schema", "public")
    if conn is None or not source_table or not column:
        return []
    return read_distinct_values(conn, source_table, column, schema)


def save_value_mapping(
    column: str,
    source_values: list[str],
    target_values: list[str],
    state: dict,
) -> tuple[dict, str]:
    """Save a value mapping for *column* into session state."""
    state = dict(state)
    vmap = state.get("value_maps", {})
    vmap[column] = {src: tgt for src, tgt in zip(source_values, target_values)}
    state["value_maps"] = vmap
    return state, f"Mapping valeurs sauvegardé pour '{column}' ({len(source_values)} valeurs)"


def build_mapping_config(state: dict) -> MappingConfig:
    """Construct a MappingConfig from session state."""
    return MappingConfig(
        column_map=state.get("column_map", {}),
        value_maps=state.get("value_maps", {}),
        source_pk=state.get("source_pk", "id"),
        target_pk=state.get("target_pk", "id"),
    )


def validate_current_mapping(
    source_table: str,
    target_table: str,
    state: dict,
) -> str:
    """Return a validation report for the current mapping."""
    src_conn = state.get("source_conn")
    tgt_conn = state.get("target_conn")
    src_schema = state.get("source_schema", "public")
    tgt_schema = state.get("target_schema", "public")

    if not src_conn or not tgt_conn:
        return "⚠ Connexions non établies."

    src_cols = [c["name"] for c in get_columns(src_conn, source_table, src_schema)]
    tgt_cols = [c["name"] for c in get_columns(tgt_conn, target_table, tgt_schema)]
    config = build_mapping_config(state)
    warnings = validate_mapping(config, src_cols, tgt_cols)

    if not warnings:
        return "✅ Configuration de mapping valide — aucun problème détecté."
    return "⚠ Avertissements :\n" + "\n".join(f"  • {w}" for w in warnings)


# ---------------------------------------------------------------------------
# Tab 1 — API connection
# ---------------------------------------------------------------------------

def handle_api_connect(
    base_url: str,
    api_version: str,
    auth_mode: str,
    token: str,
    client_id: str,
    client_secret: str,
    login_endpoint: str,
    email: str,
    password: str,
    state: dict,
) -> tuple[str, dict]:
    """Configure the API client and return (html_badge, updated_state).

    auth_mode: "Token Bearer" | "Compte de service" | "Email / Mot de passe"
    """
    client = ApiClient()
    client.configure(base_url, api_version, token if auth_mode == "Token Bearer" else "")

    if auth_mode == "Token Bearer":
        success, message = client.test_connection()
    elif auth_mode == "Compte de service":
        success, message = client.login_service_account(client_id, client_secret)
    else:
        success, message = client.login(login_endpoint, email, password)

    badge = connection_badge(success, message)
    if success:
        state = dict(state)
        state["api_client"] = client
    return badge, state


# ---------------------------------------------------------------------------
# Tab 5 — Soil Sampling Migration
# ---------------------------------------------------------------------------

def ss_load_source_fields(
    source_table: str,
    filename_filter: str,
    state: dict,
) -> tuple[list[tuple[str, int]], str]:
    """Load distinct FIELD values from the source table.

    Returns (field_list, status_message)
    """
    from audit.scripts.soil_sampling_runner import fetch_distinct_fields

    conn = state.get("source_conn")
    if conn is None or not conn.is_connected():
        return [], "❌ Connexion source non établie."
    if not source_table:
        return [], "❌ Veuillez saisir le nom de la table source."
    try:
        fields = fetch_distinct_fields(
            conn, source_table, filename_filter.strip() or None
        )
        return fields, f"✅ {len(fields)} valeur(s) FIELD distincte(s) trouvées."
    except Exception as exc:
        return [], f"❌ Erreur : {exc}"


def ss_load_units_api(state: dict) -> tuple[list[dict], str]:
    """Fetch sampling units from the xlhub API."""
    from audit.scripts.soil_sampling_runner import fetch_units_from_api

    api_client = state.get("api_client")
    if not api_client or not api_client.is_configured():
        return [], "❌ Client API non configuré."
    try:
        units = fetch_units_from_api(
            api_client.session, api_client.base_url, api_client.api_version
        )
        return units, f"✅ {len(units)} unité(s) chargée(s) depuis l'API."
    except Exception as exc:
        return [], f"❌ Erreur API : {exc}"


def ss_load_units_db(sql: str, state: dict) -> tuple[list[dict], str]:
    """Execute a custom SQL query on the target DB to fetch sampling units."""
    from audit.scripts.soil_sampling_runner import fetch_units_from_db

    conn = state.get("target_conn")
    if conn is None or not conn.is_connected():
        return [], "❌ Connexion BD cible non établie."
    if not sql.strip():
        return [], "❌ Requête SQL vide."
    try:
        rows = fetch_units_from_db(conn, sql.strip())
        return rows, f"✅ {len(rows)} unité(s) retournées par la requête SQL."
    except Exception as exc:
        return [], f"❌ Erreur SQL : {exc}"


def ss_run_migration(
    source_table: str,
    filename_filter: str,
    limit: int,
    dry_run: bool,
    lab_name: str,
    output_dir_str: str,
    unit_mapping_json: str,     # JSON string: {"FIELD": {"unit_id": "...", "sample_label": "..."}}
    state: dict,
) -> Generator[tuple[str, str | None, str | None], None, None]:
    """Run the soil sampling migration; yields (log_text, json_path, log_path)."""
    import json as _json
    from pathlib import Path
    from audit.scripts.soil_sampling_runner import run_migration

    api_client = state.get("api_client")
    source_conn = state.get("source_conn")

    if not api_client or not api_client.is_configured():
        yield "❌ Client API non configuré.", None, None
        return
    if not source_conn or not source_conn.is_connected():
        yield "❌ Connexion source non établie.", None, None
        return

    try:
        unit_mapping = _json.loads(unit_mapping_json) if unit_mapping_json else {}
    except Exception:
        yield "❌ Mapping invalide (JSON malformé).", None, None
        return

    if not unit_mapping:
        yield "❌ Aucun mapping configuré.", None, None
        return

    output_dir = Path(output_dir_str.strip()) if output_dir_str.strip() else Path("audit/output")
    log_accumulator: list[str] = []
    json_path = None
    log_path = None

    for line in run_migration(
        source_conn=source_conn,
        api_session=api_client.session,
        api_base=api_client.base_url,
        api_version=api_client.api_version,
        unit_mapping=unit_mapping,
        lab_name=lab_name.strip(),
        source_table=source_table.strip(),
        filename_filter=filename_filter.strip() or None,
        limit=int(limit),
        dry_run=dry_run,
        output_dir=output_dir,
    ):
        # Last line is a JSON completion signal
        if line.startswith('{"json_path"'):
            try:
                result = _json.loads(line)
                json_path = result.get("json_path")
                log_path = result.get("log_path")
            except Exception:
                pass
            continue
        log_accumulator.append(line)
        yield "\n".join(log_accumulator), None, None

    yield "\n".join(log_accumulator), json_path, log_path


# ---------------------------------------------------------------------------
# Tab 4 — Migration
# ---------------------------------------------------------------------------

def run_migration(
    source_table: str,
    target_table: str,
    mode: str,           # "Dry Run" | "Réel"
    write_mode: str,     # "Direct DB" | "Via API"
    api_endpoint: str,   # e.g. "/soil-sampling/imports"
    batch_size: int,
    on_error: str,       # "Continuer" | "Arrêter"
    state: dict,
    progress: gr.Progress = gr.Progress(),
) -> tuple[str, str | None]:
    """Execute the migration and return (log_text, audit_file_path)."""
    src_conn: DBConnector | None = state.get("source_conn")
    src_schema = state.get("source_schema", "public")

    dry_run = (mode == "Dry Run")
    use_api = (write_mode == "Via API") and not dry_run
    config = build_mapping_config(state)

    if not src_conn:
        return "❌ Connexion source non établie. Allez à l'onglet Connexion.", None
    if not source_table:
        return "❌ Veuillez sélectionner la table source.", None
    if not config.column_map:
        return "❌ Aucun mapping de colonnes configuré. Allez à l'onglet Mapping.", None

    # Validate write target
    if use_api:
        api_client = state.get("api_client")
        if not api_client or not api_client.is_configured():
            return "❌ Client API non configuré. Allez à l'onglet Connexion → section API.", None
        if not api_endpoint:
            return "❌ Veuillez sélectionner un endpoint API.", None
        tgt_conn = None
        tgt_schema = ""
        target_label = f"API {api_endpoint}"
    else:
        tgt_conn: DBConnector | None = state.get("target_conn")
        tgt_schema = state.get("target_schema", "public")
        api_client = None
        target_label = f"{state.get('target_db', '?')}.{tgt_schema}.{target_table}"
        if not dry_run and not tgt_conn:
            return "❌ Connexion cible non établie.", None
        if not target_table:
            return "❌ Veuillez sélectionner la table cible.", None

    audit = AuditLogger()
    migration_id = audit.start_session(
        "dry_run" if dry_run else ("api" if use_api else "real"),
        src_conn.db_name,
        state.get("target_db", api_endpoint),
    )

    engine = MigrationEngine(
        source_conn=src_conn,
        mapping_config=config,
        audit_logger=audit,
        batch_size=int(batch_size),
        dry_run=dry_run,
        on_error="continue" if on_error == "Continuer" else "abort",
        target_conn=tgt_conn,
        api_client=api_client,
        api_endpoint=api_endpoint,
    )

    log_lines: list[str] = [
        f"=== Migration {migration_id} ===",
        f"Mode : {'DRY RUN (simulation)' if dry_run else write_mode.upper()}",
        f"Source : {src_conn.db_name}.{src_schema}.{source_table}",
        f"Cible  : {target_label}",
        f"Taille de lot : {batch_size}",
        "",
    ]

    last_current, last_total = 0, 1
    for update in engine.run(source_table, src_schema, target_table or "", tgt_schema or ""):
        log_lines.append(str(update))
        last_current = update.current
        last_total = max(update.total, 1)
        progress(last_current / last_total, desc=update.message)

    log_lines.append("\n=== Génération du fichier d'audit ===")
    audit_path = audit.to_json_file(
        column_maps=config.column_map,
        value_maps=config.value_maps,
    )
    log_lines.append(f"Fichier d'audit : {audit_path}")

    return "\n".join(log_lines), audit_path
