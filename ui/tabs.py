"""Tab layout builders for the CrossMigrate Gradio application."""

from __future__ import annotations

import gradio as gr
import pandas as pd

MAX_COLS = 60  # Maximum number of source columns supported in the mapping UI

from config import DBDefaults
from ui.callbacks import (
    build_column_mapping_ui,
    handle_connect,
    load_distinct_values,
    load_more_rows,
    load_tables,
    preview_table,
    run_migration,
    save_column_mapping,
    save_value_mapping,
    validate_current_mapping,
)
from ui.components import db_connection_block, migration_log_area


# ---------------------------------------------------------------------------
# Tab 1 — Connexion
# ---------------------------------------------------------------------------

def build_tab_connexion(app_state: gr.State) -> None:
    with gr.Tab("1 · Connexion"):
        with gr.Row():
            with gr.Column():
                src = db_connection_block(
                    "Base source",
                    {
                        "host": DBDefaults.SOURCE_HOST,
                        "port": DBDefaults.SOURCE_PORT,
                        "database": DBDefaults.SOURCE_DB,
                        "user": DBDefaults.SOURCE_USER,
                        "password": DBDefaults.SOURCE_PASSWORD,
                        "schema": DBDefaults.SOURCE_SCHEMA,
                    },
                )
            with gr.Column():
                tgt = db_connection_block(
                    "Base cible",
                    {
                        "host": DBDefaults.TARGET_HOST,
                        "port": DBDefaults.TARGET_PORT,
                        "database": DBDefaults.TARGET_DB,
                        "user": DBDefaults.TARGET_USER,
                        "password": DBDefaults.TARGET_PASSWORD,
                        "schema": DBDefaults.TARGET_SCHEMA,
                    },
                )

        # Wire source connect button
        src["test_btn"].click(
            fn=lambda h, p, d, u, pw, sc, st: handle_connect("source", h, p, d, u, pw, sc, st),
            inputs=[
                src["host"], src["port"], src["database"],
                src["user"], src["password"], src["schema"],
                app_state,
            ],
            outputs=[src["status"], app_state],
        )

        # Wire target connect button
        tgt["test_btn"].click(
            fn=lambda h, p, d, u, pw, sc, st: handle_connect("target", h, p, d, u, pw, sc, st),
            inputs=[
                tgt["host"], tgt["port"], tgt["database"],
                tgt["user"], tgt["password"], tgt["schema"],
                app_state,
            ],
            outputs=[tgt["status"], app_state],
        )


# ---------------------------------------------------------------------------
# Tab 2 — Sélection & Visualisation
# ---------------------------------------------------------------------------

def build_tab_visualisation(app_state: gr.State) -> tuple[gr.Dropdown, gr.Dropdown]:
    with gr.Tab("2 · Sélection & Visualisation"):
        with gr.Row():
            src_table_dd = gr.Dropdown(
                label="Table source", choices=[], interactive=False, scale=2
            )
            tgt_table_dd = gr.Dropdown(
                label="Table cible", choices=[], interactive=False, scale=2
            )
            refresh_btn = gr.Button("🔄 Rafraîchir les tables", scale=1)

        with gr.Row():
            with gr.Column():
                src_meta = gr.Markdown("*Sélectionnez une table source*")
                src_df = gr.Dataframe(
                    label="Aperçu source",
                    interactive=False,
                    wrap=True,
                    max_height=400,
                )
                src_more_btn = gr.Button("Charger 50 lignes supplémentaires", size="sm")
                src_offset = gr.State(value=0)

            with gr.Column():
                tgt_meta = gr.Markdown("*Sélectionnez une table cible*")
                tgt_df = gr.Dataframe(
                    label="Aperçu cible",
                    interactive=False,
                    wrap=True,
                    max_height=400,
                )
                tgt_more_btn = gr.Button("Charger 50 lignes supplémentaires", size="sm")
                tgt_offset = gr.State(value=0)

        # Refresh available tables
        def refresh_tables(state):
            src_dd = load_tables("source", state)
            tgt_dd = load_tables("target", state)
            return src_dd, tgt_dd

        refresh_btn.click(
            fn=refresh_tables,
            inputs=[app_state],
            outputs=[src_table_dd, tgt_table_dd],
        )

        # Preview source table on selection
        src_table_dd.change(
            fn=lambda t, st: (*preview_table("source", t, st), 0),
            inputs=[src_table_dd, app_state],
            outputs=[src_df, src_meta, src_offset],
        )

        # Preview target table on selection
        tgt_table_dd.change(
            fn=lambda t, st: (*preview_table("target", t, st), 0),
            inputs=[tgt_table_dd, app_state],
            outputs=[tgt_df, tgt_meta, tgt_offset],
        )

        # Load more source rows
        src_more_btn.click(
            fn=lambda t, st, off: load_more_rows("source", t, st, off),
            inputs=[src_table_dd, app_state, src_offset],
            outputs=[src_df, src_offset],
        )

        # Load more target rows
        tgt_more_btn.click(
            fn=lambda t, st, off: load_more_rows("target", t, st, off),
            inputs=[tgt_table_dd, app_state, tgt_offset],
            outputs=[tgt_df, tgt_offset],
        )

    return src_table_dd, tgt_table_dd


# ---------------------------------------------------------------------------
# Tab 3 — Mapping
# ---------------------------------------------------------------------------

def build_tab_mapping(
    app_state: gr.State,
    src_table_dd: gr.Dropdown,
    tgt_table_dd: gr.Dropdown,
) -> None:
    with gr.Tab("3 · Mapping"):
        gr.Markdown(
            "Configurez ici la correspondance des colonnes entre la table source "
            "et la table cible, puis mappez les valeurs des clés étrangères."
        )

        load_mapping_btn = gr.Button(
            "📋 Charger le mapping pour les tables sélectionnées", variant="primary"
        )

        # Session state
        src_cols_state = gr.State(value=[])

        # ----------------------------------------------------------------
        # Pre-create MAX_COLS dropdown rows (hidden by default)
        # Each row: source column label + target column dropdown
        # ----------------------------------------------------------------
        mapping_area = gr.Column(visible=False)
        col_rows: list[gr.Row] = []
        col_dropdowns: list[gr.Dropdown] = []

        with mapping_area:
            gr.Markdown("### Mapping des colonnes")
            gr.Markdown(
                "Pour chaque colonne source, sélectionnez la colonne cible "
                "dans la liste déroulante, ou choisissez **— Ne pas migrer —** pour l'exclure."
            )

            for i in range(MAX_COLS):
                with gr.Row(visible=False) as row:
                    dd = gr.Dropdown(
                        label=f"col_{i}",
                        choices=[],
                        interactive=True,
                        scale=1,
                    )
                col_rows.append(row)
                col_dropdowns.append(dd)

            # ---- FK section ----
            gr.Markdown("### Clés étrangères")
            fk_display = gr.Markdown("*Aucune clé étrangère détectée*")

            with gr.Accordion("Mapper les valeurs de clés étrangères", open=False):
                gr.Markdown(
                    "Sélectionnez une colonne FK pour configurer la correspondance des valeurs."
                )
                fk_col_selector = gr.Dropdown(
                    label="Colonne FK à mapper", choices=[], interactive=True
                )
                load_fk_values_btn = gr.Button("Charger les valeurs distinctes", size="sm")

                fk_values_area = gr.Column(visible=False)
                with fk_values_area:
                    fk_values_df = gr.Dataframe(
                        label="Mapping des valeurs (éditez la colonne 'Valeur cible')",
                        headers=["Valeur source", "Compteur", "Valeur cible"],
                        datatype=["str", "number", "str"],
                        interactive=True,
                        row_count=(1, "dynamic"),
                    )
                    save_fk_mapping_btn = gr.Button(
                        "💾 Sauvegarder ce mapping de valeurs", size="sm"
                    )
                    fk_save_status = gr.Markdown("")

            save_col_mapping_btn = gr.Button(
                "💾 Sauvegarder le mapping des colonnes", variant="secondary"
            )
            validate_btn = gr.Button("✅ Valider la configuration", variant="primary")
            validation_result = gr.Markdown("")

        # ---- Load mapping ----
        def _load_mapping(src_table, tgt_table, state):
            src_names, tgt_choices, fk_cols, fk_info = build_column_mapping_ui(
                src_table, tgt_table, state
            )
            if not src_names:
                empty_row_updates = []
                for _ in range(MAX_COLS):
                    empty_row_updates.append(gr.update(visible=False))
                    empty_row_updates.append(gr.update(label="", choices=[], value=None))
                return (
                    [gr.update(visible=False), src_names]
                    + empty_row_updates
                    + ["*Aucune clé étrangère*", gr.update(choices=[])]
                )

            existing_map = state.get("column_map", {})
            fk_md = (
                "**Clés étrangères détectées :**\n"
                + "\n".join(
                    f"  • `{fk['column']}` → `{fk['ref_table']}.{fk['ref_column']}`"
                    for fk in fk_info
                )
                if fk_info
                else "*Aucune clé étrangère détectée.*"
            )

            row_updates = []
            for i in range(MAX_COLS):
                if i < len(src_names):
                    col = src_names[i]
                    existing = existing_map.get(col)
                    val = "— Ne pas migrer —" if existing is None else (existing or tgt_choices[1] if len(tgt_choices) > 1 else "")
                    row_updates.append(gr.update(visible=True))
                    row_updates.append(gr.update(label=col, choices=tgt_choices, value=val))
                else:
                    row_updates.append(gr.update(visible=False))
                    row_updates.append(gr.update(label=f"col_{i}", choices=[], value=None))

            return (
                [gr.update(visible=True), src_names]
                + row_updates
                + [fk_md, gr.update(choices=fk_cols, interactive=bool(fk_cols))]
            )

        load_mapping_btn.click(
            fn=_load_mapping,
            inputs=[src_table_dd, tgt_table_dd, app_state],
            outputs=(
                [mapping_area, src_cols_state]
                + [item for pair in zip(col_rows, col_dropdowns) for item in pair]
                + [fk_display, fk_col_selector]
            ),
        )

        # ---- Save column mapping ----
        def _save_col_mapping(src_cols, state, *dropdown_values):
            if not src_cols:
                return state, "⚠ Aucun mapping à sauvegarder."
            targets = list(dropdown_values[: len(src_cols)])
            new_state = save_column_mapping(src_cols, targets, state)
            return new_state, f"✅ Mapping sauvegardé pour {len(src_cols)} colonne(s)."

        save_col_mapping_btn.click(
            fn=_save_col_mapping,
            inputs=[src_cols_state, app_state] + col_dropdowns,
            outputs=[app_state, validation_result],
        )

        # ---- Load FK distinct values ----
        def _load_fk_values(src_table, fk_col, state):
            if not fk_col:
                return gr.update(visible=False), pd.DataFrame()
            pairs = load_distinct_values(src_table, fk_col, state)
            existing_vmap = state.get("value_maps", {}).get(fk_col, {})
            rows = [
                [str(val), count, existing_vmap.get(str(val), "")]
                for val, count in pairs
            ]
            df = pd.DataFrame(rows, columns=["Valeur source", "Compteur", "Valeur cible"])
            return gr.update(visible=True), df

        load_fk_values_btn.click(
            fn=_load_fk_values,
            inputs=[src_table_dd, fk_col_selector, app_state],
            outputs=[fk_values_area, fk_values_df],
        )

        # ---- Save FK value mapping ----
        def _save_fk_mapping(fk_col, df_data, state):
            if not fk_col or df_data is None:
                return state, "⚠ Aucune donnée à sauvegarder."
            if isinstance(df_data, pd.DataFrame):
                rows = df_data.values.tolist()
            else:
                rows = df_data
            src_vals = [str(r[0]) for r in rows if r[0] is not None]
            tgt_vals = [
                str(r[2]) if len(r) > 2 and r[2] is not None else ""
                for r in rows
                if r[0] is not None
            ]
            new_state, msg = save_value_mapping(fk_col, src_vals, tgt_vals, state)
            return new_state, f"✅ {msg}"

        save_fk_mapping_btn.click(
            fn=_save_fk_mapping,
            inputs=[fk_col_selector, fk_values_df, app_state],
            outputs=[app_state, fk_save_status],
        )

        # ---- Validate mapping ----
        validate_btn.click(
            fn=validate_current_mapping,
            inputs=[src_table_dd, tgt_table_dd, app_state],
            outputs=[validation_result],
        )


# ---------------------------------------------------------------------------
# Tab 4 — Migration
# ---------------------------------------------------------------------------

def build_tab_migration(
    app_state: gr.State,
    src_table_dd: gr.Dropdown,
    tgt_table_dd: gr.Dropdown,
) -> None:
    with gr.Tab("4 · Migration"):
        with gr.Row():
            mode_radio = gr.Radio(
                choices=["Dry Run", "Réel"],
                value="Dry Run",
                label="Mode de migration",
                info="'Dry Run' simule la migration sans écrire en base.",
            )
            batch_slider = gr.Slider(
                minimum=10, maximum=1000, value=100, step=10,
                label="Taille de lot (lignes par transaction)",
            )
            on_error_radio = gr.Radio(
                choices=["Continuer", "Arrêter"],
                value="Continuer",
                label="En cas d'erreur",
            )

        with gr.Row():
            run_btn = gr.Button("🚀 Lancer la migration", variant="primary", scale=2)

        log_box = migration_log_area()

        with gr.Row(visible=False) as result_row:
            audit_file = gr.File(label="📄 Télécharger le fichier d'audit JSON", interactive=False)
            summary_md = gr.Markdown("")

        def _run_and_show(src_table, tgt_table, mode, batch, on_err, state, progress=gr.Progress()):
            logs, audit_path = run_migration(
                src_table, tgt_table, mode, batch, on_err, state, progress
            )
            if audit_path:
                return (
                    logs,
                    gr.Row(visible=True),
                    audit_path,
                    f"✅ Migration terminée. Fichier d'audit : `{audit_path}`",
                )
            return logs, gr.Row(visible=False), None, ""

        run_btn.click(
            fn=_run_and_show,
            inputs=[src_table_dd, tgt_table_dd, mode_radio, batch_slider, on_error_radio, app_state],
            outputs=[log_box, result_row, audit_file, summary_md],
            show_progress="full",
        )
