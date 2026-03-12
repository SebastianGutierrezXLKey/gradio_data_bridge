"""Tab layout builders for the CrossMigrate Gradio application."""

from __future__ import annotations

import gradio as gr
import pandas as pd

MAX_COLS = 60  # Maximum number of source columns supported in the mapping UI

from api.writer import KNOWN_ENDPOINTS
from config import ApiDefaults, DBDefaults
from ui.callbacks import (
    build_column_mapping_ui,
    handle_api_connect,
    handle_connect,
    load_distinct_values,
    load_more_rows,
    load_tables,
    preview_table,
    run_migration,
    save_column_mapping,
    save_value_mapping,
    ss_get_source_columns,
    ss_load_source_fields,
    ss_load_units_api,
    ss_load_units_db,
    ss_manage_lab,
    ss_run_migration,
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

        # ---- API configuration ----
        gr.Markdown("---")
        with gr.Accordion("🔌 Configuration API (mode écriture via API)", open=False):
            gr.Markdown(
                "Configurez ici le client API xlhub pour envoyer les données "
                "via les endpoints REST."
            )
            with gr.Row():
                api_base_url = gr.Textbox(
                    label="Base URL", value=ApiDefaults.BASE_URL, scale=3
                )
                api_version = gr.Textbox(
                    label="Version API", value=ApiDefaults.VERSION, scale=1
                )

            auth_mode_radio = gr.Radio(
                choices=["Token Bearer", "Compte de service", "Email / Mot de passe"],
                value="Token Bearer" if ApiDefaults.TOKEN else (
                    "Compte de service" if ApiDefaults.CLIENT_ID else "Email / Mot de passe"
                ),
                label="Mode d'authentification",
            )

            # Section Token Bearer
            with gr.Column(visible=bool(ApiDefaults.TOKEN or not (ApiDefaults.CLIENT_ID or ApiDefaults.LOGIN_EMAIL))) as token_section:
                api_token = gr.Textbox(
                    label="Token Bearer",
                    value=ApiDefaults.TOKEN,
                    type="password",
                )

            # Section Compte de service
            with gr.Column(visible=bool(ApiDefaults.CLIENT_ID)) as service_account_section:
                with gr.Row():
                    api_client_id = gr.Textbox(
                        label="Client ID",
                        value=ApiDefaults.CLIENT_ID,
                        scale=1,
                    )
                    api_client_secret = gr.Textbox(
                        label="Client Secret",
                        value=ApiDefaults.CLIENT_SECRET,
                        type="password",
                        scale=1,
                    )

            # Section Email / Mot de passe
            with gr.Column(visible=bool(ApiDefaults.LOGIN_EMAIL and not ApiDefaults.TOKEN and not ApiDefaults.CLIENT_ID)) as email_section:
                with gr.Row():
                    api_login_endpoint = gr.Textbox(
                        label="Endpoint de login", value=ApiDefaults.LOGIN_ENDPOINT, scale=1
                    )
                    api_email = gr.Textbox(
                        label="Email", value=ApiDefaults.LOGIN_EMAIL, scale=2
                    )
                    api_password = gr.Textbox(
                        label="Mot de passe", value=ApiDefaults.LOGIN_PASSWORD,
                        type="password", scale=2
                    )

            api_connect_btn = gr.Button("🔑 Connecter / Tester l'API", variant="secondary")
            api_status = gr.HTML('<span style="color:#888">— API non configurée</span>')

        # Show/hide auth sections based on radio
        def _toggle_auth_sections(mode):
            return (
                gr.update(visible=(mode == "Token Bearer")),
                gr.update(visible=(mode == "Compte de service")),
                gr.update(visible=(mode == "Email / Mot de passe")),
            )

        auth_mode_radio.change(
            fn=_toggle_auth_sections,
            inputs=[auth_mode_radio],
            outputs=[token_section, service_account_section, email_section],
        )

        api_connect_btn.click(
            fn=handle_api_connect,
            inputs=[
                api_base_url, api_version, auth_mode_radio,
                api_token, api_client_id, api_client_secret,
                api_login_endpoint, api_email, api_password,
                app_state,
            ],
            outputs=[api_status, app_state],
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
    endpoint_choices = [ep for ep in KNOWN_ENDPOINTS.keys()]
    endpoint_labels = {ep: f"{info['label']}  ({ep})" for ep, info in KNOWN_ENDPOINTS.items()}

    with gr.Tab("4 · Migration"):
        with gr.Row():
            mode_radio = gr.Radio(
                choices=["Dry Run", "Réel"],
                value="Dry Run",
                label="Mode de migration",
                info="'Dry Run' simule sans aucune écriture.",
            )
            write_mode_radio = gr.Radio(
                choices=["Direct DB", "Via API"],
                value="Direct DB",
                label="Mode d'écriture",
                info="'Via API' utilise les endpoints REST xlhub.",
            )

        # API endpoint selector (shown only in Via API mode)
        with gr.Row(visible=False) as api_endpoint_row:
            api_endpoint_dd = gr.Dropdown(
                label="Endpoint API cible",
                choices=[(v, k) for k, v in endpoint_labels.items()],
                value=endpoint_choices[0] if endpoint_choices else None,
                interactive=True,
                scale=3,
            )

        with gr.Row():
            batch_slider = gr.Slider(
                minimum=10, maximum=1000, value=100, step=10,
                label="Taille de lot",
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

        # Show/hide API endpoint row based on write mode
        def _toggle_api_row(write_mode):
            return gr.update(visible=(write_mode == "Via API"))

        write_mode_radio.change(
            fn=_toggle_api_row,
            inputs=[write_mode_radio],
            outputs=[api_endpoint_row],
        )

        def _run_and_show(
            src_table, tgt_table, mode, write_mode, api_endpoint,
            batch, on_err, state, progress=gr.Progress()
        ):
            logs, audit_path = run_migration(
                src_table, tgt_table, mode, write_mode, api_endpoint,
                batch, on_err, state, progress
            )
            if audit_path:
                return (
                    logs,
                    gr.update(visible=True),
                    audit_path,
                    f"✅ Migration terminée. Fichier d'audit : `{audit_path}`",
                )
            return logs, gr.update(visible=False), None, ""

        run_btn.click(
            fn=_run_and_show,
            inputs=[
                src_table_dd, tgt_table_dd,
                mode_radio, write_mode_radio, api_endpoint_dd,
                batch_slider, on_error_radio, app_state,
            ],
            outputs=[log_box, result_row, audit_file, summary_md],
            show_progress="full",
        )


# ---------------------------------------------------------------------------
# Tab 5 — Soil Sampling Migration
# ---------------------------------------------------------------------------

MAX_SS_ROWS = 100  # Maximum distinct FIELD values supported


def build_tab_soil_sampling(app_state: gr.State) -> None:  # noqa: C901
    """Gradio tab for the soil sampling campaigns/samples/results migration."""
    import json as _json
    from config import ApiDefaults

    with gr.Tab("5 · Migration Soil Sampling"):
        gr.Markdown(
            "## Migration Soil Sampling\n"
            "Migrez `xlkey.temp_analyses` → campagnes, imports, échantillons et résultats de lab."
        )

        # ----------------------------------------------------------------
        # Section 0 — Laboratoire
        # ----------------------------------------------------------------
        with gr.Accordion("🧪 Laboratoire", open=False):
            gr.Markdown("Créez ou vérifiez le laboratoire cible dans l'API avant de migrer.")
            with gr.Row():
                lab_name_inp = gr.Textbox(label="Nom", value=_env("LAB_NAME"), scale=2)
                lab_code_inp = gr.Textbox(label="Code", value=_env("LAB_CODE"), scale=1)
            with gr.Row():
                lab_address_inp = gr.Textbox(label="Adresse", value=_env("LAB_ADDRESS"), scale=2)
                lab_city_inp = gr.Textbox(label="Ville", value=_env("LAB_CITY"), scale=1)
                lab_province_inp = gr.Textbox(label="Province", value=_env("LAB_PROVINCE"), scale=1)
                lab_postal_inp = gr.Textbox(label="Code postal", value=_env("LAB_POSTAL_CODE"), scale=1)
            with gr.Row():
                lab_email_inp = gr.Textbox(label="Courriel", value=_env("LAB_CONTACT_EMAIL"), scale=2)
                lab_phone_inp = gr.Textbox(label="Téléphone", value=_env("LAB_CONTACT_PHONE"), scale=1)
                lab_country_inp = gr.Textbox(label="Pays", value=_env("LAB_COUNTRY"), scale=1)
                lab_formats_inp = gr.Textbox(
                    label="Formats supportés (JSON)", value=_env("LAB_SUPPORTED_FORMATS") or '["CSV"]', scale=1
                )
            with gr.Row():
                lab_check_btn = gr.Button("🔍 Vérifier", variant="secondary", scale=1)
                lab_create_btn = gr.Button("➕ Créer", variant="primary", scale=1)
                lab_delete_btn = gr.Button("🗑️ Supprimer", variant="stop", scale=1)
            lab_status = gr.Markdown("")

            def _lab_inputs():
                return [
                    lab_name_inp, lab_code_inp, lab_address_inp, lab_city_inp,
                    lab_province_inp, lab_postal_inp, lab_email_inp, lab_phone_inp,
                    lab_country_inp, lab_formats_inp, app_state,
                ]

            lab_check_btn.click(
                fn=lambda *a: ss_manage_lab("check", *a),
                inputs=_lab_inputs(),
                outputs=[lab_status],
            )
            lab_create_btn.click(
                fn=lambda *a: ss_manage_lab("create", *a),
                inputs=_lab_inputs(),
                outputs=[lab_status],
            )
            lab_delete_btn.click(
                fn=lambda *a: ss_manage_lab("delete", *a),
                inputs=_lab_inputs(),
                outputs=[lab_status],
            )

        # ----------------------------------------------------------------
        # Section 1 — Configuration
        # ----------------------------------------------------------------
        with gr.Accordion("⚙️ Configuration", open=True):
            with gr.Row():
                ss_source_table = gr.Textbox(
                    label="Table source",
                    value="xlkey.temp_analyses",
                    scale=2,
                )
                ss_limit = gr.Number(
                    label="Limite de lignes",
                    value=5,
                    precision=0,
                    scale=1,
                )
            with gr.Row():
                ss_filter_col = gr.Dropdown(
                    label="Colonne source pour filtrer",
                    choices=['"FILENAME"', '"FIELD"'],
                    value='"FILENAME"',
                    allow_custom_value=True,
                    interactive=True,
                    scale=2,
                )
                ss_filename_filter = gr.Textbox(
                    label="Valeur du filtre (ILIKE)",
                    placeholder="ex: 681",
                    scale=2,
                )
                ss_refresh_cols_btn = gr.Button("🔄", size="sm", scale=0)
            with gr.Row():
                ss_lab_name = gr.Textbox(
                    label="Nom du laboratoire (LAB_NAME)",
                    value=import_env_lab_name(),
                    scale=2,
                )
                ss_output_dir = gr.Textbox(
                    label="Dossier de sortie",
                    value="audit/output",
                    scale=2,
                )
                ss_dry_run = gr.Checkbox(label="Mode Dry Run", value=True)

            ss_load_fields_btn = gr.Button("🔍 Charger les données source", variant="secondary")
            ss_fields_status = gr.Markdown("")

        # ----------------------------------------------------------------
        # Section 2 — Source des unités
        # ----------------------------------------------------------------
        with gr.Accordion("🔗 Source des unités d'échantillonnage", open=True):
            units_source_radio = gr.Radio(
                choices=["Via API", "Via BD cible (SQL)"],
                value="Via API",
                label="Source des unités",
            )

            with gr.Row():
                units_filter_col = gr.Dropdown(
                    label="Filtrer par colonne",
                    choices=["name", "unit_type"],
                    value=None,
                    allow_custom_value=True,
                    interactive=True,
                    scale=2,
                )
                units_filter_val = gr.Textbox(
                    label="Valeur (contient)",
                    placeholder="ex: FR01",
                    scale=2,
                )
                units_apply_filter_btn = gr.Button("🔎 Filtrer", scale=1)

            with gr.Column(visible=True) as api_units_section:
                ss_load_units_btn = gr.Button("📥 Charger les unités depuis l'API", variant="secondary")

            with gr.Column(visible=False) as db_units_section:
                ss_sql_editor = gr.Code(
                    label="Requête SQL (sur la BD cible)",
                    value=(
                        "SELECT id, name, unit_type\n"
                        "FROM public.sampling_units\n"
                        "WHERE deleted_at IS NULL\n"
                        "ORDER BY name ASC"
                    ),
                    language="sql",
                )
                with gr.Row():
                    ss_sql_label_col = gr.Dropdown(
                        label="Colonne à afficher dans les listes déroulantes",
                        choices=["name"],
                        value="name",
                        interactive=True,
                        scale=2,
                    )
                    ss_exec_sql_btn = gr.Button("▶ Exécuter la requête", scale=1)

            ss_units_status = gr.Markdown("")
            # Hidden state holding the units list
            ss_units_state = gr.State(value=[])

        # ----------------------------------------------------------------
        # Section 3 — Mapping des unités
        # ----------------------------------------------------------------
        mapping_area = gr.Column(visible=False)
        field_rows: list[gr.Row] = []
        unit_dropdowns: list[gr.Dropdown] = []
        label_textboxes: list[gr.Textbox] = []

        with mapping_area:
            gr.Markdown("### Mapping FIELD → unité cible")
            gr.Markdown(
                "Pour chaque valeur distincte de `FIELD` dans la table source, "
                "sélectionnez l'unité d'échantillonnage cible et personnalisez le `sample_label`."
            )
            with gr.Row():
                ss_autofill_btn = gr.Button(
                    "✏️ Remplir sample_label avec valeur FIELD", size="sm", scale=1
                )
                ss_mapping_status = gr.Markdown("")

            for i in range(MAX_SS_ROWS):
                with gr.Row(visible=False) as row:
                    gr.Markdown(f"**—**", elem_id=f"ss_field_label_{i}")  # placeholder
                    unit_dd = gr.Dropdown(
                        label="Unité cible",
                        choices=[],
                        interactive=True,
                        scale=2,
                    )
                    label_tb = gr.Textbox(
                        label="sample_label",
                        placeholder="ex: FR01_1",
                        scale=2,
                    )
                field_rows.append(row)
                unit_dropdowns.append(unit_dd)
                label_textboxes.append(label_tb)

            # Hidden state holding per-row FIELD values
            ss_field_values_state = gr.State(value=[])

        # ----------------------------------------------------------------
        # Section 4 — Exécution
        # ----------------------------------------------------------------
        with gr.Row():
            ss_run_dry_btn = gr.Button("🔍 Lancer Dry Run", variant="secondary", scale=1)
            ss_run_btn = gr.Button("🚀 Lancer la migration", variant="primary", scale=1)

        ss_log_box = gr.Textbox(
            label="Journal",
            lines=25,
            max_lines=200,
            interactive=False,
        )

        # ----------------------------------------------------------------
        # Section 5 — Résultats
        # ----------------------------------------------------------------
        with gr.Row(visible=False) as ss_result_row:
            ss_json_file = gr.File(label="📄 JSON de migration", interactive=False)
            ss_log_file = gr.File(label="📝 Log texte", interactive=False)
        ss_summary_md = gr.Markdown("")

        # ----------------------------------------------------------------
        # Internal state for mapping JSON
        # ----------------------------------------------------------------
        ss_mapping_json = gr.State(value="{}")

        # ================================================================
        # Callbacks
        # ================================================================

        # Toggle units source sections
        def _toggle_units_source(mode):
            return (
                gr.update(visible=(mode == "Via API")),
                gr.update(visible=(mode == "Via BD cible (SQL)")),
            )

        units_source_radio.change(
            fn=_toggle_units_source,
            inputs=[units_source_radio],
            outputs=[api_units_section, db_units_section],
        )

        # Refresh source column dropdown when table changes or button clicked
        def _refresh_cols(source_table, state):
            cols = ss_get_source_columns(source_table, state)
            return gr.update(choices=cols, value=cols[0] if cols else None)

        ss_source_table.change(
            fn=_refresh_cols,
            inputs=[ss_source_table, app_state],
            outputs=[ss_filter_col],
        )
        ss_refresh_cols_btn.click(
            fn=_refresh_cols,
            inputs=[ss_source_table, app_state],
            outputs=[ss_filter_col],
        )

        # Load source fields → populate mapping rows (sorted alphabetically)
        def _load_source_fields(source_table, filter_col, filter_val, state):
            fields, status = ss_load_source_fields(source_table, filter_col, filter_val, state)
            # already sorted alpha by SQL, ensure Python sort as safety net
            fields = sorted(fields, key=lambda x: x[0])
            n = len(fields)
            row_updates = []
            field_labels = []
            for i in range(MAX_SS_ROWS):
                if i < n:
                    field_val, count = fields[i]
                    row_updates.append(gr.update(visible=True))
                    field_labels.append(field_val)
                else:
                    row_updates.append(gr.update(visible=False))
            return (
                [gr.update(visible=n > 0), status, field_labels]
                + row_updates
            )

        ss_load_fields_btn.click(
            fn=_load_source_fields,
            inputs=[ss_source_table, ss_filter_col, ss_filename_filter, app_state],
            outputs=[mapping_area, ss_fields_status, ss_field_values_state] + field_rows,
        )

        # Filter units list client-side by column/value
        def _filter_units(units: list[dict], col: str, val: str) -> list[dict]:
            if not col.strip() or not val.strip():
                return units
            val_lower = val.strip().lower()
            result = []
            for u in units:
                # Support JSONB-style: properties->>'key' → look in nested dict
                if "->>" in col:
                    parts = col.replace("'", "").split("->>")
                    obj = u.get(parts[0].strip(), {}) or {}
                    cell = str(obj.get(parts[-1].strip(), "")).lower()
                else:
                    cell = str(u.get(col.strip(), "")).lower()
                if val_lower in cell:
                    result.append(u)
            return result

        def _unit_col_choices(units: list[dict]) -> list[str]:
            """Extract column names from the first unit dict."""
            if not units:
                return ["name", "unit_type"]
            return list(units[0].keys())

        # Load units from API → update all dropdowns + populate filter col dropdown
        def _load_units_api(state, label_col):
            units, status = ss_load_units_api(state)
            choices = _units_to_choices(units, label_col)
            col_choices = _unit_col_choices(units)
            dd_updates = [gr.update(choices=choices) for _ in range(MAX_SS_ROWS)]
            return [status, units, gr.update(choices=col_choices)] + dd_updates

        def _units_to_choices(units: list[dict], label_col: str) -> list[tuple[str, str]]:
            result = []
            for u in units:
                uid = str(u.get("id", ""))
                label = str(u.get(label_col) or u.get("name") or uid)
                unit_type = u.get("unit_type", "")
                display = f"[{uid}] {label}" + (f" ({unit_type})" if unit_type else "")
                result.append((display, uid))
            return result

        ss_load_units_btn.click(
            fn=lambda state, col: _load_units_api(state, col),
            inputs=[app_state, ss_sql_label_col],
            outputs=[ss_units_status, ss_units_state, units_filter_col] + unit_dropdowns,
        )

        # Apply client-side filter on loaded units
        def _apply_units_filter(units, filter_col, filter_val, label_col):
            filtered = _filter_units(units, filter_col, filter_val)
            choices = _units_to_choices(filtered, label_col)
            dd_updates = [gr.update(choices=choices) for _ in range(MAX_SS_ROWS)]
            return [f"🔎 {len(filtered)} unité(s) après filtre"] + dd_updates

        units_apply_filter_btn.click(
            fn=_apply_units_filter,
            inputs=[ss_units_state, units_filter_col, units_filter_val, ss_sql_label_col],
            outputs=[ss_units_status] + unit_dropdowns,
        )

        # Execute SQL → update all dropdowns
        def _exec_sql_units(sql, label_col, state):
            rows, status = ss_load_units_db(sql, state)
            cols = list(rows[0].keys()) if rows else ["name"]
            choices = _units_to_choices(rows, label_col)
            dd_updates = [gr.update(choices=choices) for _ in range(MAX_SS_ROWS)]
            return [status, rows, gr.update(choices=cols), gr.update(choices=cols)] + dd_updates

        ss_exec_sql_btn.click(
            fn=_exec_sql_units,
            inputs=[ss_sql_editor, ss_sql_label_col, app_state],
            outputs=[ss_units_status, ss_units_state, ss_sql_label_col, units_filter_col] + unit_dropdowns,
        )

        # Re-populate dropdowns when label column changes
        ss_sql_label_col.change(
            fn=lambda col, units: [gr.update(choices=_units_to_choices(units, col)) for _ in range(MAX_SS_ROWS)],
            inputs=[ss_sql_label_col, ss_units_state],
            outputs=unit_dropdowns,
        )

        # Auto-fill sample_label with FIELD value
        def _autofill_labels(field_values):
            updates = []
            for i in range(MAX_SS_ROWS):
                if i < len(field_values):
                    updates.append(gr.update(value=field_values[i]))
                else:
                    updates.append(gr.update())
            return updates

        ss_autofill_btn.click(
            fn=_autofill_labels,
            inputs=[ss_field_values_state],
            outputs=label_textboxes,
        )

        # Build mapping JSON from current dropdown/textbox values
        def _build_mapping_json(field_values, *dd_and_labels):
            mid = len(dd_and_labels) // 2
            unit_ids = dd_and_labels[:mid]
            labels = dd_and_labels[mid:]
            mapping = {}
            for i, field_val in enumerate(field_values):
                if i >= len(unit_ids):
                    break
                uid = unit_ids[i]
                lbl = labels[i] if i < len(labels) else field_val
                if uid:
                    mapping[field_val] = {
                        "unit_id": str(uid),
                        "sample_label": str(lbl) if lbl else field_val,
                    }
            return _json.dumps(mapping, ensure_ascii=False)

        # Run migration (dry run or real)
        def _run(dry, source_table, filename_filter, limit, lab_name, output_dir,
                 field_values, state, *dd_and_labels):
            mapping_json = _build_mapping_json(field_values, *dd_and_labels)
            log_acc = ""
            json_path = None
            log_path = None
            for log_text, jp, lp in ss_run_migration(
                source_table, filename_filter, limit, dry,
                lab_name, output_dir, mapping_json, state,
            ):
                log_acc = log_text
                if jp:
                    json_path = jp
                if lp:
                    log_path = lp
                yield (
                    log_acc,
                    gr.update(visible=False), None, None,
                    "",
                )
            # Final update with files
            has_files = bool(json_path or log_path)
            yield (
                log_acc,
                gr.update(visible=has_files),
                json_path,
                log_path,
                f"✅ Terminé. {('Fichiers sauvegardés.' if has_files else 'Dry run — aucun fichier écrit.')}",
            )

        common_run_inputs = [
            ss_source_table, ss_filename_filter, ss_limit,
            ss_lab_name, ss_output_dir,
            ss_field_values_state, app_state,
        ] + unit_dropdowns + label_textboxes

        common_run_outputs = [
            ss_log_box, ss_result_row, ss_json_file, ss_log_file, ss_summary_md,
        ]

        ss_run_dry_btn.click(
            fn=lambda *args: _run(True, *args),
            inputs=common_run_inputs,
            outputs=common_run_outputs,
        )

        ss_run_btn.click(
            fn=lambda *args: _run(False, *args),
            inputs=common_run_inputs,
            outputs=common_run_outputs,
        )


def import_env_lab_name() -> str:
    """Read LAB_NAME from environment (best-effort)."""
    import os
    return os.getenv("LAB_NAME", "")


def _env(key: str) -> str:
    """Read an env variable (best-effort, empty string if missing)."""
    import os
    return os.getenv(key, "")
