"""CrossMigrate — Point d'entrée de l'application Gradio.

Migrez des données entre deux bases PostgreSQL avec mapping manuel,
gestion des clés étrangères et journalisation JSON.
"""

from __future__ import annotations

import gradio as gr

from ui.tabs import (
    build_tab_connexion,
    build_tab_migration,
    build_tab_mapping,
    build_tab_soil_sampling,
    build_tab_visualisation,
)


def build_app() -> gr.Blocks:
    """Assemble the full Gradio application."""

    with gr.Blocks(title="CrossMigrate — Migration PostgreSQL") as demo:

        gr.Markdown(
            """
            # CrossMigrate
            **Migration de données PostgreSQL → PostgreSQL** avec mapping manuel et audit JSON.

            > Pour la migration générale : **Connexion → Visualisation → Mapping → Migration**
            > Pour la migration Soil Sampling xlhub : **Connexion → Migration Soil Sampling**
            """
        )

        # Shared session state — holds DB connectors, mappings, etc.
        # Keys: source_conn, target_conn, source_schema, target_schema,
        #       source_db, target_db, column_map, value_maps,
        #       source_pk, target_pk
        app_state = gr.State(value={})

        # Tab 1
        build_tab_connexion(app_state)

        # Tab 2 — returns shared table dropdowns used by tabs 3 & 4
        src_table_dd, tgt_table_dd = build_tab_visualisation(app_state)

        # Tab 3
        build_tab_mapping(app_state, src_table_dd, tgt_table_dd)

        # Tab 4
        build_tab_migration(app_state, src_table_dd, tgt_table_dd)

        # Tab 5 — Soil Sampling Migration
        build_tab_soil_sampling(app_state)

    return demo


if __name__ == "__main__":
    app = build_app()
    app.launch(
        server_name="0.0.0.0",
        server_port=7860,
        show_error=True,
        theme=gr.themes.Soft(),
        css=".gradio-container { max-width: 1400px !important; }",
    )
