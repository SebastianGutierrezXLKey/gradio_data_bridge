"""Reusable Gradio component builders."""

from __future__ import annotations

import gradio as gr


def db_connection_block(label: str, defaults: dict) -> dict:
    """Build a connection form block.

    Returns a dict of Gradio component references keyed by field name.
    """
    with gr.Group():
        gr.Markdown(f"### {label}")
        with gr.Row():
            host = gr.Textbox(label="Host", value=defaults.get("host", "localhost"), scale=3)
            port = gr.Textbox(label="Port", value=defaults.get("port", "5432"), scale=1)
        with gr.Row():
            database = gr.Textbox(label="Database", value=defaults.get("database", ""), scale=2)
            schema = gr.Textbox(label="Schema", value=defaults.get("schema", "public"), scale=1)
        with gr.Row():
            user = gr.Textbox(label="Utilisateur", value=defaults.get("user", "postgres"), scale=2)
            password = gr.Textbox(
                label="Mot de passe", value=defaults.get("password", ""),
                type="password", scale=2
            )
        test_btn = gr.Button("Tester la connexion", variant="secondary", size="sm")
        status = gr.HTML(value='<span style="color:#888">— Non connecté</span>')

    return {
        "host": host,
        "port": port,
        "database": database,
        "schema": schema,
        "user": user,
        "password": password,
        "test_btn": test_btn,
        "status": status,
    }


def migration_log_area() -> gr.Textbox:
    """A large read-only log display area."""
    return gr.Textbox(
        label="Logs de migration",
        lines=20,
        max_lines=50,
        interactive=False,
    )
