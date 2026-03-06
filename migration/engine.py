"""Migration engine — orchestrates dry-run and real migration.

Supports two write modes:
- Direct DB: INSERT via psycopg2 (database/writer.py)
- API:        POST via HTTP (api/writer.py)
"""

from __future__ import annotations

from typing import Generator

from loguru import logger

from audit.logger import AuditLogger
from database.connector import DBConnector
from database.reader import read_all_rows_batched
from database.schema import get_primary_key_columns
from migration.mapper import MappingConfig, apply_column_mapping, apply_value_mapping
from migration.tracker import create_mapping_table, store_mapping


class ProgressUpdate:
    """Simple progress event yielded by the engine."""

    def __init__(self, current: int, total: int, message: str, level: str = "INFO") -> None:
        self.current = current
        self.total = total
        self.message = message
        self.level = level  # INFO | WARNING | ERROR | SUCCESS

    def __str__(self) -> str:
        pct = int(100 * self.current / self.total) if self.total else 0
        return f"[{self.level}] [{pct:3d}%] {self.message}"


class MigrationEngine:
    """Runs a single table migration — supports Direct DB and API write modes."""

    def __init__(
        self,
        source_conn: DBConnector,
        mapping_config: MappingConfig,
        audit_logger: AuditLogger,
        batch_size: int = 100,
        dry_run: bool = True,
        on_error: str = "continue",  # "continue" | "abort"
        # --- Direct DB mode ---
        target_conn: DBConnector | None = None,
        # --- API mode ---
        api_client=None,           # ApiClient | None
        api_endpoint: str = "",    # e.g. "/soil-sampling/imports"
        api_id_field: str = "id",  # field in response.data holding the new ID
    ) -> None:
        self.source_conn = source_conn
        self.target_conn = target_conn
        self.api_client = api_client
        self.api_endpoint = api_endpoint
        self.api_id_field = api_id_field
        self.config = mapping_config
        self.audit = audit_logger
        self.batch_size = batch_size
        self.dry_run = dry_run
        self.on_error = on_error

        if api_client is not None:
            self._write_mode = "api"
        elif target_conn is not None:
            self._write_mode = "db"
        else:
            raise ValueError("Either target_conn or api_client must be provided.")

    def run(
        self,
        source_table: str,
        source_schema: str,
        target_table: str = "",
        target_schema: str = "",
    ) -> Generator[ProgressUpdate, None, None]:
        """Migrate *source_table* → *target_table* / API endpoint.

        Yields ProgressUpdate events for real-time UI feedback.
        """
        destination = target_table or self.api_endpoint
        mode_label = "DRY RUN" if self.dry_run else self._write_mode.upper()
        yield ProgressUpdate(0, 1, f"[{mode_label}] Démarrage : {source_table} → {destination}")

        self.audit.log_table_start(source_table, destination)

        # Resolve source PK
        src_pk_cols = get_primary_key_columns(self.source_conn, source_table, source_schema)
        src_pk = src_pk_cols[0] if src_pk_cols else self.config.source_pk

        # In real DB mode, create the mapping table
        if not self.dry_run and self._write_mode == "db" and self.target_conn:
            create_mapping_table(self.target_conn, source_table)

        from database.schema import get_row_count
        estimated_total = get_row_count(self.source_conn, source_table, source_schema)
        if estimated_total <= 0:
            estimated_total = 1

        succeeded = 0
        failed = 0
        total = 0

        for batch in read_all_rows_batched(
            self.source_conn,
            source_table,
            source_schema,
            batch_size=self.batch_size,
            order_by=src_pk if src_pk else None,
        ):
            for row in batch:
                total += 1
                source_id = str(row.get(src_pk, total))

                try:
                    mapped_row = apply_column_mapping(row, self.config)
                    mapped_row = apply_value_mapping(mapped_row, self.config)

                    if self.dry_run:
                        self.audit.log_success(source_table, source_id, "[dry_run]", mapped_row)
                        succeeded += 1
                    elif self._write_mode == "api":
                        new_id = self._write_via_api(mapped_row)
                        self.audit.log_success(source_table, source_id, str(new_id))
                        succeeded += 1
                    else:
                        new_id = self._write_via_db(
                            mapped_row, source_table, target_table, target_schema, source_id
                        )
                        self.audit.log_success(source_table, source_id, str(new_id))
                        succeeded += 1

                except Exception as exc:
                    failed += 1
                    error_msg = str(exc)
                    logger.error(f"Row {source_id} in {source_table}: {error_msg}")
                    self.audit.log_error(source_table, source_id, error_msg, row)

                    if self._write_mode == "db" and self.target_conn:
                        self.target_conn.rollback()

                    if self.on_error == "abort":
                        yield ProgressUpdate(
                            total, estimated_total,
                            f"Abandon — {source_table} id={source_id}: {error_msg}",
                            level="ERROR",
                        )
                        self.audit.log_table_end(source_table, succeeded, failed)
                        return

                yield ProgressUpdate(
                    total, estimated_total,
                    f"{source_table}: {total} lignes ({succeeded} ok, {failed} erreurs)",
                )

        self.audit.log_table_end(source_table, succeeded, failed)
        status = "SUCCESS" if failed == 0 else "WARNING"
        yield ProgressUpdate(
            total, max(total, 1),
            f"Terminé : {source_table} → {destination} | {succeeded} ok, {failed} échoués",
            level=status,
        )

    # ------------------------------------------------------------------
    # Private write helpers
    # ------------------------------------------------------------------

    def _write_via_api(self, data: dict) -> str:
        from api.writer import post_record
        new_id = post_record(self.api_client, self.api_endpoint, data, self.api_id_field)
        return str(new_id) if new_id is not None else "?"

    def _write_via_db(
        self,
        data: dict,
        source_table: str,
        target_table: str,
        target_schema: str,
        source_id: str,
    ) -> str:
        from database.writer import insert_row
        tgt_pk_cols = get_primary_key_columns(self.target_conn, target_table, target_schema)
        tgt_pk = tgt_pk_cols[0] if tgt_pk_cols else self.config.target_pk
        insert_data = {k: v for k, v in data.items() if k != tgt_pk or v is not None}
        new_id = insert_row(
            self.target_conn, target_table, target_schema, insert_data, returning_col=tgt_pk
        )
        store_mapping(self.target_conn, source_table, source_id, str(new_id))
        self.target_conn.commit()
        return str(new_id)
