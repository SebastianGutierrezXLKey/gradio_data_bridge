"""Migration engine — orchestrates dry-run and real migration."""

from __future__ import annotations

from typing import Generator

from loguru import logger

from audit.logger import AuditLogger
from database.connector import DBConnector
from database.reader import read_all_rows_batched
from database.schema import get_primary_key_columns
from database.writer import insert_row
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
    """Runs a single table migration with optional dry-run mode."""

    def __init__(
        self,
        source_conn: DBConnector,
        target_conn: DBConnector,
        mapping_config: MappingConfig,
        audit_logger: AuditLogger,
        batch_size: int = 100,
        dry_run: bool = True,
        on_error: str = "continue",  # "continue" | "abort"
    ) -> None:
        self.source_conn = source_conn
        self.target_conn = target_conn
        self.config = mapping_config
        self.audit = audit_logger
        self.batch_size = batch_size
        self.dry_run = dry_run
        self.on_error = on_error

    def run(
        self,
        source_table: str,
        source_schema: str,
        target_table: str,
        target_schema: str,
    ) -> Generator[ProgressUpdate, None, None]:
        """Migrate *source_table* → *target_table*.

        Yields ProgressUpdate events so the UI can display real-time feedback.
        """
        mode_label = "DRY RUN" if self.dry_run else "RÉEL"
        yield ProgressUpdate(0, 1, f"[{mode_label}] Démarrage de la migration : {source_table} → {target_table}")

        self.audit.log_table_start(source_table, target_table)

        # Resolve PK columns
        src_pk_cols = get_primary_key_columns(self.source_conn, source_table, source_schema)
        src_pk = src_pk_cols[0] if src_pk_cols else self.config.source_pk

        # In real mode, create the mapping table
        if not self.dry_run:
            create_mapping_table(self.target_conn, source_table)

        succeeded = 0
        failed = 0
        total = 0  # unknown until we finish iterating

        # We'll use a two-pass total estimation (fast, uses pg_class)
        from database.schema import get_row_count
        estimated_total = get_row_count(self.source_conn, source_table, source_schema)
        if estimated_total <= 0:
            estimated_total = 1  # avoid division by zero

        for batch in read_all_rows_batched(
            self.source_conn,
            source_table,
            source_schema,
            batch_size=self.batch_size,
            order_by=src_pk if src_pk else None,
        ):
            for row in batch:
                total += 1
                source_id = str(row.get(src_pk, ""))

                try:
                    # Apply mappings
                    mapped_row = apply_column_mapping(row, self.config)
                    mapped_row = apply_value_mapping(mapped_row, self.config)

                    if self.dry_run:
                        self.audit.log_success(source_table, source_id, "[dry_run]", mapped_row)
                        succeeded += 1
                    else:
                        # Determine the returning PK column on target
                        tgt_pk_cols = get_primary_key_columns(
                            self.target_conn, target_table, target_schema
                        )
                        tgt_pk = tgt_pk_cols[0] if tgt_pk_cols else self.config.target_pk

                        # Remove PK from insert if it will be auto-generated
                        insert_data = {
                            k: v for k, v in mapped_row.items()
                            if k != tgt_pk or v is not None
                        }

                        new_id = insert_row(
                            self.target_conn,
                            target_table,
                            target_schema,
                            insert_data,
                            returning_col=tgt_pk,
                        )
                        store_mapping(self.target_conn, source_table, source_id, str(new_id))
                        self.target_conn.commit()

                        self.audit.log_success(source_table, source_id, str(new_id))
                        succeeded += 1

                except Exception as exc:
                    failed += 1
                    error_msg = str(exc)
                    logger.error(f"Row {source_id} in {source_table} failed: {error_msg}")
                    self.audit.log_error(source_table, source_id, error_msg, row)

                    if not self.dry_run:
                        self.target_conn.rollback()

                    if self.on_error == "abort":
                        yield ProgressUpdate(
                            total, estimated_total,
                            f"Abandon suite à une erreur sur {source_table} id={source_id}: {error_msg}",
                            level="ERROR",
                        )
                        self.audit.log_table_end(source_table, succeeded, failed)
                        return

                yield ProgressUpdate(
                    total, estimated_total,
                    f"{source_table}: {total} lignes traitées ({succeeded} ok, {failed} erreurs)",
                )

        self.audit.log_table_end(source_table, succeeded, failed)
        status = "SUCCESS" if failed == 0 else "WARNING"
        yield ProgressUpdate(
            total, max(total, 1),
            f"Terminé : {source_table} → {target_table} | {succeeded} réussis, {failed} échoués",
            level=status,
        )
