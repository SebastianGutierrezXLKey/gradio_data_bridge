"""JSON audit logger for migration sessions."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from config import AUDIT_OUTPUT_DIR


class AuditLogger:
    """Accumulates migration events and writes a structured JSON audit file."""

    def __init__(self) -> None:
        self._migration_id: str = ""
        self._start: str = ""
        self._end: str = ""
        self._mode: str = ""
        self._source_db: str = ""
        self._target_db: str = ""
        self._tables: list[dict[str, Any]] = []
        self._current_table: dict[str, Any] | None = None
        self._errors: list[dict[str, Any]] = []
        self._dry_run_preview: list[dict[str, Any]] = []

    # ------------------------------------------------------------------
    # Session lifecycle
    # ------------------------------------------------------------------

    def start_session(self, mode: str, source_db: str, target_db: str) -> str:
        """Initialise a new audit session.

        Args:
            mode: 'dry_run' or 'real'
            source_db: Source database name.
            target_db: Target database name.

        Returns:
            migration_id string (timestamp-based).
        """
        self._migration_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self._start = datetime.now().isoformat()
        self._mode = mode
        self._source_db = source_db
        self._target_db = target_db
        self._tables = []
        self._errors = []
        self._dry_run_preview = []
        self._current_table = None
        return self._migration_id

    def log_table_start(self, source_table: str, target_table: str) -> None:
        """Open a new table entry."""
        self._current_table = {
            "source_table": source_table,
            "target_table": target_table,
            "records_attempted": 0,
            "records_succeeded": 0,
            "records_failed": 0,
        }

    def log_success(
        self,
        table: str,
        source_id: str,
        target_id: str,
        row_preview: dict[str, Any] | None = None,
    ) -> None:
        """Record a successful row insertion."""
        if self._current_table:
            self._current_table["records_attempted"] += 1
            self._current_table["records_succeeded"] += 1
        if self._mode == "dry_run" and row_preview is not None:
            self._dry_run_preview.append({
                "source_id": source_id,
                "table": table,
                "would_insert": row_preview,
            })

    def log_error(
        self,
        table: str,
        source_id: str,
        error_msg: str,
        row_data: dict[str, Any] | None = None,
    ) -> None:
        """Record a failed row."""
        if self._current_table:
            self._current_table["records_attempted"] += 1
            self._current_table["records_failed"] += 1
        self._errors.append({
            "timestamp": datetime.now().isoformat(),
            "table": table,
            "source_id": source_id,
            "error": error_msg,
            "data": _safe_serialize(row_data) if row_data else None,
        })

    def log_table_end(
        self, source_table: str, succeeded: int, failed: int
    ) -> None:
        """Close the current table entry."""
        if self._current_table:
            # Overwrite with engine-computed counts (more reliable)
            self._current_table["records_succeeded"] = succeeded
            self._current_table["records_failed"] = failed
            self._current_table["records_attempted"] = succeeded + failed
            self._tables.append(self._current_table)
            self._current_table = None

    # ------------------------------------------------------------------
    # Finalisation
    # ------------------------------------------------------------------

    def finalize(
        self,
        column_maps: dict[str, Any] | None = None,
        value_maps: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Build and return the complete audit document."""
        self._end = datetime.now().isoformat()
        doc: dict[str, Any] = {
            "migration_id": self._migration_id,
            "timestamp_start": self._start,
            "timestamp_end": self._end,
            "mode": self._mode,
            "source_db": self._source_db,
            "target_db": self._target_db,
            "tables_migrated": self._tables,
            "errors": self._errors,
            "mappings_used": {
                "columns": column_maps or {},
                "values": value_maps or {},
            },
        }
        if self._mode == "dry_run":
            doc["dry_run_preview"] = self._dry_run_preview
        return doc

    def to_json_file(
        self,
        column_maps: dict[str, Any] | None = None,
        value_maps: dict[str, Any] | None = None,
        output_dir: Path | None = None,
    ) -> str:
        """Write the audit document to a JSON file.

        Returns:
            Absolute path of the written file.
        """
        doc = self.finalize(column_maps, value_maps)
        directory = output_dir or AUDIT_OUTPUT_DIR
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"audit_{self._migration_id}.json"
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(doc, fh, indent=2, ensure_ascii=False, default=str)
        return str(path)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_serialize(obj: Any) -> Any:
    """Convert non-JSON-serialisable values to strings recursively."""
    if isinstance(obj, dict):
        return {k: _safe_serialize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_safe_serialize(v) for v in obj]
    try:
        json.dumps(obj)
        return obj
    except (TypeError, ValueError):
        return str(obj)
