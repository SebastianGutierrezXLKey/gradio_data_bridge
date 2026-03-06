"""PostgreSQL connection management using psycopg2."""

from __future__ import annotations

from typing import Any

import psycopg2
import psycopg2.extras
from loguru import logger


class DBConnector:
    """Wraps a psycopg2 connection with helpers for testing and querying.

    Automatically reconnects on dropped connections (SSL EOF, idle timeout, etc.).
    """

    def __init__(self) -> None:
        self._conn: psycopg2.extensions.connection | None = None
        self.schema: str = "public"
        self.db_name: str = ""
        # Stored to allow automatic reconnection
        self._host: str = ""
        self._port: int = 5432
        self._database: str = ""
        self._user: str = ""
        self._password: str = ""

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def connect(
        self,
        host: str,
        port: int | str,
        database: str,
        user: str,
        password: str,
        schema: str = "public",
    ) -> tuple[bool, str]:
        """Open a connection to the database.

        Returns:
            (success, message)
        """
        self.close()
        try:
            self._conn = psycopg2.connect(
                host=host,
                port=int(port),
                dbname=database,
                user=user,
                password=password,
                connect_timeout=10,
                options=f"-c search_path={schema}",
            )
            self._conn.autocommit = False
            self.schema = schema
            self.db_name = database
            # Store credentials for reconnection
            self._host = host
            self._port = int(port)
            self._database = database
            self._user = user
            self._password = password
            logger.info(f"Connected to {database} on {host}:{port} (schema={schema})")
            return True, f"Connexion réussie à '{database}' sur {host}:{port}"
        except psycopg2.OperationalError as exc:
            logger.error(f"Connection failed: {exc}")
            return False, f"Échec de connexion : {exc}"
        except Exception as exc:
            logger.error(f"Unexpected error during connect: {exc}")
            return False, f"Erreur inattendue : {exc}"

    def _reconnect(self) -> None:
        """Re-open the connection using stored credentials."""
        if not self._database:
            raise RuntimeError("Impossible de se reconnecter : aucun paramètre stocké.")
        logger.warning("Connexion perdue — reconnexion en cours...")
        try:
            if self._conn is not None:
                try:
                    self._conn.close()
                except Exception:
                    pass
            self._conn = psycopg2.connect(
                host=self._host,
                port=self._port,
                dbname=self._database,
                user=self._user,
                password=self._password,
                connect_timeout=10,
                options=f"-c search_path={self.schema}",
            )
            self._conn.autocommit = False
            logger.info("Reconnexion réussie.")
        except Exception as exc:
            self._conn = None
            raise RuntimeError(f"Reconnexion échouée : {exc}") from exc

    def test_connection(self) -> tuple[bool, str]:
        """Ping the server with a trivial query."""
        if self._conn is None:
            return False, "Aucune connexion établie."
        try:
            with self._conn.cursor() as cur:
                cur.execute("SELECT 1")
            return True, "Connexion active ✓"
        except Exception as exc:
            return False, f"Connexion perdue : {exc}"

    def close(self) -> None:
        """Close the underlying connection if open."""
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    # ------------------------------------------------------------------
    # Query helpers (with auto-reconnect on dropped connection)
    # ------------------------------------------------------------------

    @property
    def connection(self) -> psycopg2.extensions.connection:
        if self._conn is None:
            raise RuntimeError("No active database connection.")
        return self._conn

    def _ensure_connection(self) -> None:
        """Reconnect if the connection is closed or broken."""
        if self._conn is None or self._conn.closed != 0:
            self._reconnect()

    def execute_query(
        self, sql: str, params: tuple[Any, ...] | None = None
    ) -> list[dict[str, Any]]:
        """Execute a SELECT query and return rows as a list of dicts.

        Retries once on connection errors (SSL EOF, idle timeout, etc.).
        """
        for attempt in range(2):
            try:
                self._ensure_connection()
                with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(sql, params)
                    return [dict(row) for row in cur.fetchall()]
            except psycopg2.OperationalError as exc:
                if attempt == 0:
                    logger.warning(f"Query failed ({exc}), retrying after reconnect...")
                    self._reconnect()
                else:
                    raise

    def execute_write(
        self, sql: str, params: tuple[Any, ...] | None = None
    ) -> Any:
        """Execute a write statement and return the first column of the first row (if any).

        Retries once on connection errors.
        """
        for attempt in range(2):
            try:
                self._ensure_connection()
                with self._conn.cursor() as cur:
                    cur.execute(sql, params)
                    try:
                        row = cur.fetchone()
                        return row[0] if row else None
                    except psycopg2.ProgrammingError:
                        return None
            except psycopg2.OperationalError as exc:
                if attempt == 0:
                    logger.warning(f"Write failed ({exc}), retrying after reconnect...")
                    self._reconnect()
                else:
                    raise

    def commit(self) -> None:
        if self._conn:
            self._conn.commit()

    def rollback(self) -> None:
        if self._conn:
            try:
                self._conn.rollback()
            except Exception:
                pass

    def is_connected(self) -> bool:
        return self._conn is not None and self._conn.closed == 0
