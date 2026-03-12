"""HTTP API client with Bearer JWT authentication."""

from __future__ import annotations

from typing import Any

import requests
from loguru import logger
from requests import Response


class ApiClient:
    """REST client for the xlhub API.

    Supports:
    - Fixed token (stored in .env as API_TOKEN)
    - Login via email/password (POST /auth/login) to obtain token
    """

    def __init__(self) -> None:
        self._base_url: str = ""
        self._api_version: str = "/api/v1"
        self._token: str = ""
        self._session: requests.Session = requests.Session()

    # ------------------------------------------------------------------
    # Configuration
    # ------------------------------------------------------------------

    def configure(
        self,
        base_url: str,
        api_version: str = "/api/v1",
        token: str = "",
    ) -> None:
        """Set connection parameters (does NOT connect yet)."""
        self._base_url = base_url.rstrip("/")
        self._api_version = api_version
        self._token = token
        self._session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
        })
        if token:
            self._session.headers.update({"Authorization": f"Bearer {token}"})

    def login(self, login_endpoint: str, email: str, password: str) -> tuple[bool, str]:
        """Obtain a JWT token via login endpoint.

        Args:
            login_endpoint: Path relative to base_url, e.g. '/auth/login'
            email: User email.
            password: User password.

        Returns:
            (success, message)
        """
        url = f"{self._base_url}{login_endpoint}"
        try:
            resp = self._session.post(
                url,
                json={"email": email, "password": password},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            # Common patterns: data.access_token / data.token / data.data.access_token
            token = (
                data.get("access_token")
                or data.get("token")
                or (data.get("data") or {}).get("access_token")
                or (data.get("data") or {}).get("token")
            )
            if not token:
                return False, f"Token introuvable dans la réponse : {list(data.keys())}"
            self._token = token
            self._session.headers.update({"Authorization": f"Bearer {token}"})
            logger.info(f"Logged in to {self._base_url}")
            return True, "Authentification réussie ✓"
        except requests.HTTPError as exc:
            msg = f"HTTP {exc.response.status_code}: {exc.response.text[:200]}"
            logger.error(f"Login failed: {msg}")
            return False, msg
        except Exception as exc:
            logger.error(f"Login error: {exc}")
            return False, str(exc)

    def login_service_account(self, client_id: str, client_secret: str) -> tuple[bool, str]:
        """Obtain a Bearer token via the service-account endpoint.

        POST /api/v1/service-accounts/token  {client_id, client_secret}

        Returns:
            (success, message)
        """
        url = f"{self._base_url}{self._api_version}/service-accounts/token"
        try:
            resp = requests.post(
                url,
                json={"client_id": client_id, "client_secret": client_secret},
                headers={"Content-Type": "application/json", "Accept": "application/json"},
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            token = (
                data.get("access_token")
                or data.get("token")
                or (data.get("data") or {}).get("access_token")
                or (data.get("data") or {}).get("token")
            )
            if not token:
                return False, f"Token introuvable dans la réponse : {list(data.keys())}"
            self._token = token
            self._session.headers.update({"Authorization": f"Bearer {token}"})
            logger.info(f"Service account authenticated to {self._base_url}")
            return True, "Compte de service authentifié ✓"
        except requests.HTTPError as exc:
            msg = f"HTTP {exc.response.status_code}: {exc.response.text[:200]}"
            logger.error(f"Service account login failed: {msg}")
            return False, msg
        except Exception as exc:
            logger.error(f"Service account login error: {exc}")
            return False, str(exc)

    def test_connection(self) -> tuple[bool, str]:
        """Verify the token works by calling a lightweight endpoint."""
        if not self._token:
            return False, "Aucun token configuré."
        try:
            url = f"{self._base_url}{self._api_version}/soil-sampling/laboratories"
            resp = self._session.get(url, timeout=5)
            if resp.status_code in (200, 404):
                return True, f"API accessible ✓ (token valide)"
            if resp.status_code == 401:
                return False, "Token invalide ou expiré (401)"
            return False, f"Statut inattendu : {resp.status_code}"
        except Exception as exc:
            return False, f"Erreur réseau : {exc}"

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def post(self, path: str, data: dict[str, Any]) -> dict[str, Any]:
        """POST JSON *data* to *path* and return the response body.

        Raises:
            requests.HTTPError on 4xx/5xx.
            RuntimeError if not configured.
        """
        if not self._base_url:
            raise RuntimeError("ApiClient not configured. Call configure() first.")
        url = f"{self._base_url}{self._api_version}{path}"
        logger.debug(f"POST {url} payload keys={list(data.keys())}")
        resp = self._session.post(url, json=data, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def get(self, path: str, params: dict | None = None) -> dict[str, Any]:
        """GET *path* with optional query *params*."""
        url = f"{self._base_url}{self._api_version}{path}"
        resp = self._session.get(url, params=params, timeout=15)
        resp.raise_for_status()
        return resp.json()

    def is_configured(self) -> bool:
        return bool(self._base_url and self._token)

    @property
    def session(self) -> requests.Session:
        return self._session

    @property
    def base_url(self) -> str:
        return self._base_url

    @property
    def api_version(self) -> str:
        return self._api_version
