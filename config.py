"""Application configuration — loaded from environment / .env file."""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")


class DBDefaults:
    """Default connection values read from environment variables."""

    SOURCE_HOST: str = os.getenv("SOURCE_HOST", "localhost")
    SOURCE_PORT: str = os.getenv("SOURCE_PORT", "5432")
    SOURCE_DB: str = os.getenv("SOURCE_DB", "")
    SOURCE_USER: str = os.getenv("SOURCE_USER", "postgres")
    SOURCE_PASSWORD: str = os.getenv("SOURCE_PASSWORD", "")
    SOURCE_SCHEMA: str = os.getenv("SOURCE_SCHEMA", "public")

    TARGET_HOST: str = os.getenv("TARGET_HOST", "localhost")
    TARGET_PORT: str = os.getenv("TARGET_PORT", "5432")
    TARGET_DB: str = os.getenv("TARGET_DB", "")
    TARGET_USER: str = os.getenv("TARGET_USER", "postgres")
    TARGET_PASSWORD: str = os.getenv("TARGET_PASSWORD", "")
    TARGET_SCHEMA: str = os.getenv("TARGET_SCHEMA", "public")


class ApiDefaults:
    """Default API connection values read from environment variables."""

    BASE_URL: str = os.getenv("API_BASE_URL", "http://localhost:8000")
    VERSION: str = os.getenv("API_VERSION", "/api/v1")
    TOKEN: str = os.getenv("API_TOKEN", "")
    LOGIN_ENDPOINT: str = os.getenv("API_LOGIN_ENDPOINT", "/auth/login")
    LOGIN_EMAIL: str = os.getenv("API_LOGIN_EMAIL", "")
    LOGIN_PASSWORD: str = os.getenv("API_LOGIN_PASSWORD", "")


AUDIT_OUTPUT_DIR: Path = Path(os.getenv("AUDIT_OUTPUT_DIR", "/tmp/crossmigrate_audits"))
AUDIT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_PREVIEW_ROWS: int = 20
DEFAULT_BATCH_SIZE: int = 100
