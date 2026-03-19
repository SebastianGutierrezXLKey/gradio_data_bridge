#!/usr/bin/env python3
"""
Script to migrate point sampling units from xlkey.temp_points_analyse to the xlhub API.

Similar to test_sample_units_migration.py but for geometry type "point".
Source rows are fetched via a CTE joining xlkey.accounts → xlkey.fields → xlkey.temp_points_analyse.

The `name` field is derived from samp_name with two transformations:
  1. Strip a configurable prefix (default: "ROY") from the start of the value
  2. Invert [nom]_[id_point] → [id_point]_[nom]
  e.g. "ROYFR02_1" → strip ROY → "FR02_1" → invert → "1_FR02"
  e.g. "05_01"     → no prefix  → "05_01"  → invert → "01_05"

Usage:
    # Migrate points for account matching '9206' (dry run)
    python audit/scripts/test_sample_units_points_migration.py --value "9206" --dry-run

    # Real run
    python audit/scripts/test_sample_units_points_migration.py --value "9206"

    # Custom prefix to strip
    python audit/scripts/test_sample_units_points_migration.py --value "9206" --prefix "ABC"

    # No prefix stripping
    python audit/scripts/test_sample_units_points_migration.py --value "9206" --prefix ""

    # Rollback
    python audit/scripts/test_sample_units_points_migration.py --downgrade

Pre-requisites:
    - Source PostgreSQL database accessible via SOURCE_* env vars
    - xlhub API accessible via API_* env vars
    - asyncpg, requests, python-dotenv installed
"""
import asyncio
import argparse
import json
import os
import sys
import logging
from datetime import datetime, date
from pathlib import Path
from typing import Any

import asyncpg
import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent.parent / ".env")


# --- ANSI Color Codes ---
class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def print_step(msg: str):
    print(f"\n{Colors.HEADER}=== {msg} ==={Colors.ENDC}")


def print_success(msg: str):
    print(f"{Colors.OKGREEN}✅ {msg}{Colors.ENDC}")


def print_error(msg: str):
    print(f"{Colors.FAIL}❌ {msg}{Colors.ENDC}")
    sys.exit(1)


def print_warning(msg: str):
    print(f"{Colors.WARNING}⚠️  {msg}{Colors.ENDC}")


def print_info(msg: str):
    print(f"{Colors.OKBLUE}ℹ️  {msg}{Colors.ENDC}")


# --- Source DB Configuration ---
SOURCE_HOST = os.getenv("SOURCE_HOST", "localhost")
SOURCE_PORT = int(os.getenv("SOURCE_PORT", "5432"))
SOURCE_DB = os.getenv("SOURCE_DB", "")
SOURCE_USER = os.getenv("SOURCE_USER", "postgres")
SOURCE_PASSWORD = os.getenv("SOURCE_PASSWORD", "")

# --- API Configuration ---
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000").rstrip("/")
API_VERSION = os.getenv("API_VERSION", "/api/v1")
API_TOKEN = os.getenv("API_TOKEN", "")
API_LOGIN_ENDPOINT = os.getenv("API_LOGIN_ENDPOINT", "/auth/login")
API_LOGIN_EMAIL = os.getenv("API_LOGIN_EMAIL", "")
API_LOGIN_PASSWORD = os.getenv("API_LOGIN_PASSWORD", "")

# --- Service Account Credentials ---
API_CLIENT_ID = os.getenv("API_CLIENT_ID", "")
API_CLIENT_SECRET = os.getenv("API_CLIENT_SECRET", "")

UNITS_ENDPOINT = "/soil-sampling/units"
SOURCE_TABLE = "xlkey.temp_points_analyse"
ACCOUNTS_TABLE = "xlkey.accounts"
FIELDS_TABLE = "xlkey.fields"

# Columns to exclude from sample_unit_metadata (geometry handled separately, name derived)
METADATA_EXCLUDE_COLS = {"geom", "samp_name"}

DEFAULT_PREFIX = "ROY"


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

def api_session_from_token(token: str) -> requests.Session:
    """Build an authenticated session from a pre-existing Bearer token."""
    session = requests.Session()
    session.headers.update({
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
    })
    return session


def api_session_from_client_credentials(client_id: str, client_secret: str) -> requests.Session:
    """Obtain a Bearer token via the service account endpoint and return an authenticated session."""
    url = f"{API_BASE_URL}/api/v1/service-accounts/token"
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
        raise ValueError(f"access_token not found in service account response. Keys: {list(data.keys())}")
    print_info("Service account token obtained")
    return api_session_from_token(token)


def api_login(email: str, password: str) -> requests.Session:
    """Authenticate with the xlhub API and return an authenticated session."""
    session = requests.Session()
    session.headers.update({
        "Content-Type": "application/json",
        "Accept": "application/json",
    })
    url = f"{API_BASE_URL}{API_VERSION}{API_LOGIN_ENDPOINT}"
    resp = session.post(url, json={"email": email, "password": password}, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    token = (
        data.get("access_token")
        or data.get("token")
        or (data.get("data") or {}).get("access_token")
        or (data.get("data") or {}).get("token")
    )
    if not token:
        print_error(f"Token not found in login response. Keys: {list(data.keys())}")
    session.headers.update({"Authorization": f"Bearer {token}"})
    return session


# ---------------------------------------------------------------------------
# Serialization helpers
# ---------------------------------------------------------------------------

def make_serializable(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    return str(value)


# ---------------------------------------------------------------------------
# Name transformation
# ---------------------------------------------------------------------------

def transform_samp_name(samp_name: str, prefix: str) -> str:
    """
    Transform a samp_name value into the canonical point unit name.

    Steps:
      1. Strip the given prefix (case-sensitive) from the start of the value.
      2. Split on the first '_' and swap the two parts: [nom]_[id] → [id]_[nom].

    Examples (prefix="ROY"):
      "ROYFR02_1"  → strip → "FR02_1"  → invert → "1_FR02"
      "05_01"      → no strip           → invert → "01_05"
      "ROYFR02"    → strip → "FR02"     → no underscore → "FR02"
    """
    value = samp_name.strip()
    if prefix and value.startswith(prefix):
        value = value[len(prefix):]
    if "_" in value:
        parts = value.split("_", 1)
        value = f"{parts[1]}_{parts[0]}"
    return value


# ---------------------------------------------------------------------------
# Payload builder
# ---------------------------------------------------------------------------

def build_unit_payload(row: dict, prefix: str) -> dict:
    """Convert a source DB row into a POST /soil-sampling/units payload."""
    geometry = row.get("geometry")
    if isinstance(geometry, str):
        try:
            geometry = json.loads(geometry)
        except (json.JSONDecodeError, TypeError):
            geometry = None

    raw_name = str(row.get("samp_name") or "")
    name = transform_samp_name(raw_name, prefix)

    metadata: dict[str, Any] = {}
    for k, v in row.items():
        if k not in METADATA_EXCLUDE_COLS:
            metadata[k] = make_serializable(v)
    metadata["sampling_name"] = raw_name

    return {
        "unit_type": "point",
        "name": name,
        "geometry": geometry,
        "parent_sampling_unit_id": None,
        "sample_unit_metadata": metadata,
    }


# ---------------------------------------------------------------------------
# Source DB queries
# ---------------------------------------------------------------------------

async def fetch_points(
    conn: asyncpg.Connection,
    col_name: str,
    value: str | None,
) -> list[dict]:
    """Fetch sampling points via CTE: accounts → fields → temp_points_analyse."""
    select_clause = 'tp.*, ST_AsGeoJSON(tp."geom")::json AS geometry'

    if value:
        query = f"""
            WITH account_list AS (
                SELECT id FROM {ACCOUNTS_TABLE}
                WHERE {col_name} = $1
                ORDER BY id ASC
            ),
            fields_list AS (
                SELECT id FROM {FIELDS_TABLE}
                WHERE account_id IN (SELECT id FROM account_list)
                  AND lower(status) = 'active'
                  AND deleted_at IS NULL
                ORDER BY id ASC
            )
            SELECT {select_clause}
            FROM {SOURCE_TABLE} tp
            WHERE tp.field_id IN (SELECT id FROM fields_list)
        """
        param = int(value) if str(value).isdigit() else value
        rows = await conn.fetch(query, param)
    else:
        query = f'SELECT {select_clause} FROM {SOURCE_TABLE} tp'
        rows = await conn.fetch(query)

    return [dict(row) for row in rows]


# ---------------------------------------------------------------------------
# Upgrade
# ---------------------------------------------------------------------------

async def upgrade(
    conn: asyncpg.Connection,
    session: requests.Session,
    col_name: str,
    value: str | None,
    prefix: str,
    mapping_file: Path,
    dry_run: bool,
) -> None:
    print_step("UPGRADE - Querying Source Database")
    points = await fetch_points(conn, col_name, value)
    print_success(f"Fetched {len(points)} points from {SOURCE_TABLE}")

    # Pre-fetch existing units to avoid duplicates
    print_step("UPGRADE - Checking Existing Units in Target API")
    existing_keys: set[str] = set()
    if not dry_run:
        try:
            url = f"{API_BASE_URL}{API_VERSION}{UNITS_ENDPOINT}"
            resp = session.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            existing_units = data.get("data") or []
            if not isinstance(existing_units, list):
                existing_units = []
            for unit in existing_units:
                if unit.get("unit_type") == "point" and unit.get("name"):
                    existing_keys.add(str(unit["name"]))
            print_info(f"Found {len(existing_units)} existing units ({len(existing_keys)} points by name)")
        except Exception as exc:
            print_warning(f"Could not fetch existing units (proceeding without duplicate check): {exc}")
    else:
        print_warning("DRY RUN — skipping existing-units check")

    print_step(f"UPGRADE - {'Simulating' if dry_run else 'Sending to Target API'}")
    if dry_run:
        print_warning("DRY RUN — no API calls will be made")

    mapping: list[dict] = []
    succeeded = 0
    skipped = 0
    failed = 0

    for i, row in enumerate(points, 1):
        source_id = str(row.get("id", i))
        raw_name = str(row.get("samp_name") or "")
        payload = build_unit_payload(row, prefix)
        name = payload["name"]

        if dry_run:
            geom = payload.get("geometry")
            geom_type = geom.get("type") if isinstance(geom, dict) else ("present" if geom else "NULL")
            print_info(
                f"[{i}/{len(points)}] Would POST source_id={source_id} "
                f"raw_name={raw_name!r} → name={name!r} geometry_type={geom_type}"
            )
            succeeded += 1
            continue

        if name in existing_keys:
            print_warning(f"[{i}/{len(points)}] SKIP — already exists: name={name!r}")
            skipped += 1
            continue

        try:
            url = f"{API_BASE_URL}{API_VERSION}{UNITS_ENDPOINT}"
            resp = session.post(url, json=payload, timeout=30)
            if not resp.ok:
                raise RuntimeError(f"{resp.status_code}: {resp.text}")
            data = resp.json()
            target_id = str((data.get("data") or data).get("id", ""))

            mapping.append({
                "source_id": source_id,
                "target_api_id": target_id,
                "samp_name_raw": raw_name,
                "name": name,
                "unit_type": "point",
                "source_table": SOURCE_TABLE,
            })
            succeeded += 1

            if i % 50 == 0 or i == len(points):
                print_success(f"[{i}/{len(points)}] {succeeded} ok, {skipped} skipped, {failed} errors")

        except Exception as exc:
            failed += 1
            print_warning(f"[{i}/{len(points)}] source_id={source_id} failed: {exc}")

    if not dry_run and mapping:
        mapping_file.parent.mkdir(parents=True, exist_ok=True)
        with open(mapping_file, "w", encoding="utf-8") as f:
            json.dump(mapping, f, indent=2, ensure_ascii=False)
        print_success(f"Mapping file saved: {mapping_file}")

    print_step("UPGRADE - Summary")
    print_success(
        f"Fetched: {len(points)} | Sent: {succeeded} | "
        f"Skipped (already exist): {skipped} | Failed: {failed}"
    )
    if dry_run:
        print_warning("Dry run — no records were written to the API")


# ---------------------------------------------------------------------------
# Downgrade
# ---------------------------------------------------------------------------

async def downgrade(session: requests.Session, mapping_file: Path) -> None:
    print_step("DOWNGRADE - Loading Mapping File")

    if mapping_file is None or not mapping_file.exists():
        output_dir = Path(__file__).parent / "output"
        candidates = sorted(output_dir.glob("sample_units_points_mapping_*.json"), reverse=True)
        if not candidates:
            print_error(f"No mapping file found in {output_dir}. Run upgrade first.")
        mapping_file = candidates[0]
        print_info(f"Using latest mapping file: {mapping_file.name}")

    with open(mapping_file, encoding="utf-8") as f:
        mapping: list[dict] = json.load(f)
    print_success(f"Loaded {len(mapping)} entries from {mapping_file}")

    print_step("DOWNGRADE - Deleting Units from API")
    deleted = 0
    failed = 0

    for i, entry in enumerate(mapping, 1):
        target_id = entry.get("target_api_id")
        if not target_id:
            print_warning(f"[{i}] No target_api_id — skipping")
            failed += 1
            continue

        try:
            url = f"{API_BASE_URL}{API_VERSION}{UNITS_ENDPOINT}/{target_id}"
            resp = session.delete(url, timeout=15)
            resp.raise_for_status()
            deleted += 1

            if i % 50 == 0 or i == len(mapping):
                print_success(f"[{i}/{len(mapping)}] {deleted} deleted, {failed} errors")

        except Exception as exc:
            failed += 1
            print_warning(f"[{i}] target_id={target_id} (source_id={entry.get('source_id')}) failed: {exc}")

    print_step("DOWNGRADE - Summary")
    print_success(f"Total: {len(mapping)} | Deleted: {deleted} | Failed: {failed}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main() -> None:
    print(f"{Colors.OKBLUE}🚀 XLHub Sample Units (Points) Migration Script{Colors.ENDC}")
    print(f"{Colors.OKBLUE}   Migrates xlkey.temp_points_analyse → xlhub /soil-sampling/units (type: point){Colors.ENDC}\n")

    parser = argparse.ArgumentParser(
        description="Migrate point sampling units to xlhub /soil-sampling/units endpoint",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python audit/scripts/test_sample_units_points_migration.py --value "681" --dry-run
  python audit/scripts/test_sample_units_points_migration.py --value "681"
  python audit/scripts/test_sample_units_points_migration.py --value "681" --prefix "ABC"
  python audit/scripts/test_sample_units_points_migration.py --downgrade
        """,
    )
    parser.add_argument(
        "--col-name",
        default="id",
        help="Column in xlkey.accounts to filter on (default: id)",
    )
    parser.add_argument(
        "--value",
        default=None,
        help="Value to search in --col-name. If omitted, all points are migrated.",
    )
    parser.add_argument(
        "--prefix",
        default=DEFAULT_PREFIX,
        help=f"Prefix to strip from samp_name before inverting (default: '{DEFAULT_PREFIX}'). Pass empty string to disable.",
    )
    parser.add_argument(
        "--token",
        default=None,
        help="Bearer token (default: API_TOKEN from .env). Takes priority over other auth methods.",
    )
    parser.add_argument(
        "--email",
        default=None,
        help="API login email (default: API_LOGIN_EMAIL from .env)",
    )
    parser.add_argument(
        "--password",
        default=None,
        help="API login password (default: API_LOGIN_PASSWORD from .env)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate migration without posting to the API",
    )
    parser.add_argument(
        "--downgrade",
        action="store_true",
        help="Delete previously created units using the mapping file",
    )
    parser.add_argument(
        "--mapping-file",
        type=Path,
        default=None,
        help="Path to the mapping JSON file. Upgrade: auto-generated with timestamp in output/. Downgrade: defaults to latest file in output/.",
    )

    args = parser.parse_args()

    print_step("SETUP - Configuration")
    mode = "DOWNGRADE" if args.downgrade else ("DRY RUN" if args.dry_run else "UPGRADE")
    print(f"   Mode    : {mode}")
    if not args.downgrade:
        print(f"   Filter  : {args.col_name} = '{args.value}    '" if args.value else "   Filter  : none (all points)")
        print(f"   Prefix  : {args.prefix!r} (to strip from samp_name)")

    # Resolve mapping file
    output_dir = Path(__file__).parent / "output"
    if args.mapping_file is None and not args.downgrade and not args.dry_run:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        args.mapping_file = output_dir / f"sample_units_points_mapping_{ts}.json"

    print(f"   Mapping : {args.mapping_file or '(auto — latest in output/)'}")
    print(f"   API     : {API_BASE_URL}{API_VERSION}{UNITS_ENDPOINT}")

    # Authenticate
    token = args.token or API_TOKEN
    email = args.email or API_LOGIN_EMAIL
    password = args.password or API_LOGIN_PASSWORD

    print_step("API - Authenticating")
    try:
        if token:
            session = api_session_from_token(token)
            print_success(f"Using fixed Bearer token for {API_BASE_URL}")
        elif API_CLIENT_ID and API_CLIENT_SECRET:
            print_info("Fetching service account token...")
            session = api_session_from_client_credentials(API_CLIENT_ID, API_CLIENT_SECRET)
            print_success(f"Authenticated via service account for {API_BASE_URL}")
        else:
            if not email:
                print_error("No auth method configured. Set API_TOKEN, API_CLIENT_ID/SECRET, or API_LOGIN_EMAIL in .env")
            if not password:
                print_error("API password is required. Use --password or set API_LOGIN_PASSWORD in .env")
            session = api_login(email, password)
            print_success(f"Authenticated to {API_BASE_URL}")
    except Exception as exc:
        print_error(f"API authentication failed: {exc}")

    if args.downgrade:
        await downgrade(session, args.mapping_file)
        print_step("COMPLETE")
        print_success("Downgrade completed successfully!")
        return

    if not SOURCE_DB:
        print_error("SOURCE_DB is required. Set it in .env")

    print_step("DATABASE - Connecting to Source PostgreSQL")
    try:
        conn = await asyncpg.connect(
            user=SOURCE_USER,
            password=SOURCE_PASSWORD,
            host=SOURCE_HOST,
            port=SOURCE_PORT,
            database=SOURCE_DB,
        )
        print_success(f"Connected to {SOURCE_HOST}:{SOURCE_PORT}/{SOURCE_DB}")
    except Exception as exc:
        print_error(f"Failed to connect to source database: {exc}")

    try:
        await upgrade(
            conn=conn,
            session=session,
            col_name=args.col_name,
            value=args.value,
            prefix=args.prefix,
            mapping_file=args.mapping_file,
            dry_run=args.dry_run,
        )
        print_step("COMPLETE")
        print_success("Migration completed successfully!")
    except Exception as exc:
        print_error(f"An unexpected error occurred: {exc}")
    finally:
        await conn.close()
        print_step("DATABASE - Connection closed")


if __name__ == "__main__":
    asyncio.run(main())
