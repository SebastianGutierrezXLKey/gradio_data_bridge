#!/usr/bin/env python3
"""
Script to migrate sampling zones from the source database to the xlhub API.

Creates sampling units (type: zone) in the xlhub API from xlkey.sampling_zone_2,
filtered optionally by account name.

Usage:
    # Migrate all zones
    python audit/scripts/test_sample_units_migration.py

    # Filter by account name
    python audit/scripts/test_sample_units_migration.py --value "9206"

    # Dry run (no API writes)
    python audit/scripts/test_sample_units_migration.py --value "9206" --dry-run

    # Delete previously created units (downgrade)
    python audit/scripts/test_sample_units_migration.py --downgrade

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

# Load .env from project root (two levels up from this script)
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
API_LOGIN_ENDPOINT = os.getenv("API_LOGIN_ENDPOINT", "/auth/login")
API_LOGIN_EMAIL = os.getenv("API_LOGIN_EMAIL", "")
API_LOGIN_PASSWORD = os.getenv("API_LOGIN_PASSWORD", "")

UNITS_ENDPOINT = "/soil-sampling/units"
SOURCE_TABLE = "xlkey.sampling_zone_2"
ACCOUNTS_TABLE = "xlkey.accounts"

# Source table columns (excluding raw geometry which is handled via ST_AsGeoJSON)
ZONE_COLUMNS = [
    "id", '"FARM_ID"', '"FIELD_ID"', "site_id", '"FIELD_NAME"',
    '"SOURCE"', "zone_name", "zone_name_2", "year_key",
    '"S3_UPLOAD_DATE"', "area_acre",
]
# Columns to exclude from sample_unit_metadata (already used as name or geometry)
METADATA_EXCLUDE_COLS = {"zone_name_2", "geometry"}


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

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
    """Convert non-JSON-serializable values to a JSON-safe type."""
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    return str(value)


def build_unit_payload(row: dict) -> dict:
    """Convert a source DB row into a POST /soil-sampling/units payload."""
    geometry = row.get("geometry")
    if isinstance(geometry, str):
        try:
            geometry = json.loads(geometry)
        except (json.JSONDecodeError, TypeError):
            geometry = None
    name = row.get("zone_name_2")

    metadata: dict[str, Any] = {}
    for k, v in row.items():
        if k not in METADATA_EXCLUDE_COLS:
            metadata[k] = make_serializable(v)

    return {
        "unit_type": "zone",
        "name": str(name) if name is not None else None,
        "geometry": geometry,
        "sample_unit_metadata": metadata,
    }


# ---------------------------------------------------------------------------
# Source DB queries
# ---------------------------------------------------------------------------

async def count_zones(
    conn: asyncpg.Connection, col_name: str, value: str | None
) -> tuple[int, int]:
    """Return (filtered_count, total_count)."""
    total = await conn.fetchval(f'SELECT COUNT(*) FROM {SOURCE_TABLE}')

    if value:
        filtered = await conn.fetchval(
            f"""
            WITH account_list AS (
                SELECT id FROM {ACCOUNTS_TABLE}
                WHERE {col_name} ILIKE $1
                LIMIT 200
            )
            SELECT COUNT(*) FROM {SOURCE_TABLE}
            WHERE "FARM_ID" IN (SELECT id FROM account_list)
            """,
            f"%{value}%",
        )
    else:
        filtered = total

    return int(filtered), int(total)


async def fetch_zones(
    conn: asyncpg.Connection, col_name: str, value: str | None
) -> list[dict]:
    """Fetch sampling zones, converting geometry to GeoJSON."""
    col_list = ", ".join(ZONE_COLUMNS)
    select_clause = (
        f'{col_list}, ST_AsGeoJSON("geometry")::json AS geometry'
    )

    if value:
        query = f"""
            WITH account_list AS (
                SELECT id
                FROM {ACCOUNTS_TABLE}
                WHERE {col_name} ILIKE $1
                ORDER BY id ASC
                LIMIT 200
            )
            SELECT {select_clause}
            FROM {SOURCE_TABLE}
            WHERE "FARM_ID" IN (SELECT id FROM account_list)
        """
        rows = await conn.fetch(query, f"%{value}%")
    else:
        query = f'SELECT {select_clause} FROM {SOURCE_TABLE}'
        rows = await conn.fetch(query)

    return [dict(row) for row in rows]


# ---------------------------------------------------------------------------
# Upgrade (migrate zones to API)
# ---------------------------------------------------------------------------

async def upgrade(
    conn: asyncpg.Connection,
    session: requests.Session,
    col_name: str,
    value: str | None,
    mapping_file: Path,
    dry_run: bool,
) -> None:
    print_step("UPGRADE - Querying Source Database")

    filtered, total = await count_zones(conn, col_name, value)
    print_info(f"Total records in {SOURCE_TABLE}: {total}")
    if value:
        print_info(f"Records matching {col_name} ILIKE '%{value}%': {filtered}")
    print_info(f"Records to migrate: {filtered}")

    zones = await fetch_zones(conn, col_name, value)
    print_success(f"Fetched {len(zones)} zones from source database")

    print_step(f"UPGRADE - {'Simulating' if dry_run else 'Sending to Target API'}")
    if dry_run:
        print_warning("DRY RUN — no API calls will be made")

    mapping: list[dict] = []
    succeeded = 0
    failed = 0

    for i, row in enumerate(zones, 1):
        source_id = str(row.get("id", i))
        payload = build_unit_payload(row)

        if dry_run:
            print_info(
                f"[{i}/{len(zones)}] Would POST source_id={source_id} "
                f"name={payload.get('name')!r} "
                f"geometry_type={payload['geometry'].get('type') if isinstance(payload.get('geometry'), dict) else ('present' if payload.get('geometry') else 'NULL')}"
            )
            succeeded += 1
            continue

        try:
            url = f"{API_BASE_URL}{API_VERSION}{UNITS_ENDPOINT}"
            resp = session.post(url, json=payload, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            target_id = str((data.get("data") or data).get("id", ""))

            mapping.append({
                "source_id": source_id,
                "target_api_id": target_id,
                "FIELD_NAME": str(row.get("FIELD_NAME", "")),
                "zone_name_2": str(row.get("zone_name_2", "")),
            })
            succeeded += 1

            if i % 50 == 0 or i == len(zones):
                print_success(f"[{i}/{len(zones)}] {succeeded} ok, {failed} errors")

        except Exception as exc:
            failed += 1
            print_warning(f"[{i}/{len(zones)}] source_id={source_id} failed: {exc}")

    if not dry_run and mapping:
        mapping_file.parent.mkdir(parents=True, exist_ok=True)
        with open(mapping_file, "w", encoding="utf-8") as f:
            json.dump(mapping, f, indent=2, ensure_ascii=False)
        print_success(f"Mapping file saved: {mapping_file}")

    print_step("UPGRADE - Summary")
    print_success(f"Source records: {filtered} | Sent: {succeeded} | Failed: {failed}")
    if dry_run:
        print_warning("Dry run — no records were written to the API")


# ---------------------------------------------------------------------------
# Downgrade (delete created units from API)
# ---------------------------------------------------------------------------

async def downgrade(
    session: requests.Session,
    mapping_file: Path,
) -> None:
    print_step("DOWNGRADE - Loading Mapping File")

    if mapping_file is None or not mapping_file.exists():
        output_dir = Path(__file__).parent / "output"
        candidates = sorted(output_dir.glob("sample_units_mapping_*.json"), reverse=True)
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
            print_warning(f"[{i}] No target_api_id in entry — skipping")
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
            print_warning(
                f"[{i}] target_id={target_id} "
                f"(source_id={entry.get('source_id')}) failed: {exc}"
            )

    print_step("DOWNGRADE - Summary")
    print_success(f"Total entries: {len(mapping)} | Deleted: {deleted} | Failed: {failed}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main() -> None:
    print(f"{Colors.OKBLUE}🚀 XLHub Sample Units Migration Script{Colors.ENDC}")
    print(f"{Colors.OKBLUE}   Migrates xlkey.sampling_zone_2 → xlhub /soil-sampling/units{Colors.ENDC}\n")

    parser = argparse.ArgumentParser(
        description="Migrate sampling zones to xlhub /soil-sampling/units endpoint",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Migrate all zones
  python audit/scripts/test_sample_units_migration.py

  # Migrate zones for accounts matching '9206'
  python audit/scripts/test_sample_units_migration.py --value "9206"

  # Dry run (no API writes)
  python audit/scripts/test_sample_units_migration.py --value "9206" --dry-run

  # Delete previously created units
  python audit/scripts/test_sample_units_migration.py --downgrade
        """,
    )
    parser.add_argument(
        "--col-name",
        default="name_en",
        help="Column in xlkey.accounts to filter on (default: name_en)",
    )
    parser.add_argument(
        "--value",
        default=None,
        help="Value to search with ILIKE in --col-name. If omitted, all zones are migrated.",
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
    print(f"   Mode: {mode}")
    if not args.downgrade:
        if args.value:
            print(f"   Filter: {args.col_name} ILIKE '%{args.value}%'")
        else:
            print("   Filter: none (all zones)")
    # Resolve mapping file path (auto-generate timestamped name for upgrade)
    if args.mapping_file is None and not args.downgrade and not args.dry_run:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        args.mapping_file = Path(__file__).parent / "output" / f"sample_units_mapping_{ts}.json"

    print(f"   Mapping file: {args.mapping_file or '(auto — latest in output/)'}")
    print(f"   API: {API_BASE_URL}{API_VERSION}{UNITS_ENDPOINT}")

    # Resolve credentials
    email = args.email or API_LOGIN_EMAIL
    password = args.password or API_LOGIN_PASSWORD

    if not email:
        print_error("API email is required. Use --email or set API_LOGIN_EMAIL in .env")
    if not password:
        print_error("API password is required. Use --password or set API_LOGIN_PASSWORD in .env")

    # Authenticate with the API
    print_step("API - Authenticating")
    try:
        session = api_login(email, password)
        print_success(f"Authenticated to {API_BASE_URL}")
    except Exception as exc:
        print_error(f"API authentication failed: {exc}")

    # Downgrade does not need the source DB
    if args.downgrade:
        await downgrade(session, args.mapping_file)
        print_step("COMPLETE")
        print_success("Downgrade completed successfully!")
        return

    # Connect to source database
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
        await upgrade(conn, session, args.col_name, args.value, args.mapping_file, args.dry_run)
        print_step("COMPLETE")
        print_success("Migration completed successfully!")
    except Exception as exc:
        print_error(f"An unexpected error occurred: {exc}")
    finally:
        await conn.close()
        print_step("DATABASE - Connection closed")


if __name__ == "__main__":
    asyncio.run(main())
