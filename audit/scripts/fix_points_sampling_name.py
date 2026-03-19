#!/usr/bin/env python3
"""
Script to backfill sampling_name in sample_unit_metadata for existing point units.

Point units were migrated without sampling_name in their metadata (samp_name was excluded).
This script:
  1. Fetches all point sampling units from the API
  2. For each unit missing sampling_name in metadata, retrieves samp_name from the
     source DB (xlkey.temp_points_analyse) using the id stored in metadata
  3. PATCHes the unit to add sampling_name to sample_unit_metadata

Usage:
    # Dry run
    python audit/scripts/fix_points_sampling_name.py --dry-run

    # Real run
    python audit/scripts/fix_points_sampling_name.py

    # Filter by account id in source DB
    python audit/scripts/fix_points_sampling_name.py --value "681"
"""
import asyncio
import argparse
import json
import os
import sys
from datetime import datetime
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
API_CLIENT_ID = os.getenv("API_CLIENT_ID", "")
API_CLIENT_SECRET = os.getenv("API_CLIENT_SECRET", "")

SOURCE_TABLE = "xlkey.temp_points_analyse"
ACCOUNTS_TABLE = "xlkey.accounts"
FIELDS_TABLE = "xlkey.fields"
UNITS_ENDPOINT = "/soil-sampling/units"

OUTPUT_DIR = Path(__file__).parent / "output"


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

def api_session_from_token(token: str) -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
    })
    return session


def api_session_from_client_credentials(client_id: str, client_secret: str) -> requests.Session:
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
        raise ValueError("access_token not found in service account response.")
    print_info("Service account token obtained")
    return api_session_from_token(token)


def api_login(email: str, password: str) -> requests.Session:
    session = requests.Session()
    session.headers.update({"Content-Type": "application/json", "Accept": "application/json"})
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
        print_error("Token not found in login response.")
    session.headers.update({"Authorization": f"Bearer {token}"})
    return session


# ---------------------------------------------------------------------------
# Paginated fetch helper
# ---------------------------------------------------------------------------

def fetch_all_pages(session: requests.Session, url: str, params: dict | None = None) -> list[dict]:
    items: list[dict] = []
    page = 1
    while True:
        p = dict(params or {})
        p["page"] = page
        p["size"] = 100
        resp = session.get(url, params=p, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        raw = data.get("data") or []
        # Handle both paginated {"items": [...], "total": N} and flat list responses
        if isinstance(raw, dict):
            batch = raw.get("items") or []
            total = raw.get("total", 0)
        else:
            batch = raw if isinstance(raw, list) else []
            total = len(batch)  # no pagination info → treat as complete
        items.extend(batch)
        if len(items) >= total or not batch:
            break
        page += 1
    return items


# ---------------------------------------------------------------------------
# Source DB: fetch samp_name by id
# ---------------------------------------------------------------------------

async def fetch_samp_names(
    conn: asyncpg.Connection,
    ids: list[int],
    col_name: str | None = None,
    value: str | None = None,
) -> dict[int, str]:
    """Return {source_id: samp_name} for the given list of ids."""
    if not ids:
        return {}

    if col_name and value:
        # Filter by account
        param = int(value) if str(value).isdigit() else value
        rows = await conn.fetch(
            f"""
            WITH account_list AS (
                SELECT id FROM {ACCOUNTS_TABLE} WHERE {col_name} = $1
            ),
            fields_list AS (
                SELECT id FROM {FIELDS_TABLE}
                WHERE account_id IN (SELECT id FROM account_list)
                  AND lower(status) = 'active' AND deleted_at IS NULL
            )
            SELECT tp.id, tp.samp_name
            FROM {SOURCE_TABLE} tp
            WHERE tp.field_id IN (SELECT id FROM fields_list)
              AND tp.id = ANY($2::int[])
            """,
            param,
            ids,
        )
    else:
        rows = await conn.fetch(
            f"SELECT id, samp_name FROM {SOURCE_TABLE} WHERE id = ANY($1::int[])",
            ids,
        )

    return {row["id"]: str(row["samp_name"] or "") for row in rows}


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

async def fix_points(
    session: requests.Session,
    conn: asyncpg.Connection,
    col_name: str | None,
    value: str | None,
    output_file: Path | None,
    dry_run: bool,
) -> None:
    base = f"{API_BASE_URL}{API_VERSION}"

    print_step("Fetching point units from API")
    units = fetch_all_pages(session, f"{base}{UNITS_ENDPOINT}", {"unit_type": "point"})
    print_success(f"Found {len(units)} point units total")

    # Filter to those missing sampling_name in metadata
    needs_fix: list[dict] = []
    for unit in units:
        meta = unit.get("sample_unit_metadata") or {}
        if "sampling_name" not in meta:
            needs_fix.append(unit)

    print_info(f"Units missing sampling_name: {len(needs_fix)} / {len(units)}")

    if not needs_fix:
        print_success("All point units already have sampling_name — nothing to do")
        return

    # Collect source ids from metadata
    source_ids: list[int] = []
    for unit in needs_fix:
        meta = unit.get("sample_unit_metadata") or {}
        src_id = meta.get("id")
        if src_id is not None:
            try:
                source_ids.append(int(src_id))
            except (ValueError, TypeError):
                pass

    print_step("Fetching samp_name from source DB")
    samp_name_map = await fetch_samp_names(conn, source_ids, col_name, value)
    print_success(f"Retrieved {len(samp_name_map)} samp_name values from source DB")

    print_step(f"{'Simulating' if dry_run else 'Patching'} units")
    if dry_run:
        print_warning("DRY RUN — no API calls will be made")

    fixed: list[dict[str, Any]] = []
    skipped_no_source_id = 0
    skipped_not_in_db = 0
    errors = 0

    for i, unit in enumerate(needs_fix, 1):
        unit_id = unit["id"]
        unit_name = unit.get("name", "")
        meta = unit.get("sample_unit_metadata") or {}
        src_id = meta.get("id")

        if src_id is None:
            print_warning(f"  [{i}] Unit {unit_id} ({unit_name!r}) — no source id in metadata, skipping")
            skipped_no_source_id += 1
            continue

        try:
            src_id_int = int(src_id)
        except (ValueError, TypeError):
            print_warning(f"  [{i}] Unit {unit_id} ({unit_name!r}) — invalid source id {src_id!r}, skipping")
            skipped_no_source_id += 1
            continue

        samp_name = samp_name_map.get(src_id_int)
        if samp_name is None:
            print_warning(f"  [{i}] Unit {unit_id} ({unit_name!r}) — source id {src_id_int} not found in DB, skipping")
            skipped_not_in_db += 1
            continue

        new_meta = dict(meta)
        new_meta["sampling_name"] = samp_name

        if dry_run:
            print_info(f"  [{i}] Unit {unit_id} ({unit_name!r}) — would add sampling_name={samp_name!r}")
            fixed.append({"unit_id": unit_id, "unit_name": unit_name, "sampling_name": samp_name})
        else:
            resp = session.patch(
                f"{base}{UNITS_ENDPOINT}/{unit_id}",
                json={"sample_unit_metadata": new_meta},
                timeout=15,
            )
            if resp.ok:
                print_success(f"  [{i}] Unit {unit_id} ({unit_name!r}) ← sampling_name={samp_name!r}")
                fixed.append({"unit_id": unit_id, "unit_name": unit_name, "sampling_name": samp_name})
            else:
                print_warning(f"  [{i}] Unit {unit_id} PATCH failed: {resp.status_code} {resp.text}")
                errors += 1

    print_step("Summary")
    print_success(
        f"Fixed: {len(fixed)} | "
        f"Skipped (no source id): {skipped_no_source_id} | "
        f"Skipped (not in DB): {skipped_not_in_db} | "
        f"Errors: {errors}"
    )
    if dry_run:
        print_warning("Dry run — no changes were made")

    output = {
        "fixed": fixed,
        "summary": {
            "fixed": len(fixed),
            "skipped_no_source_id": skipped_no_source_id,
            "skipped_not_in_db": skipped_not_in_db,
            "errors": errors,
        },
    }

    if output_file and not dry_run:
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        print_success(f"Report saved: {output_file}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main() -> None:
    print(f"{Colors.OKBLUE}🔧 Fix sampling_name in point unit metadata{Colors.ENDC}\n")

    parser = argparse.ArgumentParser(
        description="Backfill sampling_name in sample_unit_metadata for existing point units",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--col-name",
        default="id",
        help="Column in xlkey.accounts to filter on (default: id)",
    )
    parser.add_argument(
        "--value",
        default=None,
        help="Value to search in --col-name to limit source DB lookup scope (optional)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would change without making any API calls",
    )
    parser.add_argument(
        "--output-file",
        type=Path,
        default=None,
        help="Path to save the fix report JSON (auto-timestamped by default)",
    )
    parser.add_argument("--token", default=None)
    parser.add_argument("--email", default=None)
    parser.add_argument("--password", default=None)

    args = parser.parse_args()

    if args.output_file is None and not args.dry_run:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        args.output_file = OUTPUT_DIR / f"fix_points_sampling_name_{ts}.json"

    print_step("SETUP - Configuration")
    mode = "DRY RUN" if args.dry_run else "FIX"
    print(f"   Mode        : {mode}")
    print(f"   Source filter : {args.col_name}={args.value!r}" if args.value else "   Source filter : none (all points)")
    print(f"   Output file : {args.output_file or '(none — dry run)'}")
    print(f"   API         : {API_BASE_URL}{API_VERSION}")

    print_step("API - Authenticating")
    token = args.token or API_TOKEN
    email = args.email or API_LOGIN_EMAIL
    password = args.password or API_LOGIN_PASSWORD
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
                print_error("No auth method. Set API_TOKEN, API_CLIENT_ID/SECRET, or API_LOGIN_EMAIL in .env")
            if not password:
                print_error("API password required. Use --password or set API_LOGIN_PASSWORD in .env")
            session = api_login(email, password)
            print_success(f"Authenticated to {API_BASE_URL}")
    except Exception as exc:
        print_error(f"Authentication failed: {exc}")

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
        await fix_points(
            session=session,
            conn=conn,
            col_name=args.col_name if args.value else None,
            value=args.value,
            output_file=args.output_file,
            dry_run=args.dry_run,
        )
    except Exception as exc:
        print_error(f"Unexpected error: {exc}")
    finally:
        await conn.close()
        print_step("DATABASE - Connection closed")

    print_step("COMPLETE")
    print_success("Done!")


if __name__ == "__main__":
    asyncio.run(main())
