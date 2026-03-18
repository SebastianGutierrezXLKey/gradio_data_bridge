#!/usr/bin/env python3
"""
Fix null organic_matter_percent for previously migrated lab results.

Reads the campaigns migration output JSON, fetches the M_O column from the
source DB for each source_id, then PATCHes the corresponding lab result via
the API.

Usage:
    # Dry run (show what would be changed)
    python audit/scripts/fix_organic_matter.py --dry-run

    # Apply fixes
    python audit/scripts/fix_organic_matter.py

    # Use a specific output file
    python audit/scripts/fix_organic_matter.py --input audit/scripts/output/campaigns_migration_20260312_151244.json
"""
import argparse
import asyncio
import json
import os
import sys
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


# --- API Configuration ---
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000").rstrip("/")
API_VERSION = os.getenv("API_VERSION", "/api/v1")
API_TOKEN = os.getenv("API_TOKEN", "")
API_LOGIN_ENDPOINT = os.getenv("API_LOGIN_ENDPOINT", "/auth/login")
API_LOGIN_EMAIL = os.getenv("API_LOGIN_EMAIL", "")
API_LOGIN_PASSWORD = os.getenv("API_LOGIN_PASSWORD", "")
API_CLIENT_ID = os.getenv("API_CLIENT_ID", "")
API_CLIENT_SECRET = os.getenv("API_CLIENT_SECRET", "")

# --- Source DB Configuration ---
SOURCE_HOST = os.getenv("SOURCE_HOST", "localhost")
SOURCE_PORT = int(os.getenv("SOURCE_PORT", "5432"))
SOURCE_DB = os.getenv("SOURCE_DB", "")
SOURCE_USER = os.getenv("SOURCE_USER", "postgres")
SOURCE_PASSWORD = os.getenv("SOURCE_PASSWORD", "")

SOURCE_TABLE = "xlkey.temp_analyses"
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
        raise ValueError(f"access_token not found in service account response. Keys: {list(data.keys())}")
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
        print_error(f"Token not found in login response. Keys: {list(data.keys())}")
    session.headers.update({"Authorization": f"Bearer {token}"})
    return session


# ---------------------------------------------------------------------------
# Source DB
# ---------------------------------------------------------------------------

async def fetch_mo_values(source_ids: list[int]) -> dict[str, Any]:
    """Fetch M_O column from source DB for the given source IDs.

    Returns a dict mapping str(id) -> M_O value (float, int, or None).
    """
    if not SOURCE_DB:
        print_error("SOURCE_DB is required. Set it in .env")

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
        print_error(f"Source DB connection failed: {exc}")

    try:
        rows = await conn.fetch(
            f'SELECT id, "M_O" FROM {SOURCE_TABLE} WHERE id = ANY($1)',
            source_ids,
        )
        return {str(row["id"]): row["M_O"] for row in rows}
    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# PATCH helper
# ---------------------------------------------------------------------------

def patch_lab_result(session: requests.Session, lab_result_id: str, value: Any) -> None:
    url = f"{API_BASE_URL}{API_VERSION}/soil-sampling/results/{lab_result_id}"
    resp = session.patch(url, json={"organic_matter_percent": value}, timeout=15)
    if not resp.ok:
        raise RuntimeError(f"PATCH failed {resp.status_code}: {resp.text}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print(f"{Colors.OKBLUE}🔧 Fix organic_matter_percent — re-apply M_O from source DB{Colors.ENDC}\n")

    parser = argparse.ArgumentParser(
        description="Fix null organic_matter_percent for previously migrated lab results"
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=None,
        help="Path to campaigns migration JSON output. Defaults to latest campaigns_migration_*.json in output/.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be changed without applying",
    )
    parser.add_argument(
        "--token",
        default=None,
        help="Bearer token (default: API_TOKEN from .env)",
    )
    args = parser.parse_args()

    # Resolve input file
    if args.input is None:
        candidates = sorted(OUTPUT_DIR.glob("campaigns_migration_*.json"), reverse=True)
        if not candidates:
            print_error(f"No campaigns_migration_*.json found in {OUTPUT_DIR}. Run the migration script first.")
        args.input = candidates[0]

    if not args.input.exists():
        print_error(f"Input file not found: {args.input}")

    print_step("SETUP - Configuration")
    mode = "DRY RUN" if args.dry_run else "APPLY"
    print(f"   Mode      : {mode}")
    print(f"   Input     : {args.input}")
    print(f"   API       : {API_BASE_URL}{API_VERSION}")
    print(f"   Source DB : {SOURCE_HOST}:{SOURCE_PORT}/{SOURCE_DB}")

    # Load migration output
    print_step("LOADING - Migration output")
    with open(args.input, encoding="utf-8") as f:
        records: list[dict] = json.load(f)
    print_success(f"Loaded {len(records)} records from {args.input.name}")

    # Build source_id → lab_result_id map (skip entries without both IDs)
    id_map: dict[str, str] = {}
    for entry in records:
        src = entry.get("source_id")
        lab = entry.get("lab_result_id")
        if src and lab:
            id_map[str(src)] = str(lab)
        else:
            print_warning(f"Skipping entry missing source_id or lab_result_id: {entry}")

    if not id_map:
        print_error("No valid source_id/lab_result_id pairs found in migration output.")

    print_info(f"{len(id_map)} entries with source_id + lab_result_id")

    # Fetch M_O values from source DB
    print_step("SOURCE DB - Fetching M_O values")
    source_ids_int = [int(sid) for sid in id_map]
    mo_values: dict[str, Any] = asyncio.run(fetch_mo_values(source_ids_int))
    non_null = sum(1 for v in mo_values.values() if v is not None)
    print_success(f"Fetched {len(mo_values)} rows — {non_null} with non-null M_O")

    # Authenticate (skip in dry-run if no credentials configured, to allow offline testing)
    session = None
    if not args.dry_run:
        token = args.token or API_TOKEN
        print_step("API - Authenticating")
        try:
            if token:
                session = api_session_from_token(token)
                print_success("Using fixed Bearer token")
            elif API_CLIENT_ID and API_CLIENT_SECRET:
                print_info("Fetching service account token...")
                session = api_session_from_client_credentials(API_CLIENT_ID, API_CLIENT_SECRET)
                print_success("Authenticated via service account")
            else:
                if not API_LOGIN_EMAIL:
                    print_error("No auth method configured. Set API_TOKEN, API_CLIENT_ID/SECRET, or API_LOGIN_EMAIL in .env")
                session = api_login(API_LOGIN_EMAIL, API_LOGIN_PASSWORD)
                print_success("Authenticated via email/password")
        except Exception as exc:
            print_error(f"Authentication failed: {exc}")

    # Apply patches
    print_step("PATCHING - organic_matter_percent")
    updated = 0
    skipped = 0
    failed = 0
    total = len(id_map)

    for i, (source_id, lab_result_id) in enumerate(id_map.items(), 1):
        mo_value = mo_values.get(source_id)

        if mo_value is None:
            print_info(f"[{i}/{total}] source_id={source_id} lab_result_id={lab_result_id} — MO is null, skipping")
            skipped += 1
            continue

        if args.dry_run:
            print_info(f"[{i}/{total}] source_id={source_id} lab_result_id={lab_result_id} → organic_matter_percent={mo_value}")
            updated += 1
            continue

        try:
            patch_lab_result(session, lab_result_id, float(mo_value))
            print_success(f"[{i}/{total}] lab_result_id={lab_result_id} → organic_matter_percent={mo_value}")
            updated += 1
        except Exception as exc:
            failed += 1
            print_warning(f"[{i}/{total}] lab_result_id={lab_result_id} failed: {exc}")

    print_step("SUMMARY")
    action = "Would update" if args.dry_run else "Updated"
    print_success(f"{action}: {updated} | Skipped (null M_O): {skipped} | Failed: {failed}")
    if args.dry_run:
        print_warning("Dry run — no changes were applied. Remove --dry-run to apply.")


if __name__ == "__main__":
    main()
