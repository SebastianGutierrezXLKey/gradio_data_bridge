#!/usr/bin/env python3
"""
Fix sample_label for previously migrated samples.

Reads the campaigns migration output JSON and PATCHes each sample's
sample_label to use [field]_[sample_no] format (FIELD_raw) instead of
the inverted [sample_no]_[field] that was incorrectly set.

Usage:
    # Dry run (show what would be changed)
    python audit/scripts/fix_sample_labels.py --dry-run

    # Apply fixes
    python audit/scripts/fix_sample_labels.py

    # Use a specific output file
    python audit/scripts/fix_sample_labels.py --input audit/scripts/output/campaigns_migration_20260309_120000.json
"""
import argparse
import json
import os
import sys
from pathlib import Path

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
# Fix logic
# ---------------------------------------------------------------------------

def field_raw_to_label(field_raw: str) -> str:
    """Return the correct sample_label from FIELD_raw ([field]_[sample_no])."""
    return field_raw  # already in the right format: e.g. "FR01_1"


def patch_sample_label(session: requests.Session, sample_id: str, label: str) -> None:
    url = f"{API_BASE_URL}{API_VERSION}/soil-sampling/samples/{sample_id}"
    resp = session.patch(url, json={"sample_label": label}, timeout=15)
    if not resp.ok:
        raise RuntimeError(f"PATCH failed {resp.status_code}: {resp.text}")


def main() -> None:
    print(f"{Colors.OKBLUE}🔧 Fix Sample Labels — [field]_[sample_no] format{Colors.ENDC}\n")

    parser = argparse.ArgumentParser(
        description="Fix sample_label for previously migrated samples"
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
    print(f"   Mode   : {mode}")
    print(f"   Input  : {args.input}")
    print(f"   API    : {API_BASE_URL}{API_VERSION}")

    # Authenticate
    token = args.token or API_TOKEN
    print_step("API - Authenticating")
    try:
        if token:
            session = api_session_from_token(token)
            print_success(f"Using fixed Bearer token")
        elif API_CLIENT_ID and API_CLIENT_SECRET:
            print_info("Fetching service account token...")
            session = api_session_from_client_credentials(API_CLIENT_ID, API_CLIENT_SECRET)
            print_success(f"Authenticated via service account")
        else:
            if not API_LOGIN_EMAIL:
                print_error("No auth method configured. Set API_TOKEN, API_CLIENT_ID/SECRET, or API_LOGIN_EMAIL in .env")
            session = api_login(API_LOGIN_EMAIL, API_LOGIN_PASSWORD)
            print_success(f"Authenticated via email/password")
    except Exception as exc:
        print_error(f"Authentication failed: {exc}")

    # Load migration output
    print_step("LOADING - Migration output")
    with open(args.input, encoding="utf-8") as f:
        records: list[dict] = json.load(f)
    print_success(f"Loaded {len(records)} records from {args.input.name}")

    # Apply fixes
    print_step("FIXING - Sample labels")
    updated = 0
    skipped = 0
    failed = 0

    for i, entry in enumerate(records, 1):
        sample_id = entry.get("sample_id")
        field_raw = entry.get("FIELD_raw", "")
        source_id = entry.get("source_id", "?")

        if not sample_id:
            print_warning(f"[{i}/{len(records)}] source_id={source_id} — no sample_id, skipping")
            skipped += 1
            continue

        if not field_raw:
            print_warning(f"[{i}/{len(records)}] source_id={source_id} sample_id={sample_id} — no FIELD_raw, skipping")
            skipped += 1
            continue

        correct_label = field_raw_to_label(field_raw)

        if args.dry_run:
            print_info(f"[{i}/{len(records)}] sample_id={sample_id} → sample_label={correct_label!r}")
            updated += 1
            continue

        try:
            patch_sample_label(session, sample_id, correct_label)
            print_success(f"[{i}/{len(records)}] sample_id={sample_id} → sample_label={correct_label!r}")
            updated += 1
        except Exception as exc:
            failed += 1
            print_warning(f"[{i}/{len(records)}] sample_id={sample_id} failed: {exc}")

    print_step("SUMMARY")
    action = "Would update" if args.dry_run else "Updated"
    print_success(f"{action}: {updated} | Skipped: {skipped} | Failed: {failed}")
    if args.dry_run:
        print_warning("Dry run — no changes were applied. Remove --dry-run to apply.")


if __name__ == "__main__":
    main()
