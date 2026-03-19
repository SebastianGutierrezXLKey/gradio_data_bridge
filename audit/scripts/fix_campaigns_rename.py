#!/usr/bin/env python3
"""
Script to rename sampling campaigns to French format "Campagne YYYYMM".

Old format: "Campaign 20230614" or "Campaign 2023-06-14"
New format: "Campagne 202306"

Usage:
    # Dry run — show renames without applying
    python audit/scripts/fix_campaigns_rename.py --dry-run

    # Filter by filename substring
    python audit/scripts/fix_campaigns_rename.py --filename-filter "855" --dry-run

    # Real run
    python audit/scripts/fix_campaigns_rename.py --filename-filter "855"
"""
import argparse
import json
import os
import re
import sys
from datetime import datetime
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

# Matches names already in the new format
_ALREADY_RENAMED_RE = re.compile(r"^Campagne \d{6}$")
# Extracts YYYYMMDD or YYYY-MM-DD from a campaign name
_DATE_RE = re.compile(r"(\d{4})-?(\d{2})-?\d{2}")


def parse_year_month(name: str) -> str | None:
    """Return 'YYYYMM' extracted from a campaign name, or None if not parseable."""
    m = _DATE_RE.search(name)
    if m:
        return m.group(1) + m.group(2)
    return None


# ---------------------------------------------------------------------------
# Auth helpers (same pattern as other fix_* scripts)
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
        page_data = data.get("data") or {}
        batch = page_data.get("items") or []
        if not isinstance(batch, list):
            batch = []
        items.extend(batch)
        total = page_data.get("total", 0)
        if len(items) >= total or not batch:
            break
        page += 1
    return items


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def rename_campaigns(
    session: requests.Session,
    filename_filter: str | None,
    output_file: Path | None,
    dry_run: bool,
) -> None:
    base = f"{API_BASE_URL}{API_VERSION}"

    print_step("Fetching campaigns")
    campaigns = fetch_all_pages(session, f"{base}/soil-sampling/campaigns")
    print_success(f"Found {len(campaigns)} campaigns total")

    renamed: list[dict] = []
    skipped_already_ok = 0
    skipped_unparseable = 0
    errors = 0

    for c in campaigns:
        campaign_id = c["id"]
        name = c.get("name") or ""

        # Optional filename filter
        if filename_filter:
            source_filename = str((c.get("interpolation_params") or {}).get("SOURCE_FILENAME", ""))
            if filename_filter.lower() not in source_filename.lower():
                continue

        # Already correct format → skip
        if _ALREADY_RENAMED_RE.match(name):
            skipped_already_ok += 1
            continue

        year_month = parse_year_month(name)
        if not year_month:
            print_warning(f"  Campaign {campaign_id} name={name!r} — cannot parse date, skipping")
            skipped_unparseable += 1
            continue

        new_name = f"Campagne {year_month}"

        if dry_run:
            print_info(f"  [DRY] Campaign {campaign_id}: {name!r} → {new_name!r}")
        else:
            resp = session.patch(
                f"{base}/soil-sampling/campaigns/{campaign_id}",
                json={"name": new_name},
                timeout=15,
            )
            if resp.ok:
                print_success(f"  Campaign {campaign_id}: {name!r} → {new_name!r}")
            else:
                print_warning(f"  Campaign {campaign_id} PATCH failed: {resp.status_code} {resp.text}")
                errors += 1
                continue

        renamed.append({"campaign_id": campaign_id, "old_name": name, "new_name": new_name})

    print_step("Summary")
    print_success(
        f"Renamed: {len(renamed)} | "
        f"Already OK: {skipped_already_ok} | "
        f"Unparseable: {skipped_unparseable} | "
        f"Errors: {errors}"
    )
    if dry_run:
        print_warning("Dry run — no changes were made")

    output = {
        "renamed": renamed,
        "summary": {
            "renamed": len(renamed),
            "already_ok": skipped_already_ok,
            "unparseable": skipped_unparseable,
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

def main() -> None:
    print(f"{Colors.OKBLUE}🔤 Campaigns Rename — Campaign YYYYMMDD → Campagne YYYYMM{Colors.ENDC}\n")

    parser = argparse.ArgumentParser(
        description='Rename campaigns to French format "Campagne YYYYMM"',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--filename-filter",
        default=None,
        metavar="VALUE",
        help="Only process campaigns whose SOURCE_FILENAME contains VALUE",
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
        help="Path to save the rename report JSON (auto-timestamped by default)",
    )
    parser.add_argument("--token", default=None)
    parser.add_argument("--email", default=None)
    parser.add_argument("--password", default=None)

    args = parser.parse_args()

    if args.output_file is None and not args.dry_run:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        args.output_file = OUTPUT_DIR / f"fix_campaigns_rename_{ts}.json"

    print_step("SETUP - Configuration")
    mode = "DRY RUN" if args.dry_run else "RENAME"
    print(f"   Mode            : {mode}")
    print(f"   Filename filter : {args.filename_filter or 'none (all campaigns)'}")
    print(f"   Output file     : {args.output_file or '(none — dry run)'}")
    print(f"   API             : {API_BASE_URL}{API_VERSION}")

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

    try:
        rename_campaigns(
            session=session,
            filename_filter=args.filename_filter,
            output_file=args.output_file,
            dry_run=args.dry_run,
        )
    except Exception as exc:
        print_error(f"Unexpected error: {exc}")

    print_step("COMPLETE")
    print_success("Done!")


if __name__ == "__main__":
    main()
