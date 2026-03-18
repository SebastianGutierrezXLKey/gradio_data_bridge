#!/usr/bin/env python3
"""
Script to create a laboratory in the xlhub API.

Checks if the laboratory already exists before creating it to avoid duplicates.
Laboratory details are read from environment variables with sensible defaults.

Usage:
    # Create the laboratory (upgrade)
    python audit/scripts/test_laboratories_creation.py

    # Delete the created laboratory (downgrade)
    python audit/scripts/test_laboratories_creation.py --downgrade

Pre-requisites:
    - xlhub API accessible via API_* env vars
    - requests, python-dotenv installed
"""
import argparse
import json
import os
import sys
import logging
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

LABS_ENDPOINT = "/soil-sampling/laboratories"

# --- Laboratory Details (required — all must be set in .env) ---
LAB_NAME = os.getenv("LAB_NAME", "")
LAB_CODE = os.getenv("LAB_CODE", "")
LAB_ADDRESS = os.getenv("LAB_ADDRESS", "")
LAB_CITY = os.getenv("LAB_CITY", "")
LAB_PROVINCE = os.getenv("LAB_PROVINCE", "")
LAB_POSTAL_CODE = os.getenv("LAB_POSTAL_CODE", "")
LAB_CONTACT_EMAIL = os.getenv("LAB_CONTACT_EMAIL", "")
LAB_CONTACT_PHONE = os.getenv("LAB_CONTACT_PHONE", "")
LAB_COUNTRY = os.getenv("LAB_COUNTRY", "")
LAB_SUPPORTED_FORMATS: dict = json.loads(os.getenv("LAB_SUPPORTED_FORMATS", "{}"))

OUTPUT_DIR = Path(__file__).parent / "output"


# ---------------------------------------------------------------------------
# API helpers
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


def get_existing_lab(session: requests.Session, name: str) -> dict | None:
    """Return the first lab matching *name* (case-insensitive), or None."""
    url = f"{API_BASE_URL}{API_VERSION}{LABS_ENDPOINT}"
    resp = session.get(url, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    labs = data.get("data") or data if isinstance(data.get("data"), list) else []
    if not isinstance(labs, list):
        labs = []
    for lab in labs:
        if (lab.get("name") or "").strip().lower() == name.strip().lower():
            return lab
    return None


# ---------------------------------------------------------------------------
# Upgrade (create laboratory)
# ---------------------------------------------------------------------------

def upgrade(session: requests.Session, record_file: Path) -> None:
    print_step("UPGRADE - Checking Existing Laboratories")

    existing = get_existing_lab(session, LAB_NAME)
    if existing:
        lab_id = existing.get("id")
        print_warning(
            f"Laboratory '{LAB_NAME}' already exists (id={lab_id}). "
            "Skipping creation to avoid duplicates."
        )
        print_info("Use --downgrade to delete it first if you want to recreate it.")
        return

    print_success(f"No existing laboratory named '{LAB_NAME}' found. Proceeding with creation.")

    full_address = f"{LAB_ADDRESS}, {LAB_CITY}, {LAB_PROVINCE} {LAB_POSTAL_CODE}"
    payload = {
        "name": LAB_NAME,
        "code": LAB_CODE,
        "address": LAB_ADDRESS,
        "city": LAB_CITY,
        "province": LAB_PROVINCE,
        "postal_code": LAB_POSTAL_CODE,
        "contact_email": LAB_CONTACT_EMAIL,
        "contact_phone": LAB_CONTACT_PHONE,
        "country": LAB_COUNTRY,
        "supported_formats": LAB_SUPPORTED_FORMATS,
    }

    print_step("UPGRADE - Creating Laboratory")
    print_info(f"Payload: {json.dumps(payload, indent=2)}")

    url = f"{API_BASE_URL}{API_VERSION}{LABS_ENDPOINT}"
    resp = session.post(url, json=payload, timeout=30)
    if not resp.ok:
        print_error(f"API error {resp.status_code}: {resp.text}")
    resp.raise_for_status()
    data = resp.json()
    lab = data.get("data") or data
    lab_id = lab.get("id")

    print_success(f"Laboratory created — id={lab_id}  name='{LAB_NAME}'")

    record = {"lab_id": str(lab_id), "name": LAB_NAME, "code": LAB_CODE}
    record_file.parent.mkdir(parents=True, exist_ok=True)
    with open(record_file, "w", encoding="utf-8") as f:
        json.dump(record, f, indent=2, ensure_ascii=False)
    print_success(f"Record saved to {record_file}")

    print_step("UPGRADE - Summary")
    print_success(f"Laboratory '{LAB_NAME}' (id={lab_id}) is ready in the API.")


# ---------------------------------------------------------------------------
# Downgrade (delete laboratory)
# ---------------------------------------------------------------------------

def downgrade(session: requests.Session, record_file: Path) -> None:
    print_step("DOWNGRADE - Loading Record File")

    if not record_file.exists():
        print_error(f"Record file not found: {record_file}. Run upgrade first or specify --record-file.")

    with open(record_file, encoding="utf-8") as f:
        record = json.load(f)

    lab_id = record.get("lab_id")
    name = record.get("name", "?")

    if not lab_id:
        print_error("No lab_id found in record file.")

    print_info(f"Will delete laboratory id={lab_id}  name='{name}'")

    print_step("DOWNGRADE - Deleting Laboratory")

    url = f"{API_BASE_URL}{API_VERSION}{LABS_ENDPOINT}/{lab_id}"
    resp = session.delete(url, timeout=15)
    resp.raise_for_status()

    print_success(f"Laboratory id={lab_id} ('{name}') deleted from the API.")

    print_step("DOWNGRADE - Summary")
    print_success(f"Downgrade complete. '{name}' has been removed.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    print(f"{Colors.OKBLUE}🚀 XLHub Laboratory Creation Script{Colors.ENDC}")
    print(f"{Colors.OKBLUE}   Creates or removes a laboratory via /soil-sampling/laboratories{Colors.ENDC}\n")

    parser = argparse.ArgumentParser(
        description="Create or delete a laboratory in the xlhub API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Create the laboratory
  python audit/scripts/test_laboratories_creation.py

  # Delete the created laboratory
  python audit/scripts/test_laboratories_creation.py --downgrade
        """,
    )
    parser.add_argument(
        "--downgrade",
        action="store_true",
        help="Delete the laboratory created by a previous upgrade run",
    )
    parser.add_argument(
        "--token",
        default=None,
        help="Bearer token for service account auth (default: API_TOKEN from .env). Takes priority over email/password.",
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
        "--record-file",
        type=Path,
        default=None,
        help="Path to store the created lab record. Upgrade: auto-generated with timestamp in output/. Downgrade: defaults to latest file in output/.",
    )

    args = parser.parse_args()

    # Resolve record file path
    if args.record_file is None:
        if args.downgrade:
            candidates = sorted(OUTPUT_DIR.glob("laboratory_record_*.json"), reverse=True)
            if not candidates:
                print_error(f"No record file found in {OUTPUT_DIR}. Run upgrade first.")
            args.record_file = candidates[0]
        else:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            args.record_file = OUTPUT_DIR / f"laboratory_record_{ts}.json"

    print_step("SETUP - Configuration")
    print(f"   Mode: {'DOWNGRADE' if args.downgrade else 'UPGRADE'}")
    if not args.downgrade:
        print(f"   Lab name  : {LAB_NAME}")
        print(f"   Lab code  : {LAB_CODE}")
        print(f"   Address   : {LAB_ADDRESS}")
        print(f"   City      : {LAB_CITY}")
        print(f"   Province  : {LAB_PROVINCE}")
        print(f"   Postal    : {LAB_POSTAL_CODE}")
        print(f"   Email     : {LAB_CONTACT_EMAIL}")
        print(f"   Phone     : {LAB_CONTACT_PHONE}")
        print(f"   Country   : {LAB_COUNTRY}")
        print(f"   Formats   : {LAB_SUPPORTED_FORMATS}")
    print(f"   Record file: {args.record_file}")
    print(f"   API: {API_BASE_URL}{API_VERSION}{LABS_ENDPOINT}")

    # Validate lab vars (only needed for upgrade)
    if not args.downgrade:
        missing = [
            name for name, val in {
                "LAB_NAME": LAB_NAME, "LAB_CODE": LAB_CODE,
                "LAB_ADDRESS": LAB_ADDRESS, "LAB_CITY": LAB_CITY,
                "LAB_PROVINCE": LAB_PROVINCE, "LAB_POSTAL_CODE": LAB_POSTAL_CODE,
                "LAB_CONTACT_EMAIL": LAB_CONTACT_EMAIL, "LAB_CONTACT_PHONE": LAB_CONTACT_PHONE,
                "LAB_COUNTRY": LAB_COUNTRY, "LAB_SUPPORTED_FORMATS": LAB_SUPPORTED_FORMATS,
            }.items() if not val
        ]
        if missing:
            print_error(f"Missing required .env variables: {', '.join(missing)}")

    token = args.token or API_TOKEN
    email = args.email or API_LOGIN_EMAIL
    password = args.password or API_LOGIN_PASSWORD

    print_step("API - Authenticating")
    try:
        if token:
            # Priority 1: fixed Bearer token
            session = api_session_from_token(token)
            print_success(f"Using fixed Bearer token for {API_BASE_URL}")
        elif API_CLIENT_ID and API_CLIENT_SECRET:
            # Priority 2: service account credentials
            print_info("Fetching service account token...")
            session = api_session_from_client_credentials(API_CLIENT_ID, API_CLIENT_SECRET)
            print_success(f"Authenticated via service account for {API_BASE_URL}")
        else:
            # Priority 3: email/password login
            if not email:
                print_error("No auth method configured. Set API_TOKEN, API_CLIENT_ID/SECRET, or API_LOGIN_EMAIL in .env")
            if not password:
                print_error("API password is required. Use --password or set API_LOGIN_PASSWORD in .env")
            session = api_login(email, password)
            print_success(f"Authenticated to {API_BASE_URL}")
    except Exception as exc:
        print_error(f"API authentication failed: {exc}")

    try:
        if args.downgrade:
            downgrade(session, args.record_file)
        else:
            upgrade(session, args.record_file)

        print_step("COMPLETE")
        print_success("Operation completed successfully!")

    except Exception as exc:
        print_error(f"An unexpected error occurred: {exc}")


if __name__ == "__main__":
    main()
