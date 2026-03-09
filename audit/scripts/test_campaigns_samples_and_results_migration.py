#!/usr/bin/env python3
"""
Script to migrate sampling campaigns, samples, and lab results to the xlhub API.

Reads rows from xlkey.temp_analyses and creates, in sequence:
  1. Sampling campaigns  (POST /soil-sampling/campaigns)
  2. Lab result imports  (POST /soil-sampling/import)
  3. Samples             (POST /soil-sampling/samples)
  4. Sample lab results  (POST /soil-sampling/sample-lab-results)

A sample-units mapping JSON (produced by test_sample_units_migration.py) is
required to resolve xlkey.temp_analyses.FIELD → sampling_unit_id.

Usage:
    # Dry run (default 5 rows)
    python audit/scripts/test_campaigns_samples_and_results_migration.py --dry-run

    # Real run, 1 row
    python audit/scripts/test_campaigns_samples_and_results_migration.py --limit 1

    # 20 rows filtered by filename substring
    python audit/scripts/test_campaigns_samples_and_results_migration.py --limit 20 --filename-filter "681"

    # Rollback previously created records
    python audit/scripts/test_campaigns_samples_and_results_migration.py --downgrade

Pre-requisites:
    - Source PostgreSQL database accessible via SOURCE_* env vars
    - xlhub API accessible via API_* env vars
    - asyncpg, requests, python-dotenv installed
    - A sample_units_mapping_*.json produced by test_sample_units_migration.py
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

# --- Lab config ---
LAB_NAME = os.getenv("LAB_NAME", "")

SOURCE_TABLE = "xlkey.temp_analyses"
OUTPUT_DIR = Path(__file__).parent / "output"

# Source → API field mapping for lab results
LAB_RESULT_FIELD_MAP = {
    "PH": "ph_water",
    "PH_T": "ph_buffer",
    "MO": "organic_matter_percent",
    "P": "phosphorus_kg_ha",
    "K": "potassium_kg_ha",
    "CA": "calcium_kg_ha",
    "MG": "magnesium_kg_ha",
    "AL": "aluminum_ppm",
    "SATURATION_P": "phosphorus_saturation_index",
    "CEC_MEQ": "cec_meq_100g",
    "BORE": "boron_ppm",
    "MN": "manganese_ppm",
    "CU": "copper_ppm",
    "ZN": "zinc_ppm",
    "FE": "iron_ppm",
    "S": "sulfur_ppm",
}


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
    url = f"{API_BASE_URL}{API_VERSION}/service-accounts/token"
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


def to_iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


# ---------------------------------------------------------------------------
# Mapping helpers
# ---------------------------------------------------------------------------

def load_unit_lookup(*mapping_files: Path) -> dict[str, dict]:
    """
    Load one or more sample units mapping files and merge into a single lookup dict.
    Key: zone_name_2 (zones) or name (points).
    Value: full entry dict including target_api_id, unit_type, source_table.
    Warns on key conflicts between files.
    """
    combined: dict[str, dict] = {}
    for mapping_file in mapping_files:
        if mapping_file is None or not mapping_file.exists():
            continue
        with open(mapping_file, encoding="utf-8") as f:
            entries: list[dict] = json.load(f)
        for entry in entries:
            key = entry.get("zone_name_2") or entry.get("name")
            if not key:
                continue
            if key in combined:
                print_warning(
                    f"Key conflict in mapping files: '{key}' exists in both "
                    f"'{combined[key].get('source_table')}' and '{entry.get('source_table')}'. "
                    "Keeping first match."
                )
                continue
            combined[key] = entry
    return combined


def field_to_zone_name_2(field_value: str) -> str | None:
    """
    Convert FIELD column format [field]_[sample_no] → zone_name_2 [sample_no]_[field].
    Example: 'FR01_1' → '1_FR01'
    """
    if not field_value:
        return None
    parts = field_value.split("_", 1)
    if len(parts) != 2:
        return None
    field, sample_no = parts
    return f"{sample_no}_{field}"


# ---------------------------------------------------------------------------
# Pre-flight API lookups
# ---------------------------------------------------------------------------

def get_lab_id(session: requests.Session, lab_name: str) -> str:
    """Fetch the lab ID matching LAB_NAME from the API."""
    url = f"{API_BASE_URL}{API_VERSION}/soil-sampling/laboratories"
    resp = session.get(url, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    labs = data.get("data") or data if isinstance(data.get("data"), list) else []
    if not isinstance(labs, list):
        labs = []
    for lab in labs:
        if (lab.get("name") or "").strip().lower() == lab_name.strip().lower():
            return str(lab["id"])
    print_error(f"Laboratory '{lab_name}' not found in API. Create it first with test_laboratories_creation.py")


# ---------------------------------------------------------------------------
# Source DB query
# ---------------------------------------------------------------------------

async def fetch_analyses(
    conn: asyncpg.Connection,
    limit: int,
    filename_filter: str | None,
) -> list[dict]:
    if filename_filter:
        query = f"""
            SELECT * FROM {SOURCE_TABLE}
            WHERE "FILENAME" ILIKE $1
            ORDER BY id ASC
            LIMIT {limit}
        """
        rows = await conn.fetch(query, f"%{filename_filter}%")
    else:
        query = f'SELECT * FROM {SOURCE_TABLE} ORDER BY id ASC LIMIT {limit}'
        rows = await conn.fetch(query)
    return [dict(row) for row in rows]


# ---------------------------------------------------------------------------
# API calls
# ---------------------------------------------------------------------------

def post_campaign(session: requests.Session, row: dict) -> str:
    """Create a campaign and return its ID."""
    sampling_date = row.get("sampling_date")
    date_key = row.get("DATE_KEY")

    if sampling_date is not None:
        start_date = to_iso(sampling_date)
        name = f"Campaign {start_date}"
    else:
        start_date = to_iso(date_key)
        name = f"Campaign {date_key}"

    payload = {
        "name": name,
        "start_date": start_date,
        "end_date": None,
        "status": "COMPLETED",
        "source": "manual",
        "interpolation_params": {
            "SOURCE_DB": f"{SOURCE_HOST}/{SOURCE_DB}",
            "SOURCE_TABLE": SOURCE_TABLE,
            "SOURCE_ID": str(row.get("id", "")),
            "SOURCE_FILENAME": str(row.get("FILENAME", "")),
        },
    }
    url = f"{API_BASE_URL}{API_VERSION}/soil-sampling/campaigns"
    resp = session.post(url, json=payload, timeout=30)
    if not resp.ok:
        raise RuntimeError(f"Campaign POST failed {resp.status_code}: {resp.text}")
    data = resp.json()
    return str((data.get("data") or data).get("id"))


def post_import(
    session: requests.Session,
    row: dict,
    lab_id: str,
    campaign_id: str,
) -> str:
    """Create an import record and return its ID."""
    full_path = str(row.get("FILENAME", ""))
    filename = full_path.rsplit("/", 1)[-1]
    file_extension = filename.rsplit(".", 1)[-1] if "." in filename else ""
    imported_at = to_iso(row.get("INGESTED_AT"))

    payload = {
        "lab_id": lab_id,
        "sampling_campaign_id": campaign_id,
        "filename": filename,
        "storage_location": full_path,
        "storage_location_type": "S3",
        "file_extension": file_extension,
        "data": None,
        "import_status": "PENDING",
        "imported_at": imported_at,
    }
    url = f"{API_BASE_URL}{API_VERSION}/soil-sampling/import"
    resp = session.post(url, json=payload, timeout=30)
    if not resp.ok:
        raise RuntimeError(f"Import POST failed {resp.status_code}: {resp.text}")
    data = resp.json()
    return str((data.get("data") or data).get("id"))


def post_sample(
    session: requests.Session,
    row: dict,
    sampling_unit_id: str,
    campaign_id: str,
) -> str:
    """Create a sample and return its ID."""
    sampling_date = row.get("sampling_date")
    date_key = row.get("DATE_KEY")
    sampled_at = to_iso(sampling_date) if sampling_date is not None else to_iso(date_key)
    sent_to_lab_at = to_iso(row.get("INGESTED_AT"))

    payload = {
        "sampling_unit_id": sampling_unit_id,
        "sample_label": str(row.get("samp_name") or ""),
        "sent_at": None,
        "sampling_campaign_id": campaign_id,
        "status": "ANALYZED",
        "sampled_at": sampled_at,
        "sent_to_lab_at": sent_to_lab_at,
        "tracking_number": None,
    }
    url = f"{API_BASE_URL}{API_VERSION}/soil-sampling/samples"
    resp = session.post(url, json=payload, timeout=30)
    if not resp.ok:
        raise RuntimeError(f"Sample POST failed {resp.status_code}: {resp.text}")
    data = resp.json()
    return str((data.get("data") or data).get("id"))


def post_lab_result(
    session: requests.Session,
    row: dict,
    sample_id: str,
    import_id: str,
) -> str:
    """Create a sample lab result and return its ID."""
    payload: dict[str, Any] = {
        "sample_id": sample_id,
        "import_lab_result_raw_id": import_id,
    }
    for source_col, api_field in LAB_RESULT_FIELD_MAP.items():
        payload[api_field] = make_serializable(row.get(source_col))

    url = f"{API_BASE_URL}{API_VERSION}/soil-sampling/sample-lab-results"
    resp = session.post(url, json=payload, timeout=30)
    if not resp.ok:
        raise RuntimeError(f"Lab result POST failed {resp.status_code}: {resp.text}")
    data = resp.json()
    return str((data.get("data") or data).get("id"))


# ---------------------------------------------------------------------------
# Upgrade
# ---------------------------------------------------------------------------

async def upgrade(
    conn: asyncpg.Connection,
    session: requests.Session,
    unit_lookup: dict[str, str],
    lab_id: str,
    limit: int,
    filename_filter: str | None,
    output_file: Path,
    dry_run: bool,
) -> None:
    print_step("UPGRADE - Fetching source rows")
    rows = await fetch_analyses(conn, limit, filename_filter)
    print_success(f"Fetched {len(rows)} rows from {SOURCE_TABLE}")

    # In-memory caches to deduplicate campaigns and imports
    campaign_cache: dict[tuple, str] = {}   # (DATE_KEY, NO_DISPATCH, FILENAME) → campaign_id
    import_cache: dict[str, str] = {}       # FILENAME → import_id

    results: list[dict] = []
    succeeded = 0
    skipped = 0
    failed = 0

    for i, row in enumerate(rows, 1):
        source_id = str(row.get("id", i))
        raw_field = str(row.get("FIELD") or "")
        zone_name_2 = field_to_zone_name_2(raw_field)

        if not zone_name_2:
            print_warning(f"[{i}/{len(rows)}] source_id={source_id} — cannot parse FIELD={raw_field!r}, skipping")
            skipped += 1
            continue

        unit_entry = unit_lookup.get(zone_name_2)
        if not unit_entry:
            print_warning(f"[{i}/{len(rows)}] source_id={source_id} — zone_name_2={zone_name_2!r} not in mapping, skipping")
            skipped += 1
            continue

        sampling_unit_id = str(unit_entry["target_api_id"])
        unit_type = unit_entry.get("unit_type", "unknown")

        date_key = str(row.get("DATE_KEY") or "")
        no_dispatch = str(row.get("NO_DISPATCH") or "")
        filename = str(row.get("FILENAME") or "")
        campaign_key = (date_key, no_dispatch, filename)

        if dry_run:
            sampling_date = row.get("sampling_date")
            campaign_name = f"Campaign {to_iso(sampling_date) or date_key}"
            print_info(
                f"[{i}/{len(rows)}] DRY RUN source_id={source_id} "
                f"zone_name_2={zone_name_2!r} unit_id={sampling_unit_id} unit_type={unit_type} "
                f"campaign={campaign_name!r} file={filename.rsplit('/', 1)[-1]!r}"
            )
            succeeded += 1
            continue

        try:
            prefix = f"[{i}/{len(rows)}] source_id={source_id} ({unit_type})"

            # Step 2 — Campaign (dedup by key)
            if campaign_key not in campaign_cache:
                campaign_id = post_campaign(session, row)
                campaign_cache[campaign_key] = campaign_id
                print_success(f"{prefix} Campaign CREATED  id={campaign_id}")
            else:
                campaign_id = campaign_cache[campaign_key]
                print_info(f"{prefix} Campaign REUSED   id={campaign_id}")

            # Step 3 — Import (dedup by FILENAME)
            if filename not in import_cache:
                import_id = post_import(session, row, lab_id, campaign_id)
                import_cache[filename] = import_id
                print_success(f"{prefix} Import   CREATED  id={import_id}")
            else:
                import_id = import_cache[filename]
                print_info(f"{prefix} Import   REUSED   id={import_id}")

            # Step 4 — Sample
            sample_id = post_sample(session, row, sampling_unit_id, campaign_id)
            print_success(f"{prefix} Sample   CREATED  id={sample_id}")

            # Step 5 — Lab result
            lab_result_id = post_lab_result(session, row, sample_id, import_id)
            print_success(f"{prefix} LabResult CREATED id={lab_result_id}")

            results.append({
                "source_id": source_id,
                "zone_name_2": zone_name_2,
                "unit_type": unit_type,
                "FIELD_raw": raw_field,
                "campaign_id": campaign_id,
                "import_id": import_id,
                "sample_id": sample_id,
                "lab_result_id": lab_result_id,
            })
            succeeded += 1

            if i % 10 == 0 or i == len(rows):
                print_success(f"--- Progress: {succeeded} ok, {skipped} skipped, {failed} errors ---")

        except Exception as exc:
            failed += 1
            print_warning(f"[{i}/{len(rows)}] source_id={source_id} failed: {exc}")

    if not dry_run and results:
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print_success(f"Output saved: {output_file}")

    print_step("UPGRADE - Summary")
    print_success(
        f"Rows fetched: {len(rows)} | "
        f"Succeeded: {succeeded} | Skipped: {skipped} | Failed: {failed}"
    )
    if dry_run:
        print_warning("Dry run — no records were written to the API")


# ---------------------------------------------------------------------------
# Downgrade
# ---------------------------------------------------------------------------

def downgrade(session: requests.Session, output_file: Path) -> None:
    print_step("DOWNGRADE - Loading output file")

    if not output_file.exists():
        print_error(f"Output file not found: {output_file}. Run upgrade first.")

    with open(output_file, encoding="utf-8") as f:
        records: list[dict] = json.load(f)
    print_success(f"Loaded {len(records)} records from {output_file}")

    deleted_lab_results = 0
    deleted_samples = 0
    deleted_imports: set[str] = set()
    deleted_campaigns: set[str] = set()
    failed = 0

    # Delete in reverse order: lab_results → samples → imports → campaigns
    print_step("DOWNGRADE - Deleting lab results and samples")
    for entry in reversed(records):
        source_id = entry.get("source_id", "?")

        # Lab result
        lab_result_id = entry.get("lab_result_id")
        if lab_result_id:
            try:
                url = f"{API_BASE_URL}{API_VERSION}/soil-sampling/sample-lab-results/{lab_result_id}"
                resp = session.delete(url, timeout=15)
                resp.raise_for_status()
                deleted_lab_results += 1
            except Exception as exc:
                failed += 1
                print_warning(f"source_id={source_id} lab_result {lab_result_id} delete failed: {exc}")

        # Sample
        sample_id = entry.get("sample_id")
        if sample_id:
            try:
                url = f"{API_BASE_URL}{API_VERSION}/soil-sampling/samples/{sample_id}"
                resp = session.delete(url, timeout=15)
                resp.raise_for_status()
                deleted_samples += 1
            except Exception as exc:
                failed += 1
                print_warning(f"source_id={source_id} sample {sample_id} delete failed: {exc}")

        # Collect import and campaign IDs (delete once, not per row)
        if entry.get("import_id"):
            deleted_imports.add(entry["import_id"])
        if entry.get("campaign_id"):
            deleted_campaigns.add(entry["campaign_id"])

    print_step("DOWNGRADE - Deleting imports")
    for import_id in deleted_imports:
        try:
            url = f"{API_BASE_URL}{API_VERSION}/soil-sampling/import/{import_id}"
            resp = session.delete(url, timeout=15)
            resp.raise_for_status()
            print_info(f"Import {import_id} deleted")
        except Exception as exc:
            failed += 1
            print_warning(f"Import {import_id} delete failed: {exc}")

    print_step("DOWNGRADE - Deleting campaigns")
    for campaign_id in deleted_campaigns:
        try:
            url = f"{API_BASE_URL}{API_VERSION}/soil-sampling/campaigns/{campaign_id}"
            resp = session.delete(url, timeout=15)
            resp.raise_for_status()
            print_info(f"Campaign {campaign_id} deleted")
        except Exception as exc:
            failed += 1
            print_warning(f"Campaign {campaign_id} delete failed: {exc}")

    print_step("DOWNGRADE - Summary")
    print_success(
        f"Lab results deleted: {deleted_lab_results} | Samples deleted: {deleted_samples} | "
        f"Imports deleted: {len(deleted_imports)} | Campaigns deleted: {len(deleted_campaigns)} | "
        f"Errors: {failed}"
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main() -> None:
    print(f"{Colors.OKBLUE}🚀 XLHub Campaigns / Samples / Lab Results Migration Script{Colors.ENDC}")
    print(f"{Colors.OKBLUE}   xlkey.temp_analyses → campaigns, imports, samples, lab results{Colors.ENDC}\n")

    parser = argparse.ArgumentParser(
        description="Migrate campaigns, samples, and lab results to the xlhub API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python audit/scripts/test_campaigns_samples_and_results_migration.py --dry-run
  python audit/scripts/test_campaigns_samples_and_results_migration.py --limit 1
  python audit/scripts/test_campaigns_samples_and_results_migration.py --limit 20 --filename-filter "681"
  python audit/scripts/test_campaigns_samples_and_results_migration.py --downgrade
        """,
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Number of rows to fetch from xlkey.temp_analyses (default: 5)",
    )
    parser.add_argument(
        "--filename-filter",
        default=None,
        metavar="VALUE",
        help="Filter source rows: FILENAME ILIKE '%%{VALUE}%%'",
    )
    parser.add_argument(
        "--zones-mapping-file",
        type=Path,
        default=None,
        dest="zones_mapping_file",
        help="Path to sample_units_mapping JSON (zones). Defaults to latest sample_units_mapping_*.json in output/.",
    )
    parser.add_argument(
        "--points-mapping-file",
        type=Path,
        default=None,
        dest="points_mapping_file",
        help="Path to sample_units_points_mapping JSON. Defaults to latest sample_units_points_mapping_*.json in output/ if present.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate migration without posting to the API",
    )
    parser.add_argument(
        "--downgrade",
        action="store_true",
        help="Delete previously created records using the output file",
    )
    parser.add_argument(
        "--output-file",
        type=Path,
        default=None,
        help="Path to store the migration output. Upgrade: auto-generated. Downgrade: defaults to latest in output/.",
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

    args = parser.parse_args()

    # Resolve output file
    if args.output_file is None:
        if args.downgrade:
            candidates = sorted(OUTPUT_DIR.glob("campaigns_migration_*.json"), reverse=True)
            if not candidates:
                print_error(f"No output file found in {OUTPUT_DIR}. Run upgrade first.")
            args.output_file = candidates[0]
        elif not args.dry_run:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            args.output_file = OUTPUT_DIR / f"campaigns_migration_{ts}.json"

    # Resolve mapping files (not needed for downgrade)
    if not args.downgrade:
        if args.zones_mapping_file is None:
            candidates = sorted(OUTPUT_DIR.glob("sample_units_mapping_*.json"), reverse=True)
            if not candidates:
                print_error(f"No sample_units_mapping file found in {OUTPUT_DIR}. Run test_sample_units_migration.py first.")
            args.zones_mapping_file = candidates[0]
        if not args.zones_mapping_file.exists():
            print_error(f"Zones mapping file not found: {args.zones_mapping_file}")

        if args.points_mapping_file is None:
            candidates = sorted(OUTPUT_DIR.glob("sample_units_points_mapping_*.json"), reverse=True)
            args.points_mapping_file = candidates[0] if candidates else None  # optional

    print_step("SETUP - Configuration")
    mode = "DOWNGRADE" if args.downgrade else ("DRY RUN" if args.dry_run else "UPGRADE")
    print(f"   Mode         : {mode}")
    if not args.downgrade:
        print(f"   Limit        : {args.limit} rows")
        print(f"   Filter       : {args.filename_filter or 'none'}")
        print(f"   Zones mapping  : {args.zones_mapping_file}")
        print(f"   Points mapping : {args.points_mapping_file or '(none found)'}")
    print(f"   Output file  : {args.output_file or '(none — dry run)'}")
    print(f"   API          : {API_BASE_URL}{API_VERSION}")

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
        downgrade(session, args.output_file)
        print_step("COMPLETE")
        print_success("Downgrade completed successfully!")
        return

    # Load and merge unit mappings (zones + points)
    print_step("SETUP - Loading sample units mapping")
    unit_lookup = load_unit_lookup(args.zones_mapping_file, args.points_mapping_file)
    zones_count = sum(1 for e in unit_lookup.values() if e.get("unit_type") == "zone")
    points_count = sum(1 for e in unit_lookup.values() if e.get("unit_type") == "point")
    print_success(f"Loaded {len(unit_lookup)} entries ({zones_count} zones, {points_count} points)")

    # Pre-flight: resolve lab_id
    if not args.dry_run:
        if not LAB_NAME:
            print_error("LAB_NAME is required in .env to resolve the lab for imports")
        print_step("SETUP - Resolving lab ID")
        try:
            lab_id = get_lab_id(session, LAB_NAME)
            print_success(f"Lab '{LAB_NAME}' → id={lab_id}")
        except Exception as exc:
            print_error(f"Failed to resolve lab ID: {exc}")
    else:
        lab_id = "DRY_RUN"

    # Connect to source DB
    if not SOURCE_DB:
        print_error("SOURCE_DB is required. Set it in .env")

    print_step("DATABASE - Connecting to source PostgreSQL")
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
            unit_lookup=unit_lookup,
            lab_id=lab_id,
            limit=args.limit,
            filename_filter=args.filename_filter,
            output_file=args.output_file,
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
