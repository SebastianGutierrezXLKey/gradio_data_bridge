#!/usr/bin/env python3
"""
Script to fix duplicate sampling campaigns and imports.

When the campaign migration used date-based deduplication, multiple campaigns
were created for the same source file. This script:
  1. Fetches all campaigns and groups them by SOURCE_FILENAME
  2. For each duplicate group, keeps the canonical campaign (lowest id)
  3. Reassigns samples and lab results from duplicates to the canonical records
     via PATCH, then deletes the duplicate imports and campaigns

Usage:
    # Dry run — show what would change
    python audit/scripts/fix_campaigns_dedup.py --dry-run

    # Filter by filename substring
    python audit/scripts/fix_campaigns_dedup.py --filename-filter "855" --dry-run

    # Real run
    python audit/scripts/fix_campaigns_dedup.py --filename-filter "855"

Pre-requisites:
    - xlhub API accessible via API_* env vars
    - Backend must expose sampling_campaign_id in PATCH /samples and
      import_lab_result_raw_id in PATCH /results
"""
import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

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
        raise ValueError(f"access_token not found in service account response.")
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
        print_error(f"Token not found in login response.")
    session.headers.update({"Authorization": f"Bearer {token}"})
    return session


# ---------------------------------------------------------------------------
# Paginated fetch helper
# ---------------------------------------------------------------------------

def fetch_all_pages(session: requests.Session, url: str, params: dict | None = None) -> list[dict]:
    """Fetch all pages from a paginated endpoint and return all items."""
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

def group_campaigns_by_filename(
    campaigns: list[dict],
    filename_filter: str | None,
) -> dict[str, list[dict]]:
    """Group campaigns by SOURCE_FILENAME basename, optionally filtered."""
    groups: dict[str, list[dict]] = {}
    for c in campaigns:
        params = c.get("interpolation_params") or {}
        source_filename = str(params.get("SOURCE_FILENAME", ""))
        basename = source_filename.rsplit("/", 1)[-1] if source_filename else ""
        if not basename:
            continue
        if filename_filter and filename_filter.lower() not in source_filename.lower():
            continue
        groups.setdefault(basename, []).append(c)
    return groups


def cleanup(
    session: requests.Session,
    filename_filter: str | None,
    output_file: Path | None,
    dry_run: bool,
) -> None:
    base = f"{API_BASE_URL}{API_VERSION}"

    # Step 1 — fetch all campaigns
    print_step("Fetching campaigns")
    campaigns = fetch_all_pages(session, f"{base}/soil-sampling/campaigns")
    print_success(f"Found {len(campaigns)} campaigns total")

    groups = group_campaigns_by_filename(campaigns, filename_filter)
    dup_groups = {fn: cs for fn, cs in groups.items() if len(cs) > 1}
    print_info(f"Groups with duplicates: {len(dup_groups)} / {len(groups)} total")

    if not dup_groups:
        print_success("No duplicates found — nothing to do")
        return

    fixed_groups: list[dict[str, Any]] = []
    total_samples_reassigned = 0
    total_results_reassigned = 0
    total_imports_deleted = 0
    total_campaigns_deleted = 0

    for filename, group_campaigns in dup_groups.items():
        # Canonical = lowest id (first created)
        group_campaigns.sort(key=lambda c: c["id"])
        canonical = group_campaigns[0]
        duplicates = group_campaigns[1:]

        canonical_id = canonical["id"]
        print_step(f"Processing: {filename}")
        print_info(f"  Canonical campaign id={canonical_id} ({canonical.get('name')})")
        print_info(f"  Duplicates: {[c['id'] for c in duplicates]}")

        # Find canonical import
        canonical_imports = fetch_all_pages(
            session,
            f"{base}/soil-sampling/imports",
            {"sampling_campaign_id": canonical_id},
        )
        canonical_import = next(
            (imp for imp in canonical_imports if imp.get("filename") == filename),
            canonical_imports[0] if canonical_imports else None,
        )
        canonical_import_id = canonical_import["id"] if canonical_import else None
        if canonical_import_id:
            print_info(f"  Canonical import id={canonical_import_id}")
        else:
            print_warning(f"  No canonical import found for campaign {canonical_id}")

        group_result: dict[str, Any] = {
            "filename": filename,
            "canonical_campaign_id": canonical_id,
            "canonical_import_id": canonical_import_id,
            "duplicates": [],
        }

        for dup in duplicates:
            dup_id = dup["id"]
            dup_result: dict[str, Any] = {
                "campaign_id": dup_id,
                "import_id": None,
                "samples_reassigned": [],
                "import_deleted": False,
                "campaign_deleted": False,
            }

            # Fetch samples under duplicate campaign
            dup_samples = fetch_all_pages(
                session,
                f"{base}/soil-sampling/samples",
                {"sampling_campaign_id": dup_id},
            )
            print_info(f"  Duplicate campaign {dup_id}: {len(dup_samples)} samples")

            for sample in dup_samples:
                sample_id = sample["id"]
                reassigned_results: list[int] = []

                # Fetch lab results for this sample
                resp = session.get(f"{base}/soil-sampling/samples/{sample_id}/results", timeout=15)
                resp.raise_for_status()
                results_data = resp.json()
                lab_results = results_data if isinstance(results_data, list) else (
                    (results_data.get("data") or []) if isinstance(results_data.get("data"), list)
                    else []
                )

                for result in lab_results:
                    result_id = result["id"]
                    if not dry_run and canonical_import_id:
                        patch_resp = session.patch(
                            f"{base}/soil-sampling/results/{result_id}",
                            json={"import_lab_result_raw_id": canonical_import_id},
                            timeout=15,
                        )
                        if not patch_resp.ok:
                            print_warning(f"    PATCH result {result_id} failed: {patch_resp.status_code} {patch_resp.text}")
                        else:
                            reassigned_results.append(result_id)
                            total_results_reassigned += 1
                    else:
                        reassigned_results.append(result_id)
                        if dry_run:
                            total_results_reassigned += 1

                # PATCH sample → canonical campaign
                if not dry_run:
                    patch_resp = session.patch(
                        f"{base}/soil-sampling/samples/{sample_id}",
                        json={"sampling_campaign_id": canonical_id},
                        timeout=15,
                    )
                    if not patch_resp.ok:
                        print_warning(f"    PATCH sample {sample_id} failed: {patch_resp.status_code} {patch_resp.text}")
                    else:
                        total_samples_reassigned += 1
                        print_info(f"    Sample {sample_id} → campaign {canonical_id} ({len(reassigned_results)} results reassigned)")
                else:
                    total_samples_reassigned += 1
                    print_info(f"    [DRY] Sample {sample_id} would → campaign {canonical_id} ({len(reassigned_results)} results)")

                dup_result["samples_reassigned"].append({
                    "sample_id": sample_id,
                    "results_reassigned": reassigned_results,
                })

            # Fetch and delete duplicate imports
            dup_imports = fetch_all_pages(
                session,
                f"{base}/soil-sampling/imports",
                {"sampling_campaign_id": dup_id},
            )
            for imp in dup_imports:
                imp_id = imp["id"]
                dup_result["import_id"] = imp_id
                if not dry_run:
                    del_resp = session.delete(f"{base}/soil-sampling/imports/{imp_id}", timeout=15)
                    if del_resp.ok:
                        total_imports_deleted += 1
                        dup_result["import_deleted"] = True
                        print_info(f"    Import {imp_id} deleted")
                    else:
                        print_warning(f"    DELETE import {imp_id} failed: {del_resp.status_code}")
                else:
                    total_imports_deleted += 1
                    dup_result["import_deleted"] = True
                    print_info(f"    [DRY] Import {imp_id} would be deleted")

            # Delete duplicate campaign
            if not dry_run:
                del_resp = session.delete(f"{base}/soil-sampling/campaigns/{dup_id}", timeout=15)
                if del_resp.ok:
                    total_campaigns_deleted += 1
                    dup_result["campaign_deleted"] = True
                    print_success(f"  Campaign {dup_id} deleted")
                else:
                    print_warning(f"  DELETE campaign {dup_id} failed: {del_resp.status_code}")
            else:
                total_campaigns_deleted += 1
                dup_result["campaign_deleted"] = True
                print_info(f"  [DRY] Campaign {dup_id} would be deleted")

            group_result["duplicates"].append(dup_result)

        fixed_groups.append(group_result)

    print_step("Summary")
    print_success(
        f"Groups fixed: {len(fixed_groups)} | "
        f"Samples reassigned: {total_samples_reassigned} | "
        f"Results reassigned: {total_results_reassigned} | "
        f"Imports deleted: {total_imports_deleted} | "
        f"Campaigns deleted: {total_campaigns_deleted}"
    )
    if dry_run:
        print_warning("Dry run — no changes were made")

    output = {
        "fixed_groups": fixed_groups,
        "summary": {
            "groups_fixed": len(fixed_groups),
            "samples_reassigned": total_samples_reassigned,
            "results_reassigned": total_results_reassigned,
            "imports_deleted": total_imports_deleted,
            "campaigns_deleted": total_campaigns_deleted,
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
    print(f"{Colors.OKBLUE}🔧 Campaigns / Imports Deduplication Fix{Colors.ENDC}\n")

    parser = argparse.ArgumentParser(
        description="Reassign samples/results from duplicate campaigns to canonical, then delete duplicates",
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
        help="Path to save the fix report JSON (auto-timestamped by default)",
    )
    parser.add_argument("--token", default=None)
    parser.add_argument("--email", default=None)
    parser.add_argument("--password", default=None)

    args = parser.parse_args()

    if args.output_file is None and not args.dry_run:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        args.output_file = OUTPUT_DIR / f"fix_campaigns_dedup_{ts}.json"

    print_step("SETUP - Configuration")
    mode = "DRY RUN" if args.dry_run else "FIX"
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
        cleanup(
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
