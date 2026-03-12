"""Soil sampling migration runner — callable from Gradio or CLI.

Migrates rows from a source table (e.g. xlkey.temp_analyses) into:
  1. POST /soil-sampling/campaigns
  2. POST /soil-sampling/imports
  3. POST /soil-sampling/samples
  4. POST /soil-sampling/results

Unlike the standalone CLI script, this module:
- Uses psycopg2 (DBConnector) instead of asyncpg
- Accepts an explicit unit_mapping dict built from the Gradio UI
- Yields log lines as a generator for real-time display
- Writes output files (JSON + plain-text log) to output_dir
"""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any, Generator

import requests

# ---------------------------------------------------------------------------
# Source → API field mapping for lab results
# ---------------------------------------------------------------------------

LAB_RESULT_FIELD_MAP: dict[str, str] = {
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


def to_date_str(value: Any) -> str | None:
    """Return a date-only string (YYYY-MM-DD) from a date, datetime, or ISO string."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    s = str(value).strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:]}"
    return s[:10] if len(s) >= 10 else s


# ---------------------------------------------------------------------------
# Source DB queries (psycopg2 via DBConnector)
# ---------------------------------------------------------------------------

def fetch_distinct_fields(
    conn,  # DBConnector
    source_table: str,
    filename_filter: str | None = None,
) -> list[tuple[str, int]]:
    """Return [(FIELD_value, count)] sorted by count desc."""
    if filename_filter:
        sql = (
            f'SELECT "FIELD", COUNT(*) AS cnt FROM {source_table} '
            f'WHERE "FILENAME" ILIKE %s GROUP BY "FIELD" ORDER BY cnt DESC'
        )
        rows = conn.execute_query(sql, (f"%{filename_filter}%",))
    else:
        sql = (
            f'SELECT "FIELD", COUNT(*) AS cnt FROM {source_table} '
            f'GROUP BY "FIELD" ORDER BY cnt DESC'
        )
        rows = conn.execute_query(sql)
    return [(str(r["FIELD"]), int(r["cnt"])) for r in rows if r.get("FIELD")]


def fetch_source_rows(
    conn,  # DBConnector
    source_table: str,
    limit: int,
    filename_filter: str | None = None,
) -> list[dict]:
    """Fetch rows from the source table."""
    if filename_filter:
        sql = (
            f'SELECT * FROM {source_table} '
            f'WHERE "FILENAME" ILIKE %s ORDER BY id ASC LIMIT %s'
        )
        return conn.execute_query(sql, (f"%{filename_filter}%", limit))
    sql = f'SELECT * FROM {source_table} ORDER BY id ASC LIMIT %s'
    return conn.execute_query(sql, (limit,))


# ---------------------------------------------------------------------------
# API units fetching
# ---------------------------------------------------------------------------

def fetch_units_from_api(
    session: requests.Session,
    api_base: str,
    api_version: str,
) -> list[dict]:
    """GET /soil-sampling/units and return list of unit dicts."""
    url = f"{api_base}{api_version}/soil-sampling/units"
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    items = data.get("data") or []
    return items if isinstance(items, list) else []


def fetch_units_from_db(conn, sql: str) -> list[dict]:
    """Execute a custom SQL query on the target DB and return rows."""
    return conn.execute_query(sql)


# ---------------------------------------------------------------------------
# Pre-flight API lookups
# ---------------------------------------------------------------------------

def prefetch_campaigns(session: requests.Session, api_base: str, api_version: str) -> dict[str, str]:
    url = f"{api_base}{api_version}/soil-sampling/campaigns"
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    items = data.get("data") or []
    if not isinstance(items, list):
        items = []
    return {str(c.get("name", "")): str(c["id"]) for c in items if c.get("id")}


def prefetch_imports(session: requests.Session, api_base: str, api_version: str) -> dict[str, str]:
    url = f"{api_base}{api_version}/soil-sampling/imports"
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    items = data.get("data") or []
    if not isinstance(items, list):
        items = []
    return {str(i.get("filename", "")): str(i["id"]) for i in items if i.get("id")}


def get_lab_id(session: requests.Session, api_base: str, api_version: str, lab_name: str) -> str | None:
    url = f"{api_base}{api_version}/soil-sampling/laboratories"
    resp = session.get(url, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    labs = data.get("data") or []
    if not isinstance(labs, list):
        labs = []
    for lab in labs:
        if (lab.get("name") or "").strip().lower() == lab_name.strip().lower():
            return str(lab["id"])
    return None


# ---------------------------------------------------------------------------
# API POST functions
# ---------------------------------------------------------------------------

def post_campaign(
    session: requests.Session,
    api_base: str,
    api_version: str,
    row: dict,
    source_host: str = "",
    source_db: str = "",
    source_table: str = "",
) -> str:
    sampling_date = row.get("sampling_date")
    date_key = row.get("DATE_KEY")
    if sampling_date is not None:
        start_date = to_date_str(sampling_date)
        name = f"Campaign {start_date}"
    else:
        start_date = to_date_str(date_key)
        name = f"Campaign {date_key}"

    payload = {
        "name": name,
        "start_date": start_date,
        "end_date": None,
        "status": "COMPLETED",
        "source": "manual",
        "interpolation_params": {
            "SOURCE_DB": f"{source_host}/{source_db}",
            "SOURCE_TABLE": source_table,
            "SOURCE_ID": str(row.get("id", "")),
            "SOURCE_FILENAME": str(row.get("FILENAME", "")),
        },
    }
    url = f"{api_base}{api_version}/soil-sampling/campaigns"
    resp = session.post(url, json=payload, timeout=30)
    if not resp.ok:
        raise RuntimeError(f"Campaign POST {resp.status_code}: {resp.text}")
    data = resp.json()
    return str((data.get("data") or data).get("id"))


def post_import(
    session: requests.Session,
    api_base: str,
    api_version: str,
    row: dict,
    lab_id: str,
    campaign_id: str,
) -> str:
    full_path = str(row.get("FILENAME", ""))
    filename = full_path.rsplit("/", 1)[-1]
    file_extension = filename.rsplit(".", 1)[-1] if "." in filename else ""
    payload = {
        "lab_id": lab_id,
        "sampling_campaign_id": campaign_id,
        "filename": filename,
        "storage_location": full_path,
        "storage_location_type": "S3",
        "file_extension": file_extension,
        "data": None,
        "import_status": "pending",
        "imported_at": to_iso(row.get("INGESTED_AT")),
    }
    url = f"{api_base}{api_version}/soil-sampling/imports"
    resp = session.post(url, json=payload, timeout=30)
    if not resp.ok:
        raise RuntimeError(f"Import POST {resp.status_code}: {resp.text}")
    data = resp.json()
    return str((data.get("data") or data).get("id"))


def post_sample(
    session: requests.Session,
    api_base: str,
    api_version: str,
    row: dict,
    sampling_unit_id: str,
    campaign_id: str,
    sample_label: str,
) -> str:
    sampling_date = row.get("sampling_date")
    date_key = row.get("DATE_KEY")
    sampled_at = to_iso(sampling_date) if sampling_date is not None else to_date_str(date_key)
    payload = {
        "sampling_unit_id": sampling_unit_id,
        "sample_label": sample_label or str(row.get("FIELD") or ""),
        "sent_at": None,
        "sampling_campaign_id": campaign_id,
        "status": "ANALYZED",
        "sampled_at": sampled_at,
        "sent_to_lab_at": to_iso(row.get("INGESTED_AT")),
        "tracking_number": None,
        "date_key": make_serializable(row.get("DATE_KEY")),
        "nolab": make_serializable(row.get("NOLAB")),
    }
    url = f"{api_base}{api_version}/soil-sampling/samples"
    resp = session.post(url, json=payload, timeout=30)
    if not resp.ok:
        raise RuntimeError(f"Sample POST {resp.status_code}: {resp.text}")
    data = resp.json()
    return str((data.get("data") or data).get("id"))


def post_lab_result(
    session: requests.Session,
    api_base: str,
    api_version: str,
    row: dict,
    sample_id: str,
    import_id: str,
) -> str:
    payload: dict[str, Any] = {
        "sample_id": sample_id,
        "import_lab_result_raw_id": import_id,
        "date_key": make_serializable(row.get("DATE_KEY")),
        "nolab": make_serializable(row.get("NOLAB")),
    }
    for src_col, api_field in LAB_RESULT_FIELD_MAP.items():
        payload[api_field] = make_serializable(row.get(src_col))
    url = f"{api_base}{api_version}/soil-sampling/results"
    resp = session.post(url, json=payload, timeout=30)
    if not resp.ok:
        raise RuntimeError(f"Lab result POST {resp.status_code}: {resp.text}")
    data = resp.json()
    return str((data.get("data") or data).get("id"))


# ---------------------------------------------------------------------------
# Main migration runner
# ---------------------------------------------------------------------------

def run_migration(
    source_conn,            # DBConnector
    api_session: requests.Session,
    api_base: str,
    api_version: str,
    unit_mapping: dict[str, dict],   # FIELD → {"unit_id": str, "sample_label": str}
    lab_name: str,
    source_table: str,
    filename_filter: str | None,
    limit: int,
    dry_run: bool,
    output_dir: Path,
) -> Generator[str, None, None]:
    """Run the soil sampling migration and yield log lines.

    Writes output files to output_dir when not in dry_run mode.
    The final yielded value is a dict with keys: json_path, log_path.
    """
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_lines: list[str] = []

    def log(msg: str) -> str:
        log_lines.append(msg)
        return msg

    yield log(f"=== UPGRADE — {source_table} → xlhub API ===")
    yield log(f"Mode    : {'DRY RUN' if dry_run else 'RÉEL'}")
    yield log(f"Limite  : {limit} lignes")
    yield log(f"Filtre  : {filename_filter or 'aucun'}")
    yield log(f"API     : {api_base}{api_version}")
    yield log("")

    # Pre-flight: lab ID
    lab_id = "DRY_RUN"
    if not dry_run:
        if not lab_name:
            yield log("❌ LAB_NAME requis pour créer les imports.")
            return
        yield log(f"Résolution du lab '{lab_name}'...")
        lab_id = get_lab_id(api_session, api_base, api_version, lab_name)
        if not lab_id:
            yield log(f"❌ Lab '{lab_name}' introuvable dans l'API.")
            return
        yield log(f"✅ Lab '{lab_name}' → id={lab_id}")

    # Pre-fetch existing campaigns and imports
    if not dry_run:
        yield log("Pré-chargement des campagnes existantes...")
        campaign_cache = prefetch_campaigns(api_session, api_base, api_version)
        yield log(f"✅ {len(campaign_cache)} campagne(s) existante(s)")

        yield log("Pré-chargement des imports existants...")
        import_cache = prefetch_imports(api_session, api_base, api_version)
        yield log(f"✅ {len(import_cache)} import(s) existant(s)")
    else:
        campaign_cache: dict[str, str] = {}
        import_cache: dict[str, str] = {}

    # Fetch source rows
    yield log(f"\nChargement des lignes source depuis {source_table}...")
    rows = fetch_source_rows(source_conn, source_table, limit, filename_filter)
    yield log(f"✅ {len(rows)} lignes chargées")

    results: list[dict] = []
    succeeded = 0
    skipped = 0
    failed = 0
    total = len(rows)

    for i, row in enumerate(rows, 1):
        source_id = str(row.get("id", i))
        raw_field = str(row.get("FIELD") or "")

        if not raw_field:
            yield log(f"⚠  [{i}/{total}] source_id={source_id} — FIELD vide, ignoré")
            skipped += 1
            continue

        unit_entry = unit_mapping.get(raw_field)
        if not unit_entry:
            yield log(f"⚠  [{i}/{total}] source_id={source_id} — FIELD={raw_field!r} non mappé, ignoré")
            skipped += 1
            continue

        unit_id = str(unit_entry.get("unit_id", ""))
        sample_label = str(unit_entry.get("sample_label") or raw_field)
        filename = str(row.get("FILENAME") or "")
        sampling_date = row.get("sampling_date")
        date_key = str(row.get("DATE_KEY") or "")
        campaign_name = f"Campaign {to_date_str(sampling_date) or to_date_str(date_key) or date_key}"

        if dry_run:
            yield log(
                f"ℹ  [{i}/{total}] DRY RUN source_id={source_id} "
                f"FIELD={raw_field!r} unit_id={unit_id} "
                f"label={sample_label!r} campaign={campaign_name!r}"
            )
            succeeded += 1
            continue

        try:
            prefix = f"[{i}/{total}] source_id={source_id}"

            # Campaign
            if campaign_name not in campaign_cache:
                campaign_id = post_campaign(
                    api_session, api_base, api_version, row,
                    source_table=source_table,
                )
                campaign_cache[campaign_name] = campaign_id
                yield log(f"✅ {prefix} Campaign CRÉÉE  id={campaign_id}")
            else:
                campaign_id = campaign_cache[campaign_name]
                yield log(f"ℹ  {prefix} Campaign RÉUTILISÉE id={campaign_id}")

            # Import
            fname_key = filename.rsplit("/", 1)[-1]
            if fname_key not in import_cache:
                import_id = post_import(api_session, api_base, api_version, row, lab_id, campaign_id)
                import_cache[fname_key] = import_id
                yield log(f"✅ {prefix} Import   CRÉÉ     id={import_id}")
            else:
                import_id = import_cache[fname_key]
                yield log(f"ℹ  {prefix} Import   RÉUTILISÉ id={import_id}")

            # Sample
            sample_id = post_sample(
                api_session, api_base, api_version, row,
                unit_id, campaign_id, sample_label,
            )
            yield log(f"✅ {prefix} Sample   CRÉÉ     id={sample_id}")

            # Lab result
            lab_result_id = post_lab_result(
                api_session, api_base, api_version, row, sample_id, import_id,
            )
            yield log(f"✅ {prefix} Résultat CRÉÉ     id={lab_result_id}")

            results.append({
                "source_id": source_id,
                "FIELD_raw": raw_field,
                "sample_label": sample_label,
                "unit_id": unit_id,
                "campaign_id": campaign_id,
                "import_id": import_id,
                "sample_id": sample_id,
                "lab_result_id": lab_result_id,
            })
            succeeded += 1

        except Exception as exc:
            failed += 1
            yield log(f"⚠  [{i}/{total}] source_id={source_id} ÉCHEC: {exc}")

    yield log(f"\n=== RÉSUMÉ ===")
    yield log(f"Lignes totales : {total}")
    yield log(f"Succès         : {succeeded}")
    yield log(f"Ignorées       : {skipped}")
    yield log(f"Erreurs        : {failed}")

    # Write output files
    json_path: Path | None = None
    log_path: Path | None = None

    if not dry_run and results:
        output_dir.mkdir(parents=True, exist_ok=True)
        json_path = output_dir / f"campaigns_migration_{ts}.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        yield log(f"✅ JSON sauvegardé : {json_path}")

    if log_lines:
        output_dir.mkdir(parents=True, exist_ok=True)
        log_path = output_dir / f"migration_log_{ts}.txt"
        with open(log_path, "w", encoding="utf-8") as f:
            f.write("\n".join(log_lines))
        yield log(f"✅ Log sauvegardé  : {log_path}")

    # Signal completion via state in the last yield
    yield json.dumps({"json_path": str(json_path) if json_path else None,
                       "log_path": str(log_path) if log_path else None,
                       "__done__": True})
