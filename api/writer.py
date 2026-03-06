"""API-based record writer — replaces database/writer.py for API mode."""

from __future__ import annotations

from typing import Any

from api.client import ApiClient


def post_record(
    client: ApiClient,
    endpoint: str,
    data: dict[str, Any],
    id_field: str = "id",
) -> Any:
    """POST *data* to *endpoint* and return the new record's ID.

    Args:
        client: Configured ApiClient instance.
        endpoint: API path, e.g. '/soil-sampling/imports'
        data: Payload to send (already mapped from source columns).
        id_field: Field name in response.data that holds the new ID.

    Returns:
        The new record's ID (any type), or None.

    Raises:
        requests.HTTPError on API error.
    """
    response = client.post(endpoint, data)
    # Standard xlhub response: { success: true, data: { id: ..., ... } }
    record = response.get("data") or response
    if isinstance(record, dict):
        return record.get(id_field)
    return None


# ---------------------------------------------------------------------------
# Known endpoints registry (for UI dropdown)
# ---------------------------------------------------------------------------

KNOWN_ENDPOINTS: dict[str, dict[str, Any]] = {
    "/soil-sampling/imports": {
        "label": "Import Lab Results",
        "id_field": "id",
        "required_fields": ["lab_id", "filename"],
        "optional_fields": [
            "sampling_campaign_id", "storage_location",
            "storage_location_type", "file_extension",
            "data", "import_status",
        ],
    },
    "/soil-sampling/results": {
        "label": "Lab Results",
        "id_field": "id",
        "required_fields": ["sample_id"],
        "optional_fields": [
            "import_lab_result_raw_id", "ph_water", "ph_buffer",
            "organic_matter_percent", "phosphorus_kg_ha", "potassium_kg_ha",
            "calcium_kg_ha", "magnesium_kg_ha", "aluminum_ppm",
            "phosphorus_saturation_index", "cec_meq_100g", "boron_ppm",
            "manganese_ppm", "copper_ppm", "zinc_ppm", "iron_ppm", "sulfur_ppm",
        ],
    },
    "/soil-sampling/laboratories": {
        "label": "Laboratories",
        "id_field": "id",
        "required_fields": ["name", "code"],
        "optional_fields": ["address", "contact_email", "contact_phone", "country"],
    },
    "/soil-sampling/campaigns": {
        "label": "Campaigns",
        "id_field": "id",
        "required_fields": ["name"],
        "optional_fields": ["start_date", "end_date", "status", "source", "interpolation_params"],
    },
    "/soil-sampling/samples": {
        "label": "Samples",
        "id_field": "id",
        "required_fields": ["sampling_unit_id", "sampling_campaign_id"],
        "optional_fields": [
            "sample_label", "status", "depth_min", "depth_max",
            "sampled_at", "sent_to_lab_id", "sent_at", "tracking_number",
        ],
    },
    "/soil-sampling/units": {
        "label": "Sampling Units",
        "id_field": "id",
        "required_fields": ["unit_type", "geometry"],
        "optional_fields": ["parent_sampling_unit_id", "sample_unit_metadata"],
    },
}
