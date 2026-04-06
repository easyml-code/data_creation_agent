"""
Step 2 — Legal Entity

Check LEGAL_ENTITY by buyer PAN (derived from buyer GSTIN).
  ├─ Found LE + LE Site  → return existing UUIDs
  ├─ Found LE only       → create LEGAL_ENTITY_SITE
  └─ Missing both        → create LEGAL_ENTITY then LEGAL_ENTITY_SITE

Cross-table refs must use the system UUID `id` field, not the human-readable *_id string.
"""

import logging
from api.client import get_object_id, get_records, create_record
from api.helpers import (
    pan_from_gstin, today,
    gen_legal_entity_id, gen_legal_entity_site_id,
    require, get_optional,
)
from lookups.master import lookup_country_id, lookup_state_id, lookup_currency_id

log = logging.getLogger(__name__)
STEP = "Step 2 | Legal Entity"


def handle_legal_entity(invoice_data: dict) -> dict:
    """
    Returns:
        {
            "legal_entity_id":      str  — UUID id of LEGAL_ENTITY record
            "legal_entity_site_id": str  — UUID id of LEGAL_ENTITY_SITE record
            "created":              bool
        }
    """
    static = invoice_data["static"]
    buyer  = require(invoice_data, "static", "buyer_details",
                     label="static.buyer_details")

    gstin        = require(buyer, "gstin",    label="static.buyer_details.gstin")
    gstin        = gstin.strip().upper()
    pan          = pan_from_gstin(gstin)

    country_name = require(buyer, "country",  label="static.buyer_details.country")
    state_name   = require(buyer, "state",    label="static.buyer_details.state")
    city         = require(buyer, "city",     label="static.buyer_details.city")
    pin_code     = require(buyer, "pin_code", label="static.buyer_details.pin_code")

    name       = get_optional(buyer, "name") or "Buyer Company"
    building   = get_optional(buyer, "building_name") or "N/A"
    floor_unit = get_optional(buyer, "floor_unit") or "N/A"
    currency   = get_optional(static, "currency") or "INR"

    oid_le  = get_object_id("LEGAL_ENTITY")
    oid_les = get_object_id("LEGAL_ENTITY_SITE")

    # ── 2a. Check LEGAL_ENTITY by PAN ────────────────────────────────────────
    log.info("[%s] Checking LEGAL_ENTITY by PAN=%s", STEP, pan)
    existing_le = get_records(oid_le, table_name="LEGAL_ENTITY",
                              field="LEGAL_ENTITY_PAN", value=pan)

    if existing_le:
        le_uuid = existing_le[0]["id"]
        log.info("[%s] LEGAL_ENTITY found → le_uuid=%s", STEP, le_uuid)

        # ── 2b. Check if LEGAL_ENTITY_SITE exists for this LE ────────────────
        log.info("[%s] Checking LEGAL_ENTITY_SITE for le_uuid=%s", STEP, le_uuid)
        existing_les = get_records(oid_les, table_name="LEGAL_ENTITY_SITE",
                                   field="LEGAL_ENTITY_REF", value=le_uuid)
        if existing_les:
            le_site_uuid = existing_les[0]["id"]
            log.info("[%s] LEGAL_ENTITY_SITE found → le_site_uuid=%s (no creation needed)",
                     STEP, le_site_uuid)
            return {
                "legal_entity_id":      le_uuid,
                "legal_entity_site_id": le_site_uuid,
                "created":              False
            }
        log.info("[%s] LEGAL_ENTITY_SITE not found — will create site only", STEP)
    else:
        # ── Create LEGAL_ENTITY ───────────────────────────────────────────────
        log.info("[%s] LEGAL_ENTITY not found. Creating (name=%s, pan=%s)", STEP, name, pan)
        currency_id = lookup_currency_id(currency)
        created_le  = create_record(oid_le, {
            "legal_entity_id":            gen_legal_entity_id(),
            "legal_entity_name":          name,
            "legal_entity_pan":           pan,
            "legal_entity_base_currency": currency_id,
            "effective_from":             today()
        }, table_name="LEGAL_ENTITY")
        le_uuid = created_le["id"]
        log.info("[%s] LEGAL_ENTITY created → le_uuid=%s", STEP, le_uuid)

    # ── Create LEGAL_ENTITY_SITE ──────────────────────────────────────────────
    log.info("[%s] Creating LEGAL_ENTITY_SITE (gstin=%s, city=%s, state=%s, country=%s)",
             STEP, gstin, city, state_name, country_name)
    country_id   = lookup_country_id(country_name)
    state_id     = lookup_state_id(state_name)
    created_les  = create_record(oid_les, {
        "legal_entity_site_id":   gen_legal_entity_site_id(),
        "gstin":                  gstin,
        "country_id":             country_id,
        "state_id":               state_id,
        "building_name":          building,
        "floor_unit":             floor_unit,
        "city":                   city,
        "pin_code":               str(pin_code),
        "default_shipping_flag":  True,
        "default_billing_flag":   True,
        "legal_entity_ref":       le_uuid,          # UUID id of LEGAL_ENTITY
        "effective_from":         today()
    }, table_name="LEGAL_ENTITY_SITE")
    le_site_uuid = created_les["id"]
    log.info("[%s] LEGAL_ENTITY_SITE created → le_site_uuid=%s", STEP, le_site_uuid)

    return {
        "legal_entity_id":      le_uuid,
        "legal_entity_site_id": le_site_uuid,
        "created":              True
    }
