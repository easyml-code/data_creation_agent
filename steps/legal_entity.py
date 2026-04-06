"""
Step 2 — Legal Entity

Check LEGAL_ENTITY by buyer PAN (derived from buyer GSTIN).
  ├─ Found LE + LE Site  → return existing IDs
  ├─ Found LE only       → create LEGAL_ENTITY_SITE
  └─ Missing both        → create LEGAL_ENTITY then LEGAL_ENTITY_SITE

The unique key for a legal entity is the buyer PAN number.
"""

from api.client import get_object_id, get_records, create_record
from api.helpers import (
    pan_from_gstin, today,
    gen_legal_entity_id, gen_legal_entity_site_id,
    require, get_optional,
)
from lookups.master import lookup_country_id, lookup_state_id, lookup_currency_id


def handle_legal_entity(invoice_data: dict) -> dict:
    """
    Returns:
        {
            "legal_entity_id":      str,
            "legal_entity_site_id": str,
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

    name         = get_optional(buyer, "name") or "Buyer Company"
    building     = get_optional(buyer, "building_name") or "N/A"
    floor_unit   = get_optional(buyer, "floor_unit") or "N/A"
    currency     = get_optional(static, "currency") or "INR"

    oid_le  = get_object_id("LEGAL_ENTITY")
    oid_les = get_object_id("LEGAL_ENTITY_SITE")

    # ── 2a. Check LEGAL_ENTITY by PAN ────────────────────────────────────────
    existing_le = get_records(oid_le, table_name="LEGAL_ENTITY",
                              field="LEGAL_ENTITY_PAN", value=pan)

    if existing_le:
        le_id = existing_le[0]["legal_entity_id"]

        # ── 2b. Check if LEGAL_ENTITY_SITE exists for this LE ────────────────
        existing_les = get_records(oid_les, table_name="LEGAL_ENTITY_SITE",
                                   field="LEGAL_ENTITY_REF", value=le_id)
        if existing_les:
            return {
                "legal_entity_id":      le_id,
                "legal_entity_site_id": existing_les[0]["legal_entity_site_id"],
                "created":              False
            }
        # LE exists but no site — fall through to create site only
    else:
        # ── Create LEGAL_ENTITY ───────────────────────────────────────────────
        currency_id = lookup_currency_id(currency)
        le_id       = gen_legal_entity_id()

        create_record(oid_le, {
            "legal_entity_id":            le_id,
            "legal_entity_name":          name,
            "legal_entity_pan":           pan,
            "legal_entity_base_currency": currency_id,
            "effective_from":             today()
        }, table_name="LEGAL_ENTITY")

    # ── Create LEGAL_ENTITY_SITE ──────────────────────────────────────────────
    country_id = lookup_country_id(country_name)
    state_id   = lookup_state_id(state_name)
    le_site_id = gen_legal_entity_site_id()

    create_record(oid_les, {
        "legal_entity_site_id":   le_site_id,
        "gstin":                  gstin,
        "country_id":             country_id,
        "state_id":               state_id,
        "building_name":          building,
        "floor_unit":             floor_unit,
        "city":                   city,
        "pin_code":               str(pin_code),
        "default_shipping_flag":  True,
        "default_billing_flag":   True,
        "legal_entity_ref":       le_id,
        "effective_from":         today()
    }, table_name="LEGAL_ENTITY_SITE")

    return {
        "legal_entity_id":      le_id,
        "legal_entity_site_id": le_site_id,
        "created":              True
    }
