"""
Step 1 — Supplier

Check SUPPLIER_SITE by GSTIN.
  ├─ Found    → return existing (supplier_id, supplier_site_id)
  └─ Missing  → create SUPPLIER first, then SUPPLIER_SITE

The unique key for a supplier across all invoices is the GSTIN on the supplier site.
SUPPLIER itself is keyed by PAN (derived from GSTIN chars 3–12).
"""

from api.client import get_object_id, get_records, create_record
from api.helpers import (
    pan_from_gstin, today,
    gen_supplier_id, gen_supplier_code, gen_supplier_site_id,
    require, get_optional,
)
from api.exceptions import ValidationError
from lookups.master import lookup_country_id, lookup_state_id


def handle_supplier(invoice_data: dict) -> dict:
    """
    Returns:
        {
            "supplier_id":      str,
            "supplier_site_id": str,
            "created":          bool   # True if new records were created
        }
    """
    sup = require(invoice_data, "static", "supplier_details",
                  label="static.supplier_details")

    gstin = require(sup, "gstin", label="static.supplier_details.gstin")
    gstin = gstin.strip().upper()

    pan          = pan_from_gstin(gstin)
    country_name = require(sup, "country", label="static.supplier_details.country")
    state_name   = require(sup, "state",   label="static.supplier_details.state")
    city         = require(sup, "city",    label="static.supplier_details.city")
    pin_code     = require(sup, "pin_code",label="static.supplier_details.pin_code")
    name         = get_optional(sup, "name") or "Unknown Supplier"
    building     = get_optional(sup, "building_name") or get_optional(sup, "address") or "N/A"
    floor_unit   = get_optional(sup, "floor_unit") or "N/A"

    # ── 1a. Check if SUPPLIER_SITE already exists for this GSTIN ─────────────
    oid_site = get_object_id("SUPPLIER_SITE")
    existing = get_records(oid_site, table_name="SUPPLIER_SITE",
                           field="GSTIN", value=gstin)

    if existing:
        site          = existing[0]
        supplier_id   = site["SUPPLIER_REF"]
        site_id       = site["SUPPLIER_SITE_ID"]
        return {
            "supplier_id":      supplier_id,
            "supplier_site_id": site_id,
            "created":          False
        }

    # ── 1b. SUPPLIER_SITE not found → check if SUPPLIER exists via PAN ───────
    oid_sup          = get_object_id("SUPPLIER")
    existing_supplier = get_records(oid_sup, table_name="SUPPLIER",
                                    field="PAN_NUMBER", value=pan)

    if existing_supplier:
        supplier_id = existing_supplier[0]["SUPPLIER_ID"]
    else:
        # ── Create SUPPLIER ───────────────────────────────────────────────────
        supplier_id   = gen_supplier_id()
        supplier_code = gen_supplier_code(name)

        create_record(oid_sup, {
            "SUPPLIER_ID":       supplier_id,
            "SUPPLIER_CODE":     supplier_code,
            "LEGAL_NAME":        name,
            "PAN_NUMBER":        pan,
            "SUPPLIER_CLASS":    "PREFERRED",
            "PAN_VERIFIED_FLAG": True,
            "SUPPLIER_TYPE":     "COMPANY",
            "MSME_FLAG":         False,
            "EFFECTIVE_FROM":    today()
        }, table_name="SUPPLIER")

    # ── Create SUPPLIER_SITE ──────────────────────────────────────────────────
    country_id  = lookup_country_id(country_name)
    state_id    = lookup_state_id(state_name)
    site_id     = gen_supplier_site_id()

    create_record(oid_site, {
        "SUPPLIER_SITE_ID":        site_id,
        "SUPPLIER_LEGAL_NAME_REF": name,
        "SUPPLIER_PAN_REF":        pan,
        "GSTIN":                   gstin,
        "COUNTRY_ID":              country_id,
        "STATE_ID":                state_id,
        "BUILDING_NAME":           building,
        "FLOOR_UNIT":              floor_unit,
        "CITY":                    city,
        "PIN_CODE":                str(pin_code),
        "SEZ_FLAG":                False,
        "SUPPLIER_REF":            supplier_id,
        "DEFAULT_DISPATCH_FLAG":   True,
        "DEFAULT_BILLING_FLAG":    True,
        "EFFECTIVE_FROM":          today()
    }, table_name="SUPPLIER_SITE")

    return {
        "supplier_id":      supplier_id,
        "supplier_site_id": site_id,
        "created":          True
    }
