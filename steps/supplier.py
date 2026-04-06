"""
Step 1 — Supplier

Check SUPPLIER_SITE by GSTIN.
  ├─ Found    → return existing (supplier_uuid, site_uuid) from record ids
  └─ Missing  → create SUPPLIER first, then SUPPLIER_SITE

Cross-table refs must use the system UUID `id` field, not the human-readable supplier_id string.
"""

import logging
from api.client import get_object_id, get_records, create_record
from api.helpers import (
    pan_from_gstin, today,
    gen_supplier_id, gen_supplier_code, gen_supplier_site_id,
    require, get_optional,
)
from lookups.master import lookup_country_id, lookup_state_id

log = logging.getLogger(__name__)
STEP = "Step 1 | Supplier"


def handle_supplier(invoice_data: dict) -> dict:
    """
    Returns:
        {
            "supplier_id":      str  — UUID id of SUPPLIER record
            "supplier_site_id": str  — UUID id of SUPPLIER_SITE record
            "created":          bool
        }
    """
    sup = require(invoice_data, "static", "supplier_details",
                  label="static.supplier_details")

    gstin = require(sup, "gstin", label="static.supplier_details.gstin")
    gstin = gstin.strip().upper()

    pan          = pan_from_gstin(gstin)
    country_name = require(sup, "country",   label="static.supplier_details.country")
    state_name   = require(sup, "state",     label="static.supplier_details.state")
    city         = require(sup, "city",      label="static.supplier_details.city")
    pin_code     = require(sup, "pin_code",  label="static.supplier_details.pin_code")
    name         = get_optional(sup, "name") or "Unknown Supplier"
    building     = get_optional(sup, "building_name") or get_optional(sup, "address") or "N/A"
    floor_unit   = get_optional(sup, "floor_unit") or "N/A"

    # ── 1a. Check if SUPPLIER_SITE exists for this GSTIN ─────────────────────
    log.info("[%s] Checking SUPPLIER_SITE for GSTIN=%s", STEP, gstin)
    oid_site = get_object_id("SUPPLIER_SITE")
    existing = get_records(oid_site, table_name="SUPPLIER_SITE",
                           field="GSTIN", value=gstin)

    if existing:
        site          = existing[0]
        # supplier_ref in SUPPLIER_SITE is already the UUID id of SUPPLIER
        supplier_uuid = site["supplier_ref"]
        site_uuid     = site["id"]
        log.info("[%s] SUPPLIER_SITE found → supplier_uuid=%s, site_uuid=%s (no creation needed)",
                 STEP, supplier_uuid, site_uuid)
        return {
            "supplier_id":      supplier_uuid,
            "supplier_site_id": site_uuid,
            "created":          False
        }

    # ── 1b. SUPPLIER_SITE not found → check if SUPPLIER exists via PAN ───────
    log.info("[%s] SUPPLIER_SITE not found. Checking SUPPLIER by PAN=%s", STEP, pan)
    oid_sup           = get_object_id("SUPPLIER")
    existing_supplier = get_records(oid_sup, table_name="SUPPLIER",
                                    field="PAN_NUMBER", value=pan)

    if existing_supplier:
        supplier_uuid = existing_supplier[0]["id"]
        log.info("[%s] SUPPLIER found → supplier_uuid=%s", STEP, supplier_uuid)
    else:
        log.info("[%s] SUPPLIER not found. Creating new SUPPLIER (name=%s, pan=%s)", STEP, name, pan)
        supplier_code = gen_supplier_code(name)
        created_sup   = create_record(oid_sup, {
            "supplier_id":       gen_supplier_id(),
            "supplier_code":     supplier_code,
            "legal_name":        name,
            "pan_number":        pan,
            "supplier_class":    "PREFERRED",
            "pan_verified_flag": True,
            "supplier_type":     "COMPANY",
            "msme_flag":         False,
            "effective_from":    today()
        }, table_name="SUPPLIER")
        supplier_uuid = created_sup["id"]
        log.info("[%s] SUPPLIER created → supplier_uuid=%s", STEP, supplier_uuid)

    # ── 1c. Create SUPPLIER_SITE ──────────────────────────────────────────────
    log.info("[%s] Creating SUPPLIER_SITE (gstin=%s, city=%s, state=%s, country=%s)",
             STEP, gstin, city, state_name, country_name)
    country_id = lookup_country_id(country_name)
    state_id   = lookup_state_id(state_name)

    created_site = create_record(oid_site, {
        "supplier_site_id":        gen_supplier_site_id(),
        "supplier_legal_name_ref": name,
        "supplier_pan_ref":        pan,
        "gstin":                   gstin,
        "country_id":              country_id,
        "state_id":                state_id,
        "building_name":           building,
        "floor_unit":              floor_unit,
        "city":                    city,
        "pin_code":                str(pin_code),
        "sez_flag":                False,
        "supplier_ref":            supplier_uuid,   # UUID id of SUPPLIER
        "default_dispatch_flag":   True,
        "default_billing_flag":    True,
        "effective_from":          today()
    }, table_name="SUPPLIER_SITE")
    site_uuid = created_site["id"]
    log.info("[%s] SUPPLIER_SITE created → site_uuid=%s", STEP, site_uuid)

    return {
        "supplier_id":      supplier_uuid,
        "supplier_site_id": site_uuid,
        "created":          True
    }
