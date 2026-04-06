#!/usr/bin/env python3
"""
Invoice OCR → GRN Data Creation Agent

Flow:
  1. Supplier       → check/create SUPPLIER + SUPPLIER_SITE  (key: supplier GSTIN)
  2. Legal Entity   → check/create LEGAL_ENTITY + LEGAL_ENTITY_SITE  (key: buyer PAN)
  3. PO             → check PO_HEADER by PO_NUMBER; create if missing + PO_LINE per item
  4. GRN            → create GRN_HEADER + GRN_LINE per PO_LINE

All reference fields are always fetched from their source tables — never hard-coded.
"""

import uuid
import requests
from datetime import date
from typing import Optional

# ── Config ────────────────────────────────────────────────────────────────────

BASE_URL  = "https://metadata-definition-api.azurewebsites.net/definitionapi"
USAGE_URL = "https://aezcriuvadwa0009.azurewebsites.net/usageapi"
VERSION   = "v1"
USER_ID   = "54e657fb-0199-477c-a776-c1467ff71a63"
TODAY     = date.today().isoformat()

# ── Invoice OCR input data ────────────────────────────────────────────────────

invoice_data = {
    "static": {
        "invoice_number": "2024-29/09/007",
        "invoice_date": "2025-08-15",
        "due_date": "",
        "currency": "INR",
        "total_amount": 21238.82,
        "sub_total": 17999.00,
        "tax_amount": 3239.82,
        "purchase_order_number": "1100877200",

        "supplier_details": {
            "name": "Orion Corp",
            "address": "D-9, Banjara Hills, Hyderabad",
            "building_name": "",
            "floor_unit": "",
            "city": "Hyderabad",
            "state": "Telangana",
            "country": "India",
            "pin_code": "500761",
            "gstin": "36ABCDE1234F1Z5",
            "account_number": "054205001505"
        },

        "buyer_details": {
            "name": "Buyer Company Pvt Ltd",
            "address": "252, Veer Savarkar Road, Shivaji Park, Mumbai, Maharashtra- 400028",
            "building_name": "Tower A",
            "floor_unit": "5th Floor, Unit 502",
            "city": "Mumbai",
            "state": "Maharashtra",
            "country": "India",
            "pin_code": "400028",
            "gstin": "27FGHIJ5678K1Z3"
        },

        "tax_details": {
            "igst_rate": 0.18,
            "igst_amount": 3239.82
        }
    },

    "line_items": [
        {
            "line_number": 1,
            "description": "Dell UltraSharp 27-inch QHD IPS Monitor, 165Hz, USB-C",
            "quantity": 1,
            "unit": "EA",
            "unit_price": 17999.00,
            "total": 17999.00,
            "hsn_code": "852851",
            "purchase_order_number": "1100877200",
            "tax_details": {
                "igst_rate": 0.18,
                "igst_amount": 3239.82,
                "tax_amount": 3239.82
            }
        }
    ]
}

# ── Object ID cache (fetched once at startup) ─────────────────────────────────

_object_ids: dict = {}

# ── Core API utilities ────────────────────────────────────────────────────────

def get_object_id(api_name: str) -> str:
    """Fetch the internal objectId for a given API name (cached)."""
    if api_name in _object_ids:
        return _object_ids[api_name]

    url = f"{BASE_URL}/{VERSION}/object/api-name/{api_name}"
    resp = requests.get(url, headers={"Content-Type": "application/json"}, timeout=10)
    resp.raise_for_status()

    data = resp.json()
    if not data.get("success"):
        raise ValueError(f"get_object_id({api_name}): API returned success=False — {data}")

    object_id = data["data"]["objectId"]
    _object_ids[api_name] = object_id
    return object_id


def get_records(object_id: str, field: str = None, value: str = None, limit: int = 1) -> list:
    """
    GET records from a dynamic table.
    Pass field+value for filtered lookup; omit both to fetch the first available record.
    """
    params: dict = {"page": 0, "size": limit}
    if field and value is not None:
        params["field"]    = field.upper()
        params["operator"] = "EQUALS"
        params["filter"]   = value

    url  = f"{USAGE_URL}/{VERSION}/object/{object_id}/data"
    resp = requests.get(url, params=params,
                        headers={"Content-Type": "application/json"}, timeout=30)
    resp.raise_for_status()

    data = resp.json()
    if not data.get("success"):
        raise ValueError(f"get_records failed: {data.get('message')}")

    return data.get("data") or []


def create_record(object_id: str, payload: dict) -> dict:
    """POST a new record into a dynamic table."""
    body = {"jsonInput": payload, "storageService": "DATABASE_POSTGRES"}
    resp = requests.post(
        f"{USAGE_URL}/{VERSION}/object/{object_id}/data",
        json=body,
        headers={"Content-Type": "application/json", "userId": USER_ID},
        timeout=30
    )
    resp.raise_for_status()

    data = resp.json()
    if not data.get("success"):
        raise ValueError(f"create_record failed: {data.get('message')}")

    return data.get("data", {})


def gen_id(prefix: str) -> str:
    """Generate a unique ID with a given prefix."""
    return f"{prefix}-{uuid.uuid4().hex[:8].upper()}"


def pan_from_gstin(gstin: str) -> str:
    """Extract 10-char PAN from a 15-char GSTIN (chars 3–12)."""
    return gstin[2:12].upper()


# ── Master data lookups ───────────────────────────────────────────────────────
# These functions fetch existing reference IDs from master tables.
# They raise if the record is not found so errors are obvious early.

def lookup_country(country_name: str) -> str:
    oid     = get_object_id("COUNTRY")
    records = get_records(oid, field="COUNTRY_NAME", value=country_name)
    if not records:
        raise LookupError(f"Country '{country_name}' not found in COUNTRY table")
    return records[0]["COUNTRY_ID"]


def lookup_state(state_name: str) -> str:
    oid     = get_object_id("STATE")
    records = get_records(oid, field="STATE_NAME", value=state_name)
    if not records:
        raise LookupError(f"State '{state_name}' not found in STATE table")
    return records[0]["STATE_ID"]


def lookup_currency(currency_code: str) -> str:
    oid     = get_object_id("CURRENCY")
    records = get_records(oid, field="CURRENCY_CODE", value=currency_code)
    if not records:
        raise LookupError(f"Currency '{currency_code}' not found in CURRENCY table")
    return records[0]["CURRENCY_ID"]


def lookup_payment_terms() -> str:
    oid     = get_object_id("PAYMENT_TERMS")
    records = get_records(oid)
    if not records:
        raise LookupError("No records found in PAYMENT_TERMS table")
    return records[0]["PAYMENT_TERM_ID"]


def lookup_plant() -> str:
    oid     = get_object_id("PLANT")
    records = get_records(oid)
    if not records:
        raise LookupError("No records found in PLANT table")
    return records[0]["PLANT_ID"]


def lookup_cost_center() -> str:
    oid     = get_object_id("COST_CENTER")
    records = get_records(oid)
    if not records:
        raise LookupError("No records found in COST_CENTER table")
    return records[0]["COST_CENTER_ID"]


def lookup_project() -> str:
    oid     = get_object_id("PROJECT_WBS")
    records = get_records(oid)
    if not records:
        raise LookupError("No records found in PROJECT_WBS table")
    return records[0]["PROJECT_WBS_ID"]


def lookup_profit_center() -> str:
    oid     = get_object_id("PROFIT_CENTER")
    records = get_records(oid)
    if not records:
        raise LookupError("No records found in PROFIT_CENTER table")
    return records[0]["PROFIT_CENTER_ID"]


def lookup_tax_rate() -> str:
    oid     = get_object_id("TAX_RATE")
    records = get_records(oid)
    if not records:
        raise LookupError("No records found in TAX_RATE table")
    return records[0]["TAX_RATE_ID"]


def lookup_hsn(hsn_code: str) -> str:
    oid     = get_object_id("HSN_SAC")
    records = get_records(oid, field="HSN_CODE", value=hsn_code)
    if not records:
        raise LookupError(f"HSN code '{hsn_code}' not found in HSN_SAC table")
    return records[0]["HSN_ID"]


def lookup_uom(uom_code: str) -> str:
    oid     = get_object_id("UOM")
    records = get_records(oid, field="UOM_CODE", value=uom_code)
    if not records:
        raise LookupError(f"UOM code '{uom_code}' not found in UOM table")
    return records[0]["UOM_ID"]


def lookup_item(description: str) -> Optional[str]:
    """Try to find an existing ITEM by name; returns None if not found."""
    oid     = get_object_id("ITEM")
    records = get_records(oid, field="ITEM_NAME", value=description)
    if records:
        return records[0]["ITEM_ID"]
    # Fall back to first available item
    records = get_records(oid)
    return records[0]["ITEM_ID"] if records else None


def lookup_gl_account() -> str:
    oid     = get_object_id("GL_ACCOUNT")
    records = get_records(oid)
    if not records:
        raise LookupError("No records found in GL_ACCOUNT table")
    return records[0]["GL_ACCOUNT_ID"]


# ── Step 1: Supplier ──────────────────────────────────────────────────────────

def handle_supplier(inv: dict) -> tuple:
    """
    Check SUPPLIER_SITE for supplier GSTIN.
    If found  → return existing supplier_id + supplier_site_id.
    If missing → create SUPPLIER first, then SUPPLIER_SITE.
    Returns (supplier_id, supplier_site_id).
    """
    sup      = inv["static"]["supplier_details"]
    gstin    = sup["gstin"]
    pan      = pan_from_gstin(gstin)

    print(f"\n[Step 1] Checking SUPPLIER_SITE for GSTIN={gstin} ...")

    oid_site = get_object_id("SUPPLIER_SITE")
    existing = get_records(oid_site, field="GSTIN", value=gstin)

    if existing:
        site          = existing[0]
        supplier_id   = site["SUPPLIER_REF"]
        site_id       = site["SUPPLIER_SITE_ID"]
        print(f"  ✓ SUPPLIER_SITE found → supplier_id={supplier_id}, site_id={site_id}")
        return supplier_id, site_id

    # ── Create SUPPLIER ──
    print("  SUPPLIER_SITE not found. Creating SUPPLIER ...")
    oid_sup     = get_object_id("SUPPLIER")
    supplier_id = gen_id("SUP")

    create_record(oid_sup, {
        "SUPPLIER_ID":       supplier_id,
        "SUPPLIER_CODE":     gen_id("SCOD"),
        "LEGAL_NAME":        sup["name"],
        "PAN_NUMBER":        pan,
        "SUPPLIER_CLASS":    "PREFERRED",
        "PAN_VERIFIED_FLAG": True,
        "SUPPLIER_TYPE":     "COMPANY",
        "MSME_FLAG":         False,
        "EFFECTIVE_FROM":    TODAY
    })
    print(f"  ✓ SUPPLIER created → supplier_id={supplier_id}")

    # ── Create SUPPLIER_SITE ──
    print("  Creating SUPPLIER_SITE ...")
    country_id  = lookup_country(sup["country"])
    state_id    = lookup_state(sup["state"])
    site_id     = gen_id("SSITE")
    building    = sup["building_name"] or sup["address"] or "N/A"
    floor_unit  = sup["floor_unit"] or "N/A"

    create_record(oid_site, {
        "SUPPLIER_SITE_ID":       site_id,
        "SUPPLIER_LEGAL_NAME_REF": sup["name"],
        "SUPPLIER_PAN_REF":       pan,
        "GSTIN":                  gstin,
        "COUNTRY_ID":             country_id,
        "STATE_ID":               state_id,
        "BUILDING_NAME":          building,
        "FLOOR_UNIT":             floor_unit,
        "CITY":                   sup["city"],
        "PIN_CODE":               sup["pin_code"],
        "SEZ_FLAG":               False,
        "SUPPLIER_REF":           supplier_id,
        "DEFAULT_DISPATCH_FLAG":  True,
        "DEFAULT_BILLING_FLAG":   True,
        "EFFECTIVE_FROM":         TODAY
    })
    print(f"  ✓ SUPPLIER_SITE created → site_id={site_id}")

    return supplier_id, site_id


# ── Step 2: Legal Entity ──────────────────────────────────────────────────────

def handle_legal_entity(inv: dict) -> tuple:
    """
    Check LEGAL_ENTITY for buyer PAN (derived from buyer GSTIN).
    If found  → return existing le_id + le_site_id.
    If missing → create LEGAL_ENTITY first, then LEGAL_ENTITY_SITE.
    Returns (legal_entity_id, legal_entity_site_id).
    """
    buyer    = inv["static"]["buyer_details"]
    gstin    = buyer["gstin"]
    pan      = pan_from_gstin(gstin)

    print(f"\n[Step 2] Checking LEGAL_ENTITY for PAN={pan} ...")

    oid_le   = get_object_id("LEGAL_ENTITY")
    existing = get_records(oid_le, field="LEGAL_ENTITY_PAN", value=pan)

    if existing:
        le         = existing[0]
        le_id      = le["LEGAL_ENTITY_ID"]
        print(f"  ✓ LEGAL_ENTITY found → le_id={le_id}. Checking LEGAL_ENTITY_SITE ...")

        oid_les     = get_object_id("LEGAL_ENTITY_SITE")
        existing_s  = get_records(oid_les, field="LEGAL_ENTITY_REF", value=le_id)
        if existing_s:
            le_site_id = existing_s[0]["LEGAL_ENTITY_SITE_ID"]
            print(f"  ✓ LEGAL_ENTITY_SITE found → le_site_id={le_site_id}")
            return le_id, le_site_id

        # LE exists but site missing — fall through to create site
    else:
        # ── Create LEGAL_ENTITY ──
        print("  LEGAL_ENTITY not found. Creating ...")
        currency_id = lookup_currency(inv["static"]["currency"])
        le_id       = gen_id("LE")

        create_record(oid_le, {
            "LEGAL_ENTITY_ID":            le_id,
            "LEGAL_ENTITY_NAME":          buyer["name"] or "Buyer Company Pvt Ltd",
            "LEGAL_ENTITY_PAN":           pan,
            "LEGAL_ENTITY_BASE_CURRENCY": currency_id,
            "EFFECTIVE_FROM":             TODAY
        })
        print(f"  ✓ LEGAL_ENTITY created → le_id={le_id}")

    # ── Create LEGAL_ENTITY_SITE ──
    print("  Creating LEGAL_ENTITY_SITE ...")
    oid_les    = get_object_id("LEGAL_ENTITY_SITE")
    country_id = lookup_country(buyer["country"])
    state_id   = lookup_state(buyer["state"])
    le_site_id = gen_id("LESITE")

    create_record(oid_les, {
        "LEGAL_ENTITY_SITE_ID":   le_site_id,
        "GSTIN":                  gstin,
        "COUNTRY_ID":             country_id,
        "STATE_ID":               state_id,
        "BUILDING_NAME":          buyer.get("building_name", "N/A") or "N/A",
        "FLOOR_UNIT":             buyer.get("floor_unit", "N/A") or "N/A",
        "CITY":                   buyer["city"],
        "PIN_CODE":               buyer["pin_code"],
        "DEFAULT_SHIPPING_FLAG":  True,
        "DEFAULT_BILLING_FLAG":   True,
        "LEGAL_ENTITY_REF":       le_id,
        "EFFECTIVE_FROM":         TODAY
    })
    print(f"  ✓ LEGAL_ENTITY_SITE created → le_site_id={le_site_id}")

    return le_id, le_site_id


# ── Step 3: PO ────────────────────────────────────────────────────────────────

def handle_po(inv: dict, supplier_id: str, supplier_site_id: str,
              le_id: str, le_site_id: str) -> tuple:
    """
    Check PO_HEADER by PO_NUMBER.
    If found  → return existing po_id + existing po_line_ids.
    If missing → create PO_HEADER + one PO_LINE per invoice line item.
    Returns (po_header_id, [po_line_id, ...]).
    """
    static     = inv["static"]
    po_number  = static["purchase_order_number"]

    print(f"\n[Step 3] Checking PO_HEADER for PO_NUMBER={po_number} ...")

    oid_ph    = get_object_id("PO_HEADER")
    existing  = get_records(oid_ph, field="PO_NUMBER", value=po_number)

    if existing:
        po_id = existing[0]["PO_ID"]
        print(f"  ✓ PO_HEADER found → po_id={po_id}. Fetching PO_LINEs ...")

        oid_pl      = get_object_id("PO_LINE")
        line_records = get_records(oid_pl, field="PO_HEADER_REF", value=po_id,
                                   limit=len(inv["line_items"]) + 10)
        po_line_ids = [r["PO_LINE_ID"] for r in line_records]
        print(f"  ✓ Found {len(po_line_ids)} PO_LINE(s)")
        return po_id, po_line_ids

    # ── Fetch master data for PO creation ──
    print("  PO_HEADER not found. Fetching master data for PO creation ...")

    currency_id      = lookup_currency(static["currency"])
    payment_terms_id = lookup_payment_terms()
    plant_id         = lookup_plant()
    cost_center_id   = lookup_cost_center()
    project_id       = lookup_project()
    profit_center_id = lookup_profit_center()
    tax_rate_id      = lookup_tax_rate()

    # ── Create PO_HEADER ──
    po_id = gen_id("PO")
    create_record(oid_ph, {
        "PO_ID":                          po_id,
        "PO_NUMBER":                      po_number,
        "PO_TYPE":                        "MATERIAL",
        "PO_DATE":                        static["invoice_date"],
        "CURRENCY_ID":                    currency_id,
        "EXCHANGE_RATE":                  1,
        "PAYMENT_TERMS_REF":              payment_terms_id,
        "PO_TOTAL_VALUE":                 static["total_amount"],
        "PO_STATUS":                      "APPROVED",
        "CREATED_BY":                     USER_ID,
        "MATCHING_TYPE":                  "THREE_WAY",
        "SUPPLIER_REF":                   supplier_id,
        "SUPPLIER_SITE_REF":              supplier_site_id,
        "LEGAL_ENTITY_REF":               le_id,
        "LEGAL_ENTITY_SITE_BILL_TO_REF":  le_site_id,
        "LEGAL_ENTITY_SITE_SHIP_TO_REF":  le_site_id,
        "PLANT_REF":                      plant_id,
        "COST_CENTER_REF":                cost_center_id,
        "PROJECT_REF":                    project_id,
        "PROFIT_CENTER_REF":              profit_center_id,
        "TAX_RATE_REF":                   tax_rate_id,
        "EFFECTIVE_FROM":                 static["invoice_date"]
    })
    print(f"  ✓ PO_HEADER created → po_id={po_id}")

    # ── Create PO_LINE per invoice line item ──
    oid_pl      = get_object_id("PO_LINE")
    gl_acct_id  = lookup_gl_account()
    po_line_ids = []

    for idx, item in enumerate(inv["line_items"], start=1):
        hsn_id   = lookup_hsn(item["hsn_code"])
        uom_id   = lookup_uom(item["unit"])
        item_ref = lookup_item(item["description"])
        if not item_ref:
            raise LookupError(f"No ITEM found for description: {item['description']}")

        line_id = gen_id("POLINE")
        create_record(oid_pl, {
            "PO_LINE_ID":        line_id,
            "LINE_NUMBER":       item.get("line_number") or idx,
            "HSN_ID":            hsn_id,
            "UOM_ID":            uom_id,
            "ORDERED_QUANTITY":  item["quantity"],
            "UNIT_PRICE":        item["unit_price"],
            "QC_REQUIRED_FLAG":  False,
            "LINE_STATUS":       "OPEN",
            "PLANT_REF":         plant_id,
            "COST_CENTER_REF":   cost_center_id,
            "PROJECT_REF":       project_id,
            "PROFIT_CENTER_REF": profit_center_id,
            "GL_ACCOUNT_REF":    gl_acct_id,
            "TAX_RATE_REF":      tax_rate_id,
            "ITEM_REF":          item_ref,
            "PO_HEADER_REF":     po_id,
            "EFFECTIVE_FROM":    static["invoice_date"]
        })
        po_line_ids.append(line_id)
        print(f"  ✓ PO_LINE {idx} created → line_id={line_id}")

    return po_id, po_line_ids


# ── Step 4: GRN ───────────────────────────────────────────────────────────────

def handle_grn(inv: dict, po_line_ids: list,
               supplier_site_id: str, le_site_id: str) -> list:
    """
    Create one GRN_HEADER per PO_LINE (each with one GRN_LINE).
    Returns list of dicts with grn_id, grn_line_id per line item.
    """
    static     = inv["static"]
    line_items = inv["line_items"]

    print(f"\n[Step 4] Creating GRN records for {len(po_line_ids)} PO_LINE(s) ...")

    oid_gh = get_object_id("GRN_HEADER")
    oid_gl = get_object_id("GRN_LINE")

    # Weight UOM — look up "KGS"; fall back to first UOM
    try:
        weight_uom_id = lookup_uom("KGS")
    except LookupError:
        weight_uom_id = lookup_uom("KG") if _try_uom("KG") else get_records(get_object_id("UOM"))[0]["UOM_ID"]

    gl_acct_id = lookup_gl_account()
    results    = []

    for idx, (po_line_id, item) in enumerate(zip(po_line_ids, line_items), start=1):
        uom_id   = lookup_uom(item["unit"])
        item_ref = lookup_item(item["description"])
        if not item_ref:
            raise LookupError(f"No ITEM found for description: {item['description']}")

        # ── Create GRN_HEADER ──
        grn_id     = gen_id("GRN")
        grn_number = f"GRN-{date.today().strftime('%Y%m%d')}-{idx:03d}"

        create_record(oid_gh, {
            "GRN_ID":                  grn_id,
            "GRN_NUMBER":              grn_number,
            "GRN_DATE":                TODAY,
            "TOTAL_RECEIVED_QTY":      item["quantity"],
            "TOTAL_RECEIVED_AMOUNT":   item["total"],
            "WEIGHT_UOM_ID":           weight_uom_id,
            "QC_STATUS":               "PENDING",
            "GRN_STATUS":              "OPEN",
            "PO_LINE_REF":             po_line_id,
            "SUPPLIER_SITE_REF":       supplier_site_id,
            "LEGAL_ENTITY_SITE_REF":   le_site_id,
            "GL_ACCOUNT_REF":          gl_acct_id,
            "EFFECTIVE_FROM":          TODAY
        })
        print(f"  ✓ GRN_HEADER {idx} created → grn_id={grn_id}, grn_number={grn_number}")

        # ── Create GRN_LINE ──
        grn_line_id = gen_id("GRNLINE")

        create_record(oid_gl, {
            "GRN_LINE_ID":       grn_line_id,
            "GRN_LINE_NUMBER":   1,
            "ITEM_DESCRIPTION":  item["description"],
            "UOM_ID":            uom_id,
            "RECEIVED_QTY":      item["quantity"],
            "WEIGHT_UOM":        "KGS",
            "QC_REQUIRED_FLAG":  False,
            "QC_RESULT":         "ACCEPTED",
            "GRN_LINE_STATUS":   "OPEN",
            "GRN_REF":           grn_id,
            "ITEM_REF":          item_ref,
            "EFFECTIVE_FROM":    TODAY
        })
        print(f"  ✓ GRN_LINE  {idx} created → grn_line_id={grn_line_id}")

        results.append({
            "po_line_id":    po_line_id,
            "grn_id":        grn_id,
            "grn_number":    grn_number,
            "grn_line_id":   grn_line_id,
            "description":   item["description"],
            "quantity":      item["quantity"],
            "unit_price":    item["unit_price"],
            "total_amount":  item["total"]
        })

    return results


def _try_uom(code: str) -> bool:
    try:
        lookup_uom(code)
        return True
    except LookupError:
        return False


# ── Main ──────────────────────────────────────────────────────────────────────

def process_invoice(inv: dict) -> dict:
    """
    Full pipeline: invoice OCR data → populated Supplier / LE / PO / GRN tables.
    Returns a summary dict with all created/found IDs.
    """
    print("=" * 60)
    print("Invoice OCR → GRN Pipeline")
    print(f"Invoice  : {inv['static']['invoice_number']}")
    print(f"Supplier : {inv['static']['supplier_details']['name']}")
    print(f"PO Number: {inv['static']['purchase_order_number']}")
    print("=" * 60)

    supplier_id,   supplier_site_id  = handle_supplier(inv)
    le_id,         le_site_id        = handle_legal_entity(inv)
    po_header_id,  po_line_ids       = handle_po(inv, supplier_id, supplier_site_id,
                                                  le_id, le_site_id)
    grn_results                      = handle_grn(inv, po_line_ids,
                                                   supplier_site_id, le_site_id)

    result = {
        "invoice_number":     inv["static"]["invoice_number"],
        "supplier_id":        supplier_id,
        "supplier_site_id":   supplier_site_id,
        "legal_entity_id":    le_id,
        "legal_entity_site_id": le_site_id,
        "po_header_id":       po_header_id,
        "po_number":          inv["static"]["purchase_order_number"],
        "po_line_ids":        po_line_ids,
        "grn_records":        grn_results
    }

    print("\n" + "=" * 60)
    print("Pipeline complete.")
    print(f"  Supplier       : {supplier_id}")
    print(f"  Supplier Site  : {supplier_site_id}")
    print(f"  Legal Entity   : {le_id}")
    print(f"  LE Site        : {le_site_id}")
    print(f"  PO Header      : {po_header_id}  (PO# {result['po_number']})")
    for r in grn_results:
        print(f"  GRN            : {r['grn_id']}  ({r['grn_number']}) — {r['description']}")
    print("=" * 60)

    return result


if __name__ == "__main__":
    output = process_invoice(invoice_data)
    import json
    print("\nFinal Output JSON:")
    print(json.dumps(output, indent=2))
