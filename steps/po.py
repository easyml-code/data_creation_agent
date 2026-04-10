"""
Step 3 — Purchase Order

Check PO_HEADER by PO_NUMBER.
  ├─ Found  → fetch existing PO_LINEs and return them (with UUID ids)
  └─ Missing → create PO_HEADER + one PO_LINE per invoice line item

Cross-table refs must use the system UUID `id` field, not human-readable *_id strings.
EXCEPTION: currency_id, payment_terms_ref, hsn_id, uom_id use string *_id values.
"""

import logging
from api.client import get_object_id, get_records, create_record
from api.helpers import (
    today, gen_po_id, gen_po_line_id,
    require, get_optional,
)
import config
from lookups.master import (
    lookup_currency_id,
    lookup_payment_term_id,
    lookup_plant_id,
    lookup_cost_center_id,
    lookup_project_id,
    lookup_profit_center_id,
    lookup_tax_rate_id,
    lookup_gl_account_id,
    lookup_hsn_id,
    lookup_uom_id,
    lookup_item_id,
)

log = logging.getLogger(__name__)
STEP = "Step 3 | PO"


def handle_po(
    invoice_data: dict,
    supplier_id: str,           # UUID id of SUPPLIER
    supplier_site_id: str,      # UUID id of SUPPLIER_SITE
    legal_entity_id: str,       # UUID id of LEGAL_ENTITY
    legal_entity_site_id: str,  # UUID id of LEGAL_ENTITY_SITE
) -> dict:
    """
    Returns:
        {
            "po_header_id": str  — UUID id of PO_HEADER record
            "po_number":    str
            "po_lines": [
                {
                    "po_line_id":  str   — UUID id of PO_LINE record
                    "line_number": int,
                    "description": str,
                    "quantity":    float,
                    "unit_price":  float,
                    "hsn_code":    str,
                    "uom_code":    str,
                }
            ],
            "created": bool
        }
    """
    static    = invoice_data["static"]
    po_number = require(invoice_data, "static", "purchase_order_number",
                        label="static.purchase_order_number")
    po_number = str(po_number).strip()

    line_items = invoice_data.get("line_items") or []
    if not line_items:
        raise ValueError("invoice_data.line_items is empty — cannot create PO lines")

    oid_ph = get_object_id("PO_HEADER")

    # ── 3a. Check if PO already exists ───────────────────────────────────────
    log.info("[%s] Checking PO_HEADER for PO_NUMBER=%s", STEP, po_number)
    existing = get_records(oid_ph, table_name="PO_HEADER",
                           field="PO_NUMBER", value=po_number)

    if existing:
        po_uuid = existing[0]["id"]
        log.info("[%s] PO_HEADER found → po_uuid=%s. Fetching PO_LINEs...", STEP, po_uuid)

        oid_pl  = get_object_id("PO_LINE")
        po_rows = get_records(oid_pl, table_name="PO_LINE",
                              field="PO_HEADER_REF", value=po_uuid,
                              limit=len(line_items) + 20)
        log.info("[%s] Fetched %d existing PO_LINE(s) for po_uuid=%s", STEP, len(po_rows), po_uuid)

        # Build matched po_lines — try to match each invoice line to an existing PO line.
        # Match key: hsn_id + quantity + unit_price (all 3 must match).
        # Unmatched invoice lines → create new PO_LINE.

        # Index existing lines by (hsn_id, qty, unit_price) for O(1) lookup
        existing_index: dict[tuple, dict] = {}
        for r in po_rows:
            key = (
                str(r.get("hsn_id", "")).strip(),
                float(r.get("ordered_quantity") or 0),
                float(r.get("unit_price") or 0),
            )
            existing_index[key] = r

        po_lines   = []
        new_lines  = []   # invoice items with no matching PO line

        for idx, item in enumerate(line_items, start=1):
            hsn_code   = str(item.get("hsn_code") or "").strip()
            qty        = float(item.get("quantity") or 0)
            unit_price = float(item.get("unit_price") or 0)
            desc       = item.get("description") or f"Line {idx}"
            uom_code   = str(item.get("unit") or "EA").strip().upper()
            line_num   = item.get("line_number") or idx

            if not hsn_code:
                raise ValueError(
                    f"line_items[{idx}].hsn_code is missing — required to match/create PO line"
                )

            hsn_id = lookup_hsn_id(hsn_code)   # string hsn_id
            key    = (hsn_id, qty, unit_price)
            match  = existing_index.get(key)

            if match:
                log.info("[%s] Invoice line %d matched existing PO_LINE uuid=%s "
                         "(hsn=%s, qty=%s, price=%s, desc='%s')",
                         STEP, idx, match["id"], hsn_id, qty, unit_price, desc)
                po_lines.append({
                    "po_line_id":  match["id"],
                    "line_number": match.get("line_number"),
                    "description": match.get("item_description", desc),
                    "quantity":    qty,
                    "unit_price":  unit_price,
                    "uom_id":      match.get("uom_id", ""),
                })
            else:
                log.info("[%s] Invoice line %d has NO matching PO_LINE "
                         "(hsn=%s, qty=%s, price=%s, desc='%s') — will create",
                         STEP, idx, hsn_id, qty, unit_price, desc)
                new_lines.append({
                    "idx":       idx,
                    "hsn_code":  hsn_code,
                    "hsn_id":    hsn_id,
                    "uom_code":  uom_code,
                    "desc":      desc,
                    "qty":       qty,
                    "unit_price": unit_price,
                    "line_num":  line_num,
                })

        # Create missing PO lines — fetch master refs only if needed
        if new_lines:
            log.info("[%s] %d new PO_LINE(s) to create for existing PO", STEP, len(new_lines))
            po_date          = get_optional(static, "invoice_date") or today()
            plant_uuid       = lookup_plant_id()
            cost_center_uuid = lookup_cost_center_id()
            project_uuid     = lookup_project_id()
            profit_center_uuid = lookup_profit_center_id()
            tax_rate_uuid    = lookup_tax_rate_id()
            gl_uuid          = lookup_gl_account_id()

            for nl in new_lines:
                uom_id    = lookup_uom_id(nl["uom_code"])
                item_uuid = lookup_item_id(nl["desc"])
                log.info("[%s] Creating new PO_LINE (hsn=%s, qty=%s, price=%s, desc='%s')",
                         STEP, nl["hsn_id"], nl["qty"], nl["unit_price"], nl["desc"])

                line_amt = nl["qty"] * nl["unit_price"]
                created_pl = create_record(oid_pl, {
                    "po_line_id":        gen_po_line_id(),
                    "line_number":       int(nl["line_num"]),
                    "hsn_id":            nl["hsn_id"],
                    "uom_id":            uom_id,
                    "ordered_quantity":  nl["qty"],
                    "unit_price":        nl["unit_price"],
                    "line_amount":       line_amt,
                    "total_invoiced_qty": nl["qty"],
                    "qc_required_flag":  False,
                    "line_status":       "OPEN",
                    "plant_ref":         plant_uuid,
                    "cost_center_ref":   cost_center_uuid,
                    "project_ref":       project_uuid,
                    "profit_center_ref": profit_center_uuid,
                    "gl_account_ref":    gl_uuid,
                    "tax_rate_ref":      tax_rate_uuid,
                    "item_ref":          item_uuid,
                    "po_header_ref":     po_uuid,
                    "effective_from":    po_date
                }, table_name="PO_LINE")
                line_uuid = created_pl["id"]
                log.info("[%s] New PO_LINE created → line_uuid=%s", STEP, line_uuid)

                po_lines.append({
                    "po_line_id":  line_uuid,
                    "line_number": int(nl["line_num"]),
                    "description": nl["desc"],
                    "quantity":    nl["qty"],
                    "unit_price":  nl["unit_price"],
                    "uom_id":      uom_id,
                })

        log.info("[%s] PO result → %d matched, %d newly created",
                 STEP, len(po_lines) - len(new_lines), len(new_lines))
        return {
            "po_header_id": po_uuid,
            "po_number":    po_number,
            "po_lines":     po_lines,
            "created":      False
        }

    # ── 3b. Fetch all required master refs once ───────────────────────────────
    log.info("[%s] PO_HEADER not found. Fetching master data refs...", STEP)
    currency_id      = lookup_currency_id(get_optional(static, "currency") or "INR")
    payment_term_id  = lookup_payment_term_id()
    plant_uuid       = lookup_plant_id()
    cost_center_uuid = lookup_cost_center_id()
    project_uuid     = lookup_project_id()
    profit_center_uuid = lookup_profit_center_id()
    tax_rate_uuid    = lookup_tax_rate_id()
    log.info("[%s] Master refs → currency=%s, payterm=%s, plant=%s, tax=%s",
             STEP, currency_id, payment_term_id, plant_uuid, tax_rate_uuid)

    # ── Create PO_HEADER ──────────────────────────────────────────────────────
    po_date   = get_optional(static, "invoice_date") or today()
    total_val = get_optional(static, "total_amount") or 0
    log.info("[%s] Creating PO_HEADER (po_number=%s, date=%s, total=%s)",
             STEP, po_number, po_date, total_val)

    created_ph = create_record(oid_ph, {
        "po_id":                         gen_po_id(),
        "po_number":                     po_number,
        "po_type":                       "MATERIAL",
        "po_date":                       po_date,
        "currency_id":                   currency_id,         # string currency_id
        "exchange_rate":                 1,
        "payment_terms_ref":             payment_term_id,     # string payment_term_id
        "po_total_value":                float(total_val),
        "po_status":                     "APPROVED",
        "created_by":                    config.USER_ID,
        "matching_type":                 "THREE_WAY",
        "supplier_ref":                  supplier_id,         # UUID id of SUPPLIER
        "supplier_site_ref":             supplier_site_id,    # UUID id of SUPPLIER_SITE
        "legal_entity_ref":              legal_entity_id,     # UUID id of LEGAL_ENTITY
        "legal_entity_site_bill_to_ref": legal_entity_site_id,
        "legal_entity_site_ship_to_ref": legal_entity_site_id,
        "plant_ref":                     plant_uuid,          # UUID id of PLANT
        "cost_center_ref":               cost_center_uuid,    # UUID id of COST_CENTER
        "project_ref":                   project_uuid,        # UUID id of PROJECT_WBS
        "profit_center_ref":             profit_center_uuid,  # UUID id of PROFIT_CENTER
        "tax_rate_ref":                  tax_rate_uuid,       # UUID id of TAX_RATE
        "effective_from":                po_date
    }, table_name="PO_HEADER")
    po_uuid = created_ph["id"]
    log.info("[%s] PO_HEADER created → po_uuid=%s", STEP, po_uuid)

    # ── Create PO_LINE per invoice line item ──────────────────────────────────
    oid_pl   = get_object_id("PO_LINE")
    gl_uuid  = lookup_gl_account_id()
    po_lines = []

    for idx, item in enumerate(line_items, start=1):
        hsn_code   = str(item.get("hsn_code") or "").strip()
        uom_code   = str(item.get("unit") or "EA").strip().upper()
        desc       = item.get("description") or f"Line {idx}"
        qty        = float(item.get("quantity") or 0)
        unit_price = float(item.get("unit_price") or 0)
        line_num   = item.get("line_number") or idx

        if not hsn_code:
            raise ValueError(
                f"line_items[{idx}].hsn_code is missing — required to look up HSN_ID"
            )

        hsn_id    = lookup_hsn_id(hsn_code)    # string hsn_id
        uom_id    = lookup_uom_id(uom_code)    # string uom_id
        item_uuid = lookup_item_id(desc)       # UUID id of ITEM

        log.info("[%s] Creating PO_LINE %d (desc=%s, qty=%s, price=%s, hsn=%s)",
                 STEP, idx, desc, qty, unit_price, hsn_code)

        line_amt = qty * unit_price
        created_pl = create_record(oid_pl, {
            "po_line_id":        gen_po_line_id(),
            "line_number":       int(line_num),
            "hsn_id":            hsn_id,           # string hsn_id
            "uom_id":            uom_id,           # string uom_id
            "ordered_quantity":  qty,
            "unit_price":        unit_price,
            "line_amount":       line_amt,          # qty × unit_price
            "total_invoiced_qty": qty,              # invoiced qty from this invoice
            "qc_required_flag":  False,
            "line_status":       "OPEN",
            "plant_ref":         plant_uuid,       # UUID id of PLANT
            "cost_center_ref":   cost_center_uuid, # UUID id of COST_CENTER
            "project_ref":       project_uuid,     # UUID id of PROJECT_WBS
            "profit_center_ref": profit_center_uuid,
            "gl_account_ref":    gl_uuid,          # UUID id of GL_ACCOUNT
            "tax_rate_ref":      tax_rate_uuid,    # UUID id of TAX_RATE
            "item_ref":          item_uuid,        # UUID id of ITEM
            "po_header_ref":     po_uuid,          # UUID id of PO_HEADER
            "effective_from":    po_date
        }, table_name="PO_LINE")
        line_uuid = created_pl["id"]
        log.info("[%s] PO_LINE %d created → line_uuid=%s", STEP, idx, line_uuid)

        po_lines.append({
            "po_line_id":  line_uuid,   # UUID id — passed to GRN as po_line_ref
            "line_number": int(line_num),
            "description": desc,
            "quantity":    qty,
            "unit_price":  unit_price,
            "uom_id":      uom_id,      # already-resolved string uom_id (e.g. "UOM3")
        })

    return {
        "po_header_id": po_uuid,
        "po_number":    po_number,
        "po_lines":     po_lines,
        "created":      True
    }
