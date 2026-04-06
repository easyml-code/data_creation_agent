"""
Step 3 — Purchase Order

Check PO_HEADER by PO_NUMBER.
  ├─ Found  → fetch existing PO_LINEs and return them
  └─ Missing → create PO_HEADER + one PO_LINE per invoice line item

All relationship refs (supplier, LE, plant, cost center, etc.) are passed in
from upstream steps — never looked up independently here.
"""

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


def handle_po(
    invoice_data: dict,
    supplier_id: str,
    supplier_site_id: str,
    legal_entity_id: str,
    legal_entity_site_id: str,
) -> dict:
    """
    Returns:
        {
            "po_header_id": str,
            "po_number":    str,
            "po_lines":     [
                {
                    "po_line_id":   str,
                    "line_number":  int,
                    "description":  str,
                    "quantity":     float,
                    "unit_price":   float,
                    "hsn_code":     str,
                    "uom_code":     str,
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
    existing = get_records(oid_ph, table_name="PO_HEADER",
                           field="PO_NUMBER", value=po_number)

    if existing:
        po_id = existing[0]["PO_ID"]

        oid_pl  = get_object_id("PO_LINE")
        po_rows = get_records(oid_pl, table_name="PO_LINE",
                              field="PO_HEADER_REF", value=po_id,
                              limit=len(line_items) + 20)

        po_lines = [
            {
                "po_line_id":  r["PO_LINE_ID"],
                "line_number": r.get("LINE_NUMBER"),
                "description": r.get("ITEM_DESCRIPTION", ""),
                "quantity":    r.get("ORDERED_QUANTITY"),
                "unit_price":  r.get("UNIT_PRICE"),
                "hsn_code":    r.get("HSN_ID", ""),
                "uom_code":    r.get("UOM_ID", ""),
            }
            for r in po_rows
        ]

        return {
            "po_header_id": po_id,
            "po_number":    po_number,
            "po_lines":     po_lines,
            "created":      False
        }

    # ── 3b. Fetch all required master refs (once) ─────────────────────────────
    currency_id      = lookup_currency_id(get_optional(static, "currency") or "INR")
    payment_term_id  = lookup_payment_term_id()
    plant_id         = lookup_plant_id()
    cost_center_id   = lookup_cost_center_id()
    project_id       = lookup_project_id()
    profit_center_id = lookup_profit_center_id()
    tax_rate_id      = lookup_tax_rate_id()

    # ── Create PO_HEADER ──────────────────────────────────────────────────────
    po_id      = gen_po_id()
    po_date    = get_optional(static, "invoice_date") or today()
    total_val  = get_optional(static, "total_amount") or 0

    create_record(oid_ph, {
        "PO_ID":                         po_id,
        "PO_NUMBER":                     po_number,
        "PO_TYPE":                       "MATERIAL",
        "PO_DATE":                       po_date,
        "CURRENCY_ID":                   currency_id,
        "EXCHANGE_RATE":                 1,
        "PAYMENT_TERMS_REF":             payment_term_id,
        "PO_TOTAL_VALUE":                float(total_val),
        "PO_STATUS":                     "APPROVED",
        "CREATED_BY":                    config.USER_ID,
        "MATCHING_TYPE":                 "THREE_WAY",
        "SUPPLIER_REF":                  supplier_id,
        "SUPPLIER_SITE_REF":             supplier_site_id,
        "LEGAL_ENTITY_REF":              legal_entity_id,
        "LEGAL_ENTITY_SITE_BILL_TO_REF": legal_entity_site_id,
        "LEGAL_ENTITY_SITE_SHIP_TO_REF": legal_entity_site_id,
        "PLANT_REF":                     plant_id,
        "COST_CENTER_REF":               cost_center_id,
        "PROJECT_REF":                   project_id,
        "PROFIT_CENTER_REF":             profit_center_id,
        "TAX_RATE_REF":                  tax_rate_id,
        "EFFECTIVE_FROM":                po_date
    }, table_name="PO_HEADER")

    # ── Create PO_LINE per invoice line item ──────────────────────────────────
    oid_pl    = get_object_id("PO_LINE")
    gl_acct   = lookup_gl_account_id()
    po_lines  = []

    for idx, item in enumerate(line_items, start=1):
        hsn_code  = str(item.get("hsn_code") or "").strip()
        uom_code  = str(item.get("unit") or "EA").strip().upper()
        desc      = item.get("description") or f"Line {idx}"
        qty       = float(item.get("quantity") or 0)
        unit_price= float(item.get("unit_price") or 0)
        line_num  = item.get("line_number") or idx
        item_po   = str(item.get("purchase_order_number") or po_number).strip()

        if not hsn_code:
            raise ValueError(
                f"line_items[{idx}].hsn_code is missing — required to look up HSN_ID"
            )

        hsn_id   = lookup_hsn_id(hsn_code)
        uom_id   = lookup_uom_id(uom_code)
        item_id  = lookup_item_id(desc)
        line_id  = gen_po_line_id()

        create_record(oid_pl, {
            "PO_LINE_ID":       line_id,
            "LINE_NUMBER":      int(line_num),
            "HSN_ID":           hsn_id,
            "UOM_ID":           uom_id,
            "ORDERED_QUANTITY": qty,
            "UNIT_PRICE":       unit_price,
            "QC_REQUIRED_FLAG": False,
            "LINE_STATUS":      "OPEN",
            "PLANT_REF":        plant_id,
            "COST_CENTER_REF":  cost_center_id,
            "PROJECT_REF":      project_id,
            "PROFIT_CENTER_REF":profit_center_id,
            "GL_ACCOUNT_REF":   gl_acct,
            "TAX_RATE_REF":     tax_rate_id,
            "ITEM_REF":         item_id,
            "PO_HEADER_REF":    po_id,
            "EFFECTIVE_FROM":   po_date
        }, table_name="PO_LINE")

        po_lines.append({
            "po_line_id":  line_id,
            "line_number": int(line_num),
            "description": desc,
            "quantity":    qty,
            "unit_price":  unit_price,
            "hsn_code":    hsn_code,
            "uom_code":    uom_code,
        })

    return {
        "po_header_id": po_id,
        "po_number":    po_number,
        "po_lines":     po_lines,
        "created":      True
    }
