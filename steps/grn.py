"""
Step 4 — Goods Receipt Note (GRN)

Always creates: one GRN_HEADER per PO_LINE, each with one GRN_LINE.
All upstream IDs (po_line_id, supplier_site_id, le_site_id) are passed in —
never re-fetched here.
"""

from api.client import get_object_id, create_record
from api.helpers import (
    today, gen_grn_id, gen_grn_line_id, gen_grn_number, get_optional,
)
from lookups.master import lookup_uom_id, lookup_gl_account_id, lookup_item_id, lookup_weight_uom_id


def handle_grn(
    invoice_data: dict,
    po_lines: list,           # list of dicts from steps/po.py
    supplier_site_id: str,
    legal_entity_site_id: str,
) -> list:
    """
    Creates GRN_HEADER + GRN_LINE for each PO line.

    Returns list of:
        {
            "po_line_id":        str,
            "grn_id":            str,
            "grn_number":        str,
            "grn_line_id":       str,
            "description":       str,
            "quantity":          float,
            "unit_price":        float,
            "total_amount":      float,
        }
    """
    if not po_lines:
        raise ValueError("po_lines is empty — cannot create GRN records")

    line_items  = invoice_data.get("line_items") or []
    oid_gh      = get_object_id("GRN_HEADER")
    oid_gl      = get_object_id("GRN_LINE")
    gl_acct_id  = lookup_gl_account_id()
    weight_uom  = lookup_weight_uom_id()
    grn_date    = today()

    results = []

    for idx, po_line in enumerate(po_lines):
        po_line_id = po_line["po_line_id"]
        desc       = po_line.get("description") or f"Line {idx + 1}"
        qty        = float(po_line.get("quantity") or 0)
        unit_price = float(po_line.get("unit_price") or 0)
        uom_code   = po_line.get("uom_code") or "EA"

        # Match with invoice line item to get total (if available)
        inv_item   = line_items[idx] if idx < len(line_items) else {}
        total_amt  = float(inv_item.get("total") or (qty * unit_price))

        uom_id     = lookup_uom_id(uom_code)
        item_id    = lookup_item_id(desc)

        # ── Create GRN_HEADER ─────────────────────────────────────────────────
        grn_id     = gen_grn_id()
        grn_number = gen_grn_number()

        create_record(oid_gh, {
            "grn_id":                  grn_id,
            "grn_number":              grn_number,
            "grn_date":                grn_date,
            "total_received_qty":      qty,
            "total_received_amount":   total_amt,
            "weight_uom_id":           weight_uom,
            "qc_status":               "PENDING",
            "grn_status":              "OPEN",
            "po_line_ref":             po_line_id,
            "supplier_site_ref":       supplier_site_id,
            "legal_entity_site_ref":   legal_entity_site_id,
            "gl_account_ref":          gl_acct_id,
            "effective_from":          grn_date
        }, table_name="GRN_HEADER")

        # ── Create GRN_LINE ───────────────────────────────────────────────────
        grn_line_id = gen_grn_line_id()

        create_record(oid_gl, {
            "grn_line_id":      grn_line_id,
            "grn_line_number":  1,
            "item_description": desc,
            "uom_id":           uom_id,
            "received_qty":     qty,
            "weight_uom":       "KGS",
            "qc_required_flag": False,
            "qc_result":        "ACCEPTED",
            "grn_line_status":  "OPEN",
            "grn_ref":          grn_id,
            "item_ref":         item_id,
            "effective_from":   grn_date
        }, table_name="GRN_LINE")

        results.append({
            "po_line_id":   po_line_id,
            "grn_id":       grn_id,
            "grn_number":   grn_number,
            "grn_line_id":  grn_line_id,
            "description":  desc,
            "quantity":     qty,
            "unit_price":   unit_price,
            "total_amount": total_amt,
        })

    return results
