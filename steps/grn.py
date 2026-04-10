"""
Step 4 — Goods Receipt Note (GRN)

Always creates: one GRN_HEADER per PO_LINE, each with one GRN_LINE.
All upstream UUIDs (po_line_id, supplier_site_id, le_site_id) are passed in.

Cross-table refs must use UUID id fields.
EXCEPTION: uom_id, weight_uom use string *_id values.
"""

import logging
from api.client import get_object_id, create_record
from api.helpers import (
    today, gen_grn_id, gen_grn_line_id, gen_grn_number, get_optional,
)
from lookups.master import lookup_gl_account_id, lookup_item_id, lookup_weight_uom_id

log = logging.getLogger(__name__)
STEP = "Step 4 | GRN"


def handle_grn(
    invoice_data: dict,
    po_lines: list,               # from steps/po.py — po_line_id is UUID id of PO_LINE
    supplier_site_id: str,        # UUID id of SUPPLIER_SITE
    legal_entity_site_id: str,    # UUID id of LEGAL_ENTITY_SITE
) -> list:
    """
    Creates GRN_HEADER + GRN_LINE for each PO line.

    Returns list of:
        {
            "po_line_id":   str   — UUID id of PO_LINE
            "grn_id":       str   — human-readable GRN id we assigned
            "grn_number":   str
            "grn_line_id":  str   — human-readable GRN line id we assigned
            "description":  str,
            "quantity":     float,
            "unit_price":   float,
            "total_amount": float,
        }
    """
    if not po_lines:
        raise ValueError("po_lines is empty — cannot create GRN records")

    line_items     = invoice_data.get("line_items") or []
    oid_gh         = get_object_id("GRN_HEADER")
    oid_gl         = get_object_id("GRN_LINE")
    gl_uuid        = lookup_gl_account_id()    # UUID id of GL_ACCOUNT
    weight_uom_str = lookup_weight_uom_id()    # string uom_id like "UOM1"
    grn_date       = today()

    log.info("[%s] Creating GRN records for %d PO line(s)", STEP, len(po_lines))
    results = []

    for idx, po_line in enumerate(po_lines):
        po_line_uuid = po_line["po_line_id"]   # UUID id of PO_LINE
        desc         = po_line.get("description") or f"Line {idx + 1}"
        qty          = float(po_line.get("quantity") or 0)
        unit_price   = float(po_line.get("unit_price") or 0)
        # uom_id is already the resolved string uom_id from po_lines — no lookup needed
        uom_str      = po_line.get("uom_id") or "UOM1"

        inv_item  = line_items[idx] if idx < len(line_items) else {}
        total_amt = float(inv_item.get("total") or (qty * unit_price))
        item_uuid = lookup_item_id(desc)        # UUID id of ITEM

        grn_id_str = gen_grn_id()
        grn_number = gen_grn_number()

        # ── Create GRN_HEADER ─────────────────────────────────────────────────
        log.info("[%s] Creating GRN_HEADER for po_line_uuid=%s (qty=%s, amount=%s)",
                 STEP, po_line_uuid, qty, total_amt)
        created_gh = create_record(oid_gh, {
            "grn_id":                  grn_id_str,
            "grn_number":              grn_number,
            "grn_date":                grn_date,
            "total_received_qty":      qty,
            "total_received_amount":   total_amt,
            "weight_uom_id":           weight_uom_str,      # string uom_id
            "qc_status":               "PENDING",
            "grn_status":              "OPEN",
            "po_line_ref":             po_line_uuid,        # UUID id of PO_LINE
            "supplier_site_ref":       supplier_site_id,    # UUID id of SUPPLIER_SITE
            "legal_entity_site_ref":   legal_entity_site_id, # UUID id of LE_SITE
            "gl_account_ref":          gl_uuid,             # UUID id of GL_ACCOUNT
            "effective_from":          grn_date
        }, table_name="GRN_HEADER")
        grn_uuid = created_gh["id"]
        log.info("[%s] GRN_HEADER created → grn_uuid=%s (grn_number=%s)", STEP, grn_uuid, grn_number)

        # ── Create GRN_LINE ───────────────────────────────────────────────────
        grn_line_id_str = gen_grn_line_id()
        log.info("[%s] Creating GRN_LINE for grn_uuid=%s (desc=%s, qty=%s)",
                 STEP, grn_uuid, desc, qty)
        create_record(oid_gl, {
            "grn_line_id":            grn_line_id_str,
            "grn_line_number":        1,
            "item_description":       desc,
            "uom_id":                 uom_str,          # string uom_id
            "received_qty":           qty,
            "accepted_qty":           qty,               # QC accepted = received
            "unit_price":             unit_price,
            "total_received_amount":  total_amt,         # qty × unit_price
            "weight_uom":            weight_uom_str,    # string uom_id
            "qc_required_flag":       False,
            "qc_result":              "ACCEPTED",
            "grn_line_status":        "OPEN",
            "grn_ref":                grn_uuid,         # UUID id of GRN_HEADER
            "item_ref":               item_uuid,        # UUID id of ITEM
            "effective_from":         grn_date
        }, table_name="GRN_LINE")
        log.info("[%s] GRN_LINE created for grn_uuid=%s", STEP, grn_uuid)

        results.append({
            "po_line_id":   po_line_uuid,
            "grn_id":       grn_id_str,
            "grn_number":   grn_number,
            "grn_line_id":  grn_line_id_str,
            "description":  desc,
            "quantity":     qty,
            "unit_price":   unit_price,
            "total_amount": total_amt,
        })

    log.info("[%s] Done — created %d GRN_HEADER + GRN_LINE pair(s)", STEP, len(results))
    return results
