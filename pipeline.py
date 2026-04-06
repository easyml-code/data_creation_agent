"""
Orchestrator — runs all 4 steps in order and returns a unified result dict.

Each step receives only what it needs and passes forward only what the next step needs.
No global state, no re-fetching of upstream data.
"""

import traceback

from steps.supplier      import handle_supplier
from steps.legal_entity  import handle_legal_entity
from steps.po            import handle_po
from steps.grn           import handle_grn
from api.exceptions      import APIError, ValidationError
from api.exceptions      import LookupError as MasterLookupError


def run_pipeline(invoice_data: dict) -> dict:
    """
    Full invoice → GRN pipeline.

    Args:
        invoice_data: OCR-extracted invoice dict (same shape as the input example).

    Returns:
        {
            "invoice_number":        str,
            "supplier":              { supplier_id, supplier_site_id, created },
            "legal_entity":          { legal_entity_id, legal_entity_site_id, created },
            "po":                    { po_header_id, po_number, po_lines, created },
            "grn_records":           [ { grn_id, grn_number, grn_line_id, ... } ],
            "summary": {
                "total_po_lines": int,
                "total_grn_created": int,
            }
        }

    Raises:
        ValidationError  — missing / empty invoice field
        MasterLookupError — master data record not found in DB
        APIError         — HTTP/API failure
        ValueError       — bad data shape
    """

    # ── Step 1: Supplier ──────────────────────────────────────────────────────
    supplier = handle_supplier(invoice_data)

    # ── Step 2: Legal Entity ──────────────────────────────────────────────────
    legal_entity = handle_legal_entity(invoice_data)

    # ── Step 3: PO ────────────────────────────────────────────────────────────
    po = handle_po(
        invoice_data,
        supplier_id          = supplier["supplier_id"],
        supplier_site_id     = supplier["supplier_site_id"],
        legal_entity_id      = legal_entity["legal_entity_id"],
        legal_entity_site_id = legal_entity["legal_entity_site_id"],
    )

    # ── Step 4: GRN ───────────────────────────────────────────────────────────
    grn_records = handle_grn(
        invoice_data,
        po_lines             = po["po_lines"],
        supplier_site_id     = supplier["supplier_site_id"],
        legal_entity_site_id = legal_entity["legal_entity_site_id"],
    )

    return {
        "invoice_number": invoice_data["static"].get("invoice_number"),
        "supplier":       supplier,
        "legal_entity":   legal_entity,
        "po":             po,
        "grn_records":    grn_records,
        "summary": {
            "total_po_lines":    len(po["po_lines"]),
            "total_grn_created": len(grn_records),
        }
    }
