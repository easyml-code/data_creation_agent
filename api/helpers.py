"""
Utility functions shared across all modules.
No business logic here — only pure helpers.
"""

import re
import uuid
from datetime import date


def today() -> str:
    return date.today().isoformat()


def pan_from_gstin(gstin: str) -> str:
    """
    Extract 10-character PAN from a 15-character Indian GSTIN.
    GSTIN format: SS AAAAA NNNN A N Z
    PAN occupies positions 3–12 (0-indexed: 2:12).
    """
    gstin = gstin.strip().upper()
    if len(gstin) != 15:
        raise ValueError(
            f"Invalid GSTIN length: '{gstin}' (expected 15 chars, got {len(gstin)})"
        )
    return gstin[2:12]


# ── ID generators ─────────────────────────────────────────────────────────────
# Each generator respects the field's max length from the object structure.
# Pattern: readable prefix + short UUID hex so IDs are traceable in logs.

def gen_supplier_id() -> str:
    """text, length 36"""
    return str(uuid.uuid4())


def gen_supplier_code(name: str) -> str:
    """text, length 50. Prefix from supplier name + hex for uniqueness."""
    prefix = re.sub(r"[^A-Z0-9]", "", name.upper())[:6] or "SUP"
    return f"SC-{prefix}-{uuid.uuid4().hex[:8].upper()}"  # max ~20 chars, well within 50


def gen_supplier_site_id() -> str:
    """text — no max length in object structure, use readable format."""
    return f"SSITE-{uuid.uuid4().hex[:10].upper()}"


def gen_legal_entity_id() -> str:
    return f"LE-{uuid.uuid4().hex[:10].upper()}"


def gen_legal_entity_site_id() -> str:
    return f"LESITE-{uuid.uuid4().hex[:10].upper()}"


def gen_po_id() -> str:
    return f"PO-{uuid.uuid4().hex[:10].upper()}"


def gen_po_line_id() -> str:
    return f"POLINE-{uuid.uuid4().hex[:10].upper()}"


def gen_grn_id() -> str:
    return f"GRN-{uuid.uuid4().hex[:10].upper()}"


def gen_grn_line_id() -> str:
    return f"GRNLINE-{uuid.uuid4().hex[:10].upper()}"


def gen_grn_number() -> str:
    return f"GRN-{date.today().strftime('%Y%m%d')}-{uuid.uuid4().hex[:6].upper()}"


# ── Invoice field extractors ──────────────────────────────────────────────────

def require(data: dict, *keys, label: str = "") -> object:
    """
    Safely walk nested dict keys; raise ValidationError with a clear path if missing.
    Usage: require(invoice_data, "static", "supplier_details", "gstin")
    """
    from api.exceptions import ValidationError
    current = data
    path    = label or ".".join(str(k) for k in keys)
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            raise ValidationError(path, "is required but missing from invoice_data")
        current = current[key]
    if current is None or current == "":
        raise ValidationError(path, "is present but empty")
    return current


def get_optional(data: dict, *keys, default=None):
    """Walk nested dict keys, return default if any key is missing."""
    current = data
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current if current not in (None, "") else default
