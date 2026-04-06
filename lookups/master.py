"""
Master-data lookup functions.

Each function fetches a reference ID from a master table using a human-readable key
(country name, currency code, etc.) and raises LookupError with a clear message if
the record does not exist.

These tables are assumed to be pre-seeded. The pipeline never creates master data —
it only reads it.
"""

from api.client import get_object_id, get_records
from api.exceptions import LookupError as MasterLookupError


def _fetch_one(table_api_name: str, field: str, value: str, id_field: str) -> str:
    """Generic single-record lookup. Raises MasterLookupError if not found."""
    oid     = get_object_id(table_api_name)
    records = get_records(oid, table_name=table_api_name, field=field, value=value)
    if not records:
        raise MasterLookupError(table_api_name, field, value)
    return records[0][id_field]


def _fetch_first(table_api_name: str, id_field: str) -> str:
    """Return the first available record's ID from a table. Used for refs the invoice doesn't specify."""
    oid     = get_object_id(table_api_name)
    records = get_records(oid, table_name=table_api_name, limit=1)
    if not records:
        raise MasterLookupError(table_api_name, "(any)", "(first available record)")
    return records[0][id_field]


# ── Geographic ────────────────────────────────────────────────────────────────

def lookup_country_id(country_name: str) -> str:
    """COUNTRY.COUNTRY_NAME → COUNTRY.country_id"""
    return _fetch_one("COUNTRY", "COUNTRY_NAME", country_name, "country_id")


def lookup_state_id(state_name: str) -> str:
    """STATE.STATE_NAME → STATE.state_id"""
    return _fetch_one("STATE", "STATE_NAME", state_name, "state_id")


# ── Financial ─────────────────────────────────────────────────────────────────

def lookup_currency_id(currency_code: str) -> str:
    """CURRENCY.CURRENCY_CODE → CURRENCY.currency_id"""
    return _fetch_one("CURRENCY", "CURRENCY_CODE", currency_code, "currency_id")


def lookup_payment_term_id() -> str:
    """Return first available PAYMENT_TERMS record (invoice doesn't specify this)."""
    return _fetch_first("PAYMENT_TERMS", "payment_term_id")


# ── Tax ───────────────────────────────────────────────────────────────────────

def lookup_tax_rate_id() -> str:
    """Return first available TAX_RATE record."""
    return _fetch_first("TAX_RATE", "tax_rate_id")


# ── Item / Goods ─────────────────────────────────────────────────────────────

def lookup_hsn_id(hsn_code: str) -> str:
    """HSN_SAC.HSN_CODE → HSN_SAC.hsn_id"""
    return _fetch_one("HSN_SAC", "HSN_CODE", hsn_code, "hsn_id")


def lookup_uom_id(uom_code: str) -> str:
    """UOM.UOM_CODE → UOM.uom_id"""
    return _fetch_one("UOM", "UOM_CODE", uom_code, "uom_id")


def lookup_item_id(item_name: str) -> str:
    """
    Try to match ITEM.ITEM_NAME → ITEM.item_id.
    Falls back to first available item if no exact match.
    """
    oid     = get_object_id("ITEM")
    records = get_records(oid, table_name="ITEM", field="ITEM_NAME", value=item_name)
    if records:
        return records[0]["item_id"]
    # Fallback: use the first item in the table
    records = get_records(oid, table_name="ITEM", limit=1)
    if not records:
        raise MasterLookupError("ITEM", "ITEM_NAME", item_name)
    return records[0]["item_id"]


# ── Organisational ────────────────────────────────────────────────────────────

def lookup_plant_id() -> str:
    return _fetch_first("PLANT", "plant_id")


def lookup_cost_center_id() -> str:
    return _fetch_first("COST_CENTER", "cost_center_id")


def lookup_project_id() -> str:
    return _fetch_first("PROJECT_WBS", "project_wbs_id")


def lookup_profit_center_id() -> str:
    return _fetch_first("PROFIT_CENTER", "profit_center_id")


def lookup_gl_account_id() -> str:
    return _fetch_first("GL_ACCOUNT", "gl_account_id")


def lookup_weight_uom_id() -> str:
    """
    Try common weight UOM codes in order; use first available UOM as last resort.
    """
    for code in ("KGS", "KG", "KGM"):
        try:
            return lookup_uom_id(code)
        except MasterLookupError:
            continue
    return _fetch_first("UOM", "uom_id")
