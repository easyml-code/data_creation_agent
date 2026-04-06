"""
Master-data lookup functions.

Rule (from API data model):
  - Simple code tables (COUNTRY, STATE, CURRENCY, UOM, HSN_SAC, PAYMENT_TERMS)
    use their human-readable *_id string as the ref value (e.g. "COUNTRY31", "UOM1").
  - Complex entity tables (TAX_RATE, PLANT, COST_CENTER, PROJECT_WBS, PROFIT_CENTER,
    GL_ACCOUNT, ITEM) use the system UUID `id` field as the ref value.

These tables are pre-seeded. The pipeline never creates master data — only reads it.
"""

import logging
from api.client import get_object_id, get_records
from api.exceptions import LookupError as MasterLookupError

log = logging.getLogger(__name__)


def _fetch_one(table_api_name: str, field: str, value: str, id_field: str) -> str:
    """Generic single-record lookup. Raises MasterLookupError if not found."""
    oid     = get_object_id(table_api_name)
    records = get_records(oid, table_name=table_api_name, field=field, value=value)
    if not records:
        raise MasterLookupError(table_api_name, field, value)
    return records[0][id_field]


def _fetch_first(table_api_name: str, id_field: str) -> str:
    """Return the first available record's id_field from a table."""
    oid     = get_object_id(table_api_name)
    records = get_records(oid, table_name=table_api_name, limit=1)
    if not records:
        raise MasterLookupError(table_api_name, "(any)", "(first available record)")
    return records[0][id_field]


# ── Geographic — string *_id refs ─────────────────────────────────────────────

def lookup_country_id(country_name: str) -> str:
    """Returns string country_id (e.g. "COUNTRY31")"""
    return _fetch_one("COUNTRY", "COUNTRY_NAME", country_name, "country_id")


def lookup_state_id(state_name: str) -> str:
    """Returns string state_id (e.g. "STATE26")"""
    return _fetch_one("STATE", "STATE_NAME", state_name, "state_id")


# ── Financial — string *_id refs ──────────────────────────────────────────────

def lookup_currency_id(currency_code: str) -> str:
    """Returns string currency_id (e.g. "CURRENCY31")"""
    return _fetch_one("CURRENCY", "CURRENCY_CODE", currency_code, "currency_id")


def lookup_payment_term_id() -> str:
    """Returns string payment_term_id (e.g. "PAYTERM01")"""
    return _fetch_first("PAYMENT_TERMS", "payment_term_id")


# ── Item / Goods — string *_id refs ──────────────────────────────────────────

def lookup_hsn_id(hsn_code: str) -> str:
    """Returns string hsn_id (e.g. "HSN22016")"""
    return _fetch_one("HSN_SAC", "HSN_CODE", hsn_code, "hsn_id")


def lookup_uom_id(uom_code: str) -> str:
    """Returns string uom_id (e.g. "UOM1")"""
    return _fetch_one("UOM", "UOM_CODE", uom_code, "uom_id")


def lookup_weight_uom_id() -> str:
    """Returns string uom_id for weight. Tries common codes, falls back to first."""
    for code in ("KGS", "KG", "KGM"):
        try:
            return lookup_uom_id(code)
        except MasterLookupError:
            continue
    return _fetch_first("UOM", "uom_id")


# ── Tax — UUID id refs ────────────────────────────────────────────────────────

def lookup_tax_rate_id() -> str:
    """Returns UUID id of first TAX_RATE record"""
    return _fetch_first("TAX_RATE", "id")


# ── Item — UUID id ref ────────────────────────────────────────────────────────

def lookup_item_id(item_name: str) -> str:
    """
    Returns UUID id of ITEM matched by ITEM_NAME.
    Falls back to first available item if no exact match.
    """
    oid     = get_object_id("ITEM")
    records = get_records(oid, table_name="ITEM", field="ITEM_NAME", value=item_name)
    if records:
        log.debug("[lookup_item_id] matched '%s' → uuid=%s", item_name, records[0]["id"])
        return records[0]["id"]
    records = get_records(oid, table_name="ITEM", limit=1)
    if not records:
        raise MasterLookupError("ITEM", "ITEM_NAME", item_name)
    log.debug("[lookup_item_id] no match for '%s', using fallback uuid=%s", item_name, records[0]["id"])
    return records[0]["id"]


# ── Organisational — UUID id refs ─────────────────────────────────────────────

def lookup_plant_id() -> str:
    """Returns UUID id of first PLANT record"""
    return _fetch_first("PLANT", "id")


def lookup_cost_center_id() -> str:
    """Returns UUID id of first COST_CENTER record"""
    return _fetch_first("COST_CENTER", "id")


def lookup_project_id() -> str:
    """Returns UUID id of first PROJECT_WBS record"""
    return _fetch_first("PROJECT_WBS", "id")


def lookup_profit_center_id() -> str:
    """Returns UUID id of first PROFIT_CENTER record"""
    return _fetch_first("PROFIT_CENTER", "id")


def lookup_gl_account_id() -> str:
    """Returns UUID id of first GL_ACCOUNT record"""
    return _fetch_first("GL_ACCOUNT", "id")
