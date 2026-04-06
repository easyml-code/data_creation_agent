"""
Low-level HTTP wrapper around the metadata/usage APIs.
Every function raises a typed exception — callers never deal with raw HTTP errors.
"""

import json
import logging
import requests
from functools import lru_cache
from typing import Any

import config
from api.exceptions import APIError

log = logging.getLogger(__name__)

_HEADERS_GET = {
    "Content-Type": "application/json;charset=UTF-8",
    "Accept":       "application/json",
}


def _headers_post() -> dict:
    # No Content-Type here — requests sets it automatically to
    # multipart/form-data (with boundary) when using files=
    return {
        "Accept":  "application/json",
        "userId":  config.USER_ID,
    }


# ── Object-ID cache ───────────────────────────────────────────────────────────

@lru_cache(maxsize=None)
def get_object_id(api_name: str) -> str:
    """
    Fetch the internal objectId for a given API name.
    Result is cached in-process so we only call the definition API once per table.
    """
    url = f"{config.BASE_URL}/{config.API_VERSION}/object/api-name/{api_name}"
    log.debug("[get_object_id] GET %s", url)
    try:
        resp = requests.get(url, headers=_HEADERS_GET, timeout=10)
    except requests.exceptions.ConnectionError as e:
        log.error("[get_object_id] Connection failed for %s: %s", api_name, e)
        raise APIError("get_object_id", api_name, f"Connection failed: {e}")
    except requests.exceptions.Timeout:
        log.error("[get_object_id] Timeout for %s", api_name)
        raise APIError("get_object_id", api_name, "Request timed out after 10s")

    log.debug("[get_object_id] %s → HTTP %s | body: %s", api_name, resp.status_code, resp.text[:500])

    if not resp.ok:
        raise APIError("get_object_id", api_name,
                       "HTTP error fetching objectId",
                       status_code=resp.status_code, body=resp.text)

    body = resp.json()
    if not body.get("success"):
        raise APIError("get_object_id", api_name,
                       f"API success=False: {body.get('message', body)}")

    object_id = body.get("data", {}).get("objectId")
    if not object_id:
        raise APIError("get_object_id", api_name,
                       f"objectId missing in response: {body}")

    log.info("[get_object_id] %s → objectId=%s", api_name, object_id)
    return object_id


# ── GET records ───────────────────────────────────────────────────────────────

def get_records(
    object_id: str,
    table_name: str = "?",
    field: str = None,
    value: str = None,
    operator: str = "EQUALS",
    limit: int = 1,
) -> list[dict]:
    """
    Fetch rows from a dynamic table.
    Pass field + value for a filtered lookup; omit both to get the first N rows.
    Returns a list of record dicts (may be empty).
    """
    params: dict[str, Any] = {"page": 0, "size": limit}
    if field and value is not None:
        params["field"]    = field.upper()
        params["operator"] = operator
        params["filter"]   = str(value)

    url = f"{config.USAGE_URL}/{config.API_VERSION}/object/{object_id}/data"
    log.debug("[get_records] GET %s | params: %s", url, params)
    try:
        resp = requests.get(url, params=params, headers=_HEADERS_GET, timeout=30)
    except requests.exceptions.ConnectionError as e:
        log.error("[get_records] Connection failed for %s: %s", table_name, e)
        raise APIError("get_records", table_name, f"Connection failed: {e}")
    except requests.exceptions.Timeout:
        log.error("[get_records] Timeout for %s (field=%s, value=%s)", table_name, field, value)
        raise APIError("get_records", table_name,
                       f"GET timed out (field={field}, value={value})")

    log.debug("[get_records] %s → HTTP %s | body: %s", table_name, resp.status_code, resp.text[:500])

    if not resp.ok:
        log.error("[get_records] HTTP %s for %s | response: %s", resp.status_code, table_name, resp.text)
        raise APIError("get_records", table_name,
                       f"HTTP error fetching records (field={field}, value={value})",
                       status_code=resp.status_code, body=resp.text)

    body = resp.json()
    if not body.get("success"):
        log.error("[get_records] success=False for %s | response: %s", table_name, body)
        raise APIError("get_records", table_name,
                       f"API success=False: {body.get('message', body)}")

    records = body.get("data") or []
    log.info("[get_records] %s (field=%s, value=%s) → %d record(s)", table_name, field, value, len(records))
    return records


# ── POST / create record ──────────────────────────────────────────────────────

def create_record(
    object_id: str,
    payload: dict,
    table_name: str = "?",
) -> dict:
    """
    Insert a new row into a dynamic table.
    Wraps the payload in the envelope the API expects and returns the created record dict.
    """
    # The API expects multipart/form-data with jsonInput as a JSON string field
    form_data = {
        "jsonInput":      (None, json.dumps(payload)),
        "storageService": (None, "DATABASE_POSTGRES"),
    }
    url = f"{config.USAGE_URL}/{config.API_VERSION}/object/{object_id}/data"
    log.debug("[create_record] POST %s (multipart) | jsonInput: %s", url, json.dumps(payload))
    try:
        resp = requests.post(
            url,
            files=form_data,
            headers=_headers_post(),
            timeout=30
        )
    except requests.exceptions.ConnectionError as e:
        log.error("[create_record] Connection failed for %s: %s", table_name, e)
        raise APIError("create_record", table_name, f"Connection failed: {e}")
    except requests.exceptions.Timeout:
        log.error("[create_record] Timeout for %s", table_name)
        raise APIError("create_record", table_name, "POST timed out")

    log.debug("[create_record] %s → HTTP %s | body: %s", table_name, resp.status_code, resp.text[:500])

    if not resp.ok:
        log.error("[create_record] HTTP %s for %s | jsonInput: %s | response: %s",
                  resp.status_code, table_name, json.dumps(payload), resp.text)
        raise APIError("create_record", table_name,
                       f"HTTP error creating record",
                       status_code=resp.status_code, body=resp.text)

    body_resp = resp.json()
    if not body_resp.get("success"):
        log.error("[create_record] success=False for %s | response: %s", table_name, body_resp)
        raise APIError("create_record", table_name,
                       f"API success=False: {body_resp.get('message', body_resp)}")

    # API returns data as a list with the created record at index 0
    data = body_resp.get("data") or []
    created = data[0] if data else {}
    log.info("[create_record] %s created → uuid=%s", table_name, created.get("id", "?"))
    return created
