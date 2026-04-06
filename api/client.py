"""
Low-level HTTP wrapper around the metadata/usage APIs.
Every function raises a typed exception — callers never deal with raw HTTP errors.
"""

import requests
from functools import lru_cache
from typing import Any

import config
from api.exceptions import APIError


# ── Object-ID cache ───────────────────────────────────────────────────────────

@lru_cache(maxsize=None)
def get_object_id(api_name: str) -> str:
    """
    Fetch the internal objectId for a given API name.
    Result is cached in-process so we only call the definition API once per table.
    """
    url = f"{config.BASE_URL}/{config.API_VERSION}/object/api-name/{api_name}"
    try:
        resp = requests.get(
            url,
            headers={"Content-Type": "application/json"},
            timeout=10
        )
    except requests.exceptions.ConnectionError as e:
        raise APIError("get_object_id", api_name, f"Connection failed: {e}")
    except requests.exceptions.Timeout:
        raise APIError("get_object_id", api_name, "Request timed out after 10s")

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

    return object_id


# ── GET records ───────────────────────────────────────────────────────────────

def get_records(
    object_id: str,
    table_name: str = "?",      # only used for error messages
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
    try:
        resp = requests.get(
            url,
            params=params,
            headers={"Content-Type": "application/json"},
            timeout=30
        )
    except requests.exceptions.ConnectionError as e:
        raise APIError("get_records", table_name, f"Connection failed: {e}")
    except requests.exceptions.Timeout:
        raise APIError("get_records", table_name,
                       f"GET timed out (field={field}, value={value})")

    if not resp.ok:
        raise APIError("get_records", table_name,
                       f"HTTP error fetching records (field={field}, value={value})",
                       status_code=resp.status_code, body=resp.text)

    body = resp.json()
    if not body.get("success"):
        raise APIError("get_records", table_name,
                       f"API success=False: {body.get('message', body)}")

    return body.get("data") or []


# ── POST / create record ──────────────────────────────────────────────────────

def create_record(
    object_id: str,
    payload: dict,
    table_name: str = "?",      # only used for error messages
) -> dict:
    """
    Insert a new row into a dynamic table.
    Wraps the payload in the envelope the API expects and returns the created record dict.
    """
    body = {
        "jsonInput":      payload,
        "storageService": "DATABASE_POSTGRES"
    }
    url = f"{config.USAGE_URL}/{config.API_VERSION}/object/{object_id}/data"
    try:
        resp = requests.post(
            url,
            json=body,
            headers={
                "Content-Type": "application/json",
                "userId":       config.USER_ID
            },
            timeout=30
        )
    except requests.exceptions.ConnectionError as e:
        raise APIError("create_record", table_name, f"Connection failed: {e}")
    except requests.exceptions.Timeout:
        raise APIError("create_record", table_name, "POST timed out")

    if not resp.ok:
        raise APIError("create_record", table_name,
                       f"HTTP error creating record",
                       status_code=resp.status_code, body=resp.text)

    body_resp = resp.json()
    if not body_resp.get("success"):
        raise APIError("create_record", table_name,
                       f"API success=False: {body_resp.get('message', body_resp)}")

    return body_resp.get("data") or {}
