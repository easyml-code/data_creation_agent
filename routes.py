"""
API routes — single Blueprint, registered in main.py.
"""

from flask import Blueprint, request, jsonify

from pipeline import run_pipeline
from api.exceptions import APIError, ValidationError
from api.exceptions import LookupError as MasterLookupError

bp = Blueprint("invoice", __name__, url_prefix="/api/v1")


@bp.route("/process-invoice", methods=["POST"])
def process_invoice():
    """
    POST /api/v1/process-invoice

    Request body (JSON):
    {
        "invoice_data": { ...OCR extracted invoice... }
    }

    Response (200):
    {
        "success": true,
        "data": {
            "invoice_number": "...",
            "supplier":       { ... },
            "legal_entity":   { ... },
            "po":             { ... },
            "grn_records":    [ ... ],
            "summary":        { ... }
        }
    }

    Error responses:
        400 — validation error or bad invoice data
        422 — master data lookup failed (table not seeded)
        502 — upstream API error
        500 — unexpected server error
    """
    body = request.get_json(silent=True)

    if not body:
        return _error(400, "INVALID_JSON", "Request body must be valid JSON.")

    invoice_data = body.get("invoice_data")
    if not invoice_data:
        return _error(400, "MISSING_FIELD",
                      "Request body must contain key 'invoice_data'.")

    try:
        result = run_pipeline(invoice_data)
        return jsonify({"success": True, "data": result}), 200

    except ValidationError as e:
        return _error(400, "VALIDATION_ERROR", str(e))

    except ValueError as e:
        return _error(400, "VALUE_ERROR", str(e))

    except MasterLookupError as e:
        return _error(422, "MASTER_DATA_NOT_FOUND", str(e))

    except APIError as e:
        return _error(502, "UPSTREAM_API_ERROR", str(e), {
            "step":        e.step,
            "table":       e.table,
            "http_status": e.status_code,
        })

    except Exception as e:
        # Log full traceback server-side; return sanitised message to client
        import traceback
        traceback.print_exc()
        return _error(500, "INTERNAL_ERROR",
                      f"Unexpected error: {type(e).__name__}: {e}")


@bp.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


# ── Helper ────────────────────────────────────────────────────────────────────

def _error(status: int, code: str, message: str, detail: dict = None):
    body = {"success": False, "error": {"code": code, "message": message}}
    if detail:
        body["error"]["detail"] = detail
    return jsonify(body), status
