"""
API routes — FastAPI router, included in main.py.
"""

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Any

from pipeline import run_pipeline
from api.exceptions import APIError, ValidationError
from api.exceptions import LookupError as MasterLookupError

router = APIRouter(prefix="/api/v1")


class InvoiceRequest(BaseModel):
    invoice_data: dict[str, Any]


@router.post("/process-invoice")
async def process_invoice(body: InvoiceRequest):
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
    try:
        result = run_pipeline(body.invoice_data)
        return {"success": True, "data": result}

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
        import traceback
        traceback.print_exc()
        return _error(500, "INTERNAL_ERROR",
                      f"Unexpected error: {type(e).__name__}: {e}")


@router.get("/health")
async def health():
    return {"status": "ok"}


# ── Helper ────────────────────────────────────────────────────────────────────

def _error(status: int, code: str, message: str, detail: dict = None):
    body = {"success": False, "error": {"code": code, "message": message}}
    if detail:
        body["error"]["detail"] = detail
    return JSONResponse(status_code=status, content=body)
