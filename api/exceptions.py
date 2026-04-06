class APIError(Exception):
    """Raised when the remote API returns an error response."""
    def __init__(self, step: str, table: str, message: str, status_code: int = None, body: str = None):
        self.step        = step
        self.table       = table
        self.status_code = status_code
        self.body        = body
        detail = f"[{step} → {table}] {message}"
        if status_code:
            detail += f"  (HTTP {status_code})"
        if body:
            detail += f"\n  Response body: {body}"
        super().__init__(detail)


class LookupError(Exception):
    """Raised when a required master-data record is not found."""
    def __init__(self, table: str, field: str, value: str):
        self.table = table
        self.field = field
        self.value = value
        super().__init__(
            f"[Lookup] '{value}' not found in {table}.{field}. "
            f"Ensure master data is seeded before running the pipeline."
        )


class ValidationError(Exception):
    """Raised when invoice input data is missing required fields."""
    def __init__(self, field_path: str, reason: str = "is required"):
        self.field_path = field_path
        super().__init__(f"[Validation] invoice_data.{field_path} {reason}")
