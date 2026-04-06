import os
from dotenv import load_dotenv

load_dotenv()

def _require(key: str) -> str:
    value = os.getenv(key)
    if not value:
        raise EnvironmentError(f"Required env var '{key}' is not set. Check your .env file.")
    return value

BASE_URL    = _require("BASE_URL")
USAGE_URL   = _require("USAGE_URL")
API_VERSION = os.getenv("API_VERSION", "v1")
USER_ID     = _require("USER_ID")
PORT        = int(os.getenv("PORT", "5000"))