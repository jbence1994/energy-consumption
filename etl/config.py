import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")


def _require(key: str) -> str:
    value = os.getenv(key)
    if not value:
        raise EnvironmentError(f"Missing required environment variable: {key}")
    return value


DATA_PATH: str = _require("DATA")
BRONZE_PATH: str = _require("MEDALLION_BRONZE")
SILVER_PATH: str = _require("MEDALLION_SILVER")
GOLD_PATH: str = _require("MEDALLION_GOLD")
