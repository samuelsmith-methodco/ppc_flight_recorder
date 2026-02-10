"""
PPC Flight Recorder – config and credentials (from .env in this folder).
"""

import os
from pathlib import Path
from typing import Optional

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

# Load .env from this project folder (ppc_flight_recorder/)
_ROOT = Path(__file__).resolve().parent
_ENV_FILE = _ROOT / ".env"
if _ENV_FILE.exists() and load_dotenv:
    load_dotenv(_ENV_FILE)

# Snowflake (mirror backend: AUTH_METHOD PASSWORD | KEYPAIR; KEYPAIR avoids MFA/TOTP)
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER", "")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD", "")
SNOWFLAKE_AUTH_METHOD = (os.getenv("SNOWFLAKE_AUTH_METHOD", "KEYPAIR") or "KEYPAIR").upper()
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "")
# KEYPAIR: use SNOWFLAKE_PRIVATE_KEY (inline PEM) or SNOWFLAKE_PRIVATE_KEY_PATH (file path)
SNOWFLAKE_PRIVATE_KEY = os.getenv("SNOWFLAKE_PRIVATE_KEY", "")
SNOWFLAKE_PRIVATE_KEY_PATH = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", "")
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "")

# Google Ads
GOOGLE_ADS_DEVELOPER_TOKEN = os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN", "")
GOOGLE_ADS_CLIENT_ID = os.getenv("GOOGLE_ADS_CLIENT_ID", "")
GOOGLE_ADS_CLIENT_SECRET = os.getenv("GOOGLE_ADS_CLIENT_SECRET", "")
GOOGLE_ADS_REFRESH_TOKEN = os.getenv("GOOGLE_ADS_REFRESH_TOKEN", "")
GOOGLE_ADS_LOGIN_CUSTOMER_ID = os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "")
GOOGLE_ADS_CUSTOMER_ID = os.getenv("GOOGLE_ADS_CUSTOMER_ID", "")
GOOGLE_ADS_CUSTOMER_ID_THEPINCH = os.getenv("GOOGLE_ADS_CUSTOMER_ID_THEPINCH", "")
GOOGLE_ADS_CUSTOMER_ID_THENICKEL = os.getenv("GOOGLE_ADS_CUSTOMER_ID_THENICKEL", "")
GOOGLE_ADS_CUSTOMER_ID_THEQUOIN = os.getenv("GOOGLE_ADS_CUSTOMER_ID_THEQUOIN", "")
GOOGLE_ADS_CUSTOMER_ID_ANTHOLOGY = os.getenv("GOOGLE_ADS_CUSTOMER_ID_ANTHOLOGY", "")
GOOGLE_ADS_CUSTOMER_ID_MYROOST = os.getenv("GOOGLE_ADS_CUSTOMER_ID_MYROOST", "")

# GA4 (Apps Script)
GA4_MARKETING_API_URL = os.getenv("GA4_MARKETING_API_URL", "")

# Projects to sync (comma-separated)
PPC_PROJECTS = os.getenv("PPC_PROJECTS", "the-pinch")

# Daily sync scheduler (server only): timezone and local time (24h)
# Use IANA timezone (e.g. America/New_York for EST/EDT). Hour/minute are in that timezone.
SYNC_SCHEDULE_TIMEZONE = os.getenv("SYNC_SCHEDULE_TIMEZONE", "America/New_York")
SYNC_SCHEDULE_HOUR = int(os.getenv("SYNC_SCHEDULE_HOUR", "21"))   # default 9:30 PM EST
SYNC_SCHEDULE_MINUTE = int(os.getenv("SYNC_SCHEDULE_MINUTE", "30"))


def normalize_customer_id(customer_id: Optional[str]) -> str:
    """Normalize Google Ads customer ID for storage (no dashes)."""
    if not customer_id:
        return ""
    return (customer_id or "").replace("-", "").strip()


def get_google_ads_customer_id(project: str) -> Optional[str]:
    """Resolve Google Ads customer ID for a project name."""
    mapping = {
        "the-nickel": GOOGLE_ADS_CUSTOMER_ID_THENICKEL or GOOGLE_ADS_CUSTOMER_ID,
        "the-quoin": GOOGLE_ADS_CUSTOMER_ID_THEQUOIN or GOOGLE_ADS_CUSTOMER_ID,
        "anthology": GOOGLE_ADS_CUSTOMER_ID_ANTHOLOGY or GOOGLE_ADS_CUSTOMER_ID,
        "myroost-com": GOOGLE_ADS_CUSTOMER_ID_MYROOST or GOOGLE_ADS_CUSTOMER_ID,
        "myroost": GOOGLE_ADS_CUSTOMER_ID_MYROOST or GOOGLE_ADS_CUSTOMER_ID,
    }
    roost = [
        "roost-midtown", "roost-east-market", "roost-baltimore", "roost-washington-dc",
        "roost-tampa", "roost-cleveland", "roost-detroit", "roost-rainey", "roost-philadelphia",
    ]
    if project in roost:
        return GOOGLE_ADS_CUSTOMER_ID_MYROOST or GOOGLE_ADS_CUSTOMER_ID
    if project in mapping:
        return mapping[project] or None
    return GOOGLE_ADS_CUSTOMER_ID_THEPINCH or GOOGLE_ADS_CUSTOMER_ID
