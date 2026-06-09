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
GOOGLE_ADS_CUSTOMER_ID_WM_MULHERINS_SONS = os.getenv("GOOGLE_ADS_CUSTOMER_ID_WM_MULHERINS_SONS", "")

# GA4 (Apps Script)
GA4_MARKETING_API_URL = os.getenv("GA4_MARKETING_API_URL", "")
# When True, daily (scheduled and manual) sync will fetch and save GA4 data. Default False.
SAVE_GA4_ON_DAILY_SYNC = os.getenv("SAVE_GA4_ON_DAILY_SYNC", "").strip().lower() in ("1", "true", "yes")

# Mews Connector API (PMS Flight Recorder — self-contained in this project)
MEWS_BASE_URL = os.getenv("MEWS_BASE_URL", "https://api.mews.com/api/connector/v1").rstrip("/")
MEWS_CLIENT_TOKEN = os.getenv("MEWS_CLIENT_TOKEN", "")
MEWS_ACCESS_TOKEN = os.getenv("MEWS_ACCESS_TOKEN", "")
MEWS_CLIENT_NAME = os.getenv("MEWS_CLIENT_NAME", "PPC Flight Recorder")
# Optional: pin enterprise when token is portfolio-wide (else resolved from configuration / enterprises list)
MEWS_ENTERPRISE_ID = os.getenv("MEWS_ENTERPRISE_ID", "").strip()
# requests timeout: (connect, read). Read default raised — large orderitems/bills can exceed 120s.
MEWS_REQUEST_CONNECT_TIMEOUT_SEC = int(os.getenv("MEWS_REQUEST_CONNECT_TIMEOUT_SEC", "30"))
MEWS_REQUEST_TIMEOUT_SEC = int(os.getenv("MEWS_REQUEST_TIMEOUT_SEC", "600"))
# Retries for ReadTimeout / connection drops / 429 / 5xx
MEWS_REQUEST_RETRIES = int(os.getenv("MEWS_REQUEST_RETRIES", "5"))
MEWS_RETRY_BACKOFF_SEC = float(os.getenv("MEWS_RETRY_BACKOFF_SEC", "2.0"))
MEWS_RETRY_BACKOFF_MAX_SEC = float(os.getenv("MEWS_RETRY_BACKOFF_MAX_SEC", "60.0"))
# Pagination default page size (Mews Limitation.Count)
MEWS_PAGE_SIZE = int(os.getenv("MEWS_PAGE_SIZE", "1000"))
# Max IDs per request for array parameters (ServiceIds, CustomerIds, …); Mews caps many at 1000.
MEWS_MAX_BATCH_IDS = int(os.getenv("MEWS_MAX_BATCH_IDS", "1000"))


def _mews_int_env(primary: str, default: str, *legacy_keys: str) -> int:
    """Read first non-empty env among primary, legacy keys, then default."""
    for key in (primary,) + legacy_keys:
        raw = os.getenv(key)
        if raw is not None and str(raw).strip() != "":
            return int(raw)
    return int(default)


# UTC window for UpdatedUtc: from snapshot_date 00:00 UTC minus BACK calendar days through
# the full snapshot_date (UTC). EndUtc is midnight on the next UTC day (exclusive), so the
# snapshot day is never omitted.
# Legacy: MEWS_RESERVATION_DAYS_BACK still honored if MEWS_SNAPSHOT_DAYS_BACK unset.
MEWS_SNAPSHOT_DAYS_BACK = _mews_int_env(
    "MEWS_SNAPSHOT_DAYS_BACK",
    "365",
    "MEWS_RESERVATION_DAYS_BACK",
)
# Split UpdatedUtc into N-day sub-requests (each slice ≤ Mews max 3M1D). 0 = use 90-day chunks.
MEWS_TIME_SLICE_DAYS = int(os.getenv("MEWS_TIME_SLICE_DAYS", "7"))
# availabilityBlocks/getAll: max interval 100 hours — chunk size in hours
MEWS_AVAILABILITY_BLOCK_HOURS = int(os.getenv("MEWS_AVAILABILITY_BLOCK_HOURS", "99"))
# Snowflake: max rows per write_pandas batch. Smaller batches use less temp disk during Parquet encrypt/upload
# (Errno 28 "No space left on device" often hits %TEMP%). Raise for faster loads on machines with plenty of disk.
MEWS_SNOWFLAKE_WRITE_BATCH_ROWS = int(os.getenv("MEWS_SNOWFLAKE_WRITE_BATCH_ROWS", "1000"))
# Non-core APIs (reference data, rates, customers, vouchers, …): UpdatedUtc from this calendar
# date (00:00 UTC) through the end of the sync snapshot_date (UTC), independent of
# MEWS_SNAPSHOT_DAYS_BACK. Core transactional APIs (reservations, productServiceOrders,
# orderItems, payments, bills, availabilityBlocks) still use MEWS_SNAPSHOT_DAYS_BACK.
MEWS_ALL_FETCH_DATA_START_DATE = os.getenv("MEWS_ALL_FETCH_DATA_START_DATE", "2021-01-01")

# Projects to sync (comma-separated)
PPC_PROJECTS = os.getenv("PPC_PROJECTS", "the_pinch_charleston")

# Daily sync scheduler (server only): timezone and local time (24h)
# Use IANA timezone (e.g. America/New_York for EST/EDT). Hour/minute are in that timezone.
SYNC_SCHEDULE_TIMEZONE = os.getenv("SYNC_SCHEDULE_TIMEZONE", "America/New_York")
SYNC_SCHEDULE_HOUR = int(os.getenv("SYNC_SCHEDULE_HOUR", "21"))   # default 9:30 PM EST
SYNC_SCHEDULE_MINUTE = int(os.getenv("SYNC_SCHEDULE_MINUTE", "30"))
# After PPC daily sync (server scheduler + POST /sync), run PMS Mews flight recorder (sync_mews) for the same snapshot_date.
RUN_MEWS_SYNC_AFTER_DAILY_SYNC = os.getenv("RUN_MEWS_SYNC_AFTER_DAILY_SYNC", "1").strip().lower() in (
    "1",
    "true",
    "yes",
)
# When True, Mews post-sync also writes pms_mews_*_diff_daily (same as sync_mews.py --diff).
MEWS_SYNC_DO_DIFF_ON_SCHEDULE = os.getenv("MEWS_SYNC_DO_DIFF_ON_SCHEDULE", "").strip().lower() in (
    "1",
    "true",
    "yes",
)

# ----- IDeaS G3 Flight Recorder (SFTPCloud daily archives) -----
IDEAS_SFTP_HOST = os.getenv("IDEAS_SFTP_HOST", "us-east-1.sftpcloud.io")
IDEAS_SFTP_PORT = int(os.getenv("IDEAS_SFTP_PORT", "22"))
IDEAS_FTPS_PORT = int(os.getenv("IDEAS_FTPS_PORT", "21"))
IDEAS_SFTP_USERNAME = os.getenv("IDEAS_SFTP_USERNAME", "")
IDEAS_SFTP_PASSWORD = os.getenv("IDEAS_SFTP_PASSWORD", "")
IDEAS_SFTP_REMOTE_DIR = os.getenv("IDEAS_SFTP_REMOTE_DIR", "/")
IDEAS_SFTP_PROTOCOL = os.getenv("IDEAS_SFTP_PROTOCOL", "auto").lower()
IDEAS_SFTP_MAX_RETRIES = int(os.getenv("IDEAS_SFTP_MAX_RETRIES", "3"))
IDEAS_SFTP_RETRY_BACKOFF_SEC = float(os.getenv("IDEAS_SFTP_RETRY_BACKOFF_SEC", "2.0"))

# IDeaS daily scheduler (server only): default 9:00 AM in SYNC_SCHEDULE_TIMEZONE
IDEAS_SYNC_SCHEDULE_TIMEZONE = os.getenv("IDEAS_SYNC_SCHEDULE_TIMEZONE", SYNC_SCHEDULE_TIMEZONE)
IDEAS_SYNC_SCHEDULE_HOUR = int(os.getenv("IDEAS_SYNC_SCHEDULE_HOUR", "9"))
IDEAS_SYNC_SCHEDULE_MINUTE = int(os.getenv("IDEAS_SYNC_SCHEDULE_MINUTE", "0"))
RUN_IDEAS_SYNC_ON_SCHEDULE = os.getenv("RUN_IDEAS_SYNC_ON_SCHEDULE", "1").strip().lower() in (
    "1",
    "true",
    "yes",
)


# ----- Lighthouse Rates Flight Recorder (Lighthouse API v3 -> Snowflake; self-contained) -----
LIGHTHOUSE_RATE_API_TOKEN = os.getenv("LIGHTHOUSE_RATE_API_TOKEN", "")
LIGHTHOUSE_RATE_API_BASE_URL = os.getenv("LIGHTHOUSE_RATE_API_BASE_URL", "https://api.mylighthouse.com/v3").rstrip("/")
# Lighthouse daily scheduler (server only): default 9:30 AM in SYNC_SCHEDULE_TIMEZONE.
# Lighthouse shops rates early morning (~04:00 ET), so mid-morning captures fresh extracts.
LIGHTHOUSE_SYNC_SCHEDULE_TIMEZONE = os.getenv("LIGHTHOUSE_SYNC_SCHEDULE_TIMEZONE", SYNC_SCHEDULE_TIMEZONE)
LIGHTHOUSE_SYNC_SCHEDULE_HOUR = int(os.getenv("LIGHTHOUSE_SYNC_SCHEDULE_HOUR", "9"))
LIGHTHOUSE_SYNC_SCHEDULE_MINUTE = int(os.getenv("LIGHTHOUSE_SYNC_SCHEDULE_MINUTE", "30"))
RUN_LIGHTHOUSE_SYNC_ON_SCHEDULE = os.getenv("RUN_LIGHTHOUSE_SYNC_ON_SCHEDULE", "1").strip().lower() in (
    "1",
    "true",
    "yes",
)


def normalize_customer_id(customer_id: Optional[str]) -> str:
    """Normalize Google Ads customer ID for storage (no dashes)."""
    if not customer_id:
        return ""
    return (customer_id or "").replace("-", "").strip()


def get_google_ads_customer_id(project: str) -> Optional[str]:
    """Resolve Google Ads customer ID for a project name."""
    mapping = {
        "the_nickel_hotel": GOOGLE_ADS_CUSTOMER_ID_THENICKEL or GOOGLE_ADS_CUSTOMER_ID,
        "the_quoin_hotel": GOOGLE_ADS_CUSTOMER_ID_THEQUOIN or GOOGLE_ADS_CUSTOMER_ID,
        "anthology": GOOGLE_ADS_CUSTOMER_ID_ANTHOLOGY or GOOGLE_ADS_CUSTOMER_ID,
        "myroost_com": GOOGLE_ADS_CUSTOMER_ID_MYROOST or GOOGLE_ADS_CUSTOMER_ID,
        "myroost": GOOGLE_ADS_CUSTOMER_ID_MYROOST or GOOGLE_ADS_CUSTOMER_ID,
        "wm_mulherins_sons": GOOGLE_ADS_CUSTOMER_ID_WM_MULHERINS_SONS or GOOGLE_ADS_CUSTOMER_ID,
    }
    roost = [
        "roost_midtown", "roost_east_market", "roost_baltimore", "roost_white_house",
        "roost_tampa", "roost_cleveland", "roost_detroit", "roost_rainey", "roost_philadelphia",
    ]
    if project in roost:
        return GOOGLE_ADS_CUSTOMER_ID_MYROOST or GOOGLE_ADS_CUSTOMER_ID
    if project in mapping:
        return mapping[project] or None
    return GOOGLE_ADS_CUSTOMER_ID_THEPINCH or GOOGLE_ADS_CUSTOMER_ID
