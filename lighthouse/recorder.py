"""
Lighthouse rates flight recorder — daily snapshot of Lighthouse rates to Snowflake.

Each run captures, for every subscription (hotel) on the account:
  - Lowest Rates (/v3/rates)                       per OTA, from <lookback> days ago up to 365 days out
  - Lowest Rates per Roomtype (/v3/roomtyperates)  per OTA, same window
  - Parity rates (/v3/parities)                    from today (API does not allow past fromDate)

Results are written to Snowflake snapshot tables keyed by snapshot_date:
each run DELETEs existing rows for that snapshot_date, then INSERTs fresh data.
Also refreshes the lighthouse_hotels / lighthouse_hotel_competitors dimension tables.

Snowflake DDL: sql/lighthouse-rates-flight-recorder-tables.sql (auto-applied).

Request budget per run (per subscription, per Lighthouse API endpoint):
  /v3/rates         — max 20 requests / 24h
  /v3/roomtyperates — max 20 requests / 24h
Default shop matrix uses ~28 /v3/rates calls (los/persons/meals extended on
bookingdotcom; compsets 1+2 fetched in one call). Tune via LIGHTHOUSE_FR_* env vars.

Entry point: sync_lighthouse.py (CLI and server scheduler both call record_snapshot).
"""

import os
import json
import time
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import snowflake.connector

from config import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_AUTH_METHOD,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_PRIVATE_KEY,
    SNOWFLAKE_PRIVATE_KEY_PASSPHRASE,
    SNOWFLAKE_PRIVATE_KEY_PATH,
    SNOWFLAKE_ROLE,
    SNOWFLAKE_SCHEMA,
    SNOWFLAKE_USER,
    SNOWFLAKE_WAREHOUSE,
)
from lighthouse.client import fetch_hotels, fetch_parities, fetch_rates, fetch_roomtype_rates, log

_project_root = Path(__file__).resolve().parent.parent

# Flight recorder config (env overridable; .env loaded by config import)
LOOKBACK_DAYS = int(os.getenv("LIGHTHOUSE_FR_LOOKBACK_DAYS", "10"))
SHOP_LENGTH = int(os.getenv("LIGHTHOUSE_FR_SHOP_LENGTH", "365"))
OTAS = [s.strip() for s in os.getenv("LIGHTHOUSE_FR_OTAS", "bookingdotcom,expedia,branddotcom").split(",") if s.strip()]
COMPSET_IDS = [int(s) for s in os.getenv("LIGHTHOUSE_FR_COMPSET_IDS", "1,2").split(",") if s.strip()]
REQUEST_DELAY_SECONDS = float(os.getenv("LIGHTHOUSE_FR_REQUEST_DELAY", "0.6"))
SNOWFLAKE_UPLOAD_BATCH_SIZE = max(1, int(os.getenv("LIGHTHOUSE_FR_SNOWFLAKE_BATCH_SIZE", "500")))
MAX_RATES_REQUESTS = max(1, int(os.getenv("LIGHTHOUSE_FR_MAX_RATES_REQUESTS", "28")))


def _parse_int_list(env_key: str, default: str) -> list[int]:
    raw = os.getenv(env_key, default)
    out = []
    for part in raw.split(","):
        part = part.strip()
        if part:
            out.append(int(part))
    return out or [int(x) for x in default.split(",") if x.strip()]


def _parse_ota_set(env_key: str, default: str) -> set[str]:
    raw = os.getenv(env_key, default)
    return {s.strip() for s in raw.split(",") if s.strip()}


LOS_VALUES = _parse_int_list("LIGHTHOUSE_FR_LOS", "1,2,3,4,5")
LOS_EXTENDED_OTAS = _parse_ota_set("LIGHTHOUSE_FR_LOS_OTAS", "bookingdotcom")
PERSONS_VALUES = _parse_int_list("LIGHTHOUSE_FR_PERSONS", "1,2,3,4")
PERSONS_EXTENDED_OTAS = _parse_ota_set("LIGHTHOUSE_FR_PERSONS_OTAS", "bookingdotcom")
MEAL_TYPES = _parse_int_list("LIGHTHOUSE_FR_MEAL_TYPES", "0,1,2,3,4")
MEAL_EXTENDED_OTAS = _parse_ota_set("LIGHTHOUSE_FR_MEAL_OTAS", "bookingdotcom")

DDL_PATH = _project_root / "sql" / "lighthouse-rates-flight-recorder-tables.sql"

# Column specs: (column_name, kind) where kind drives SQL literal formatting.
_RATE_COMMON_COLS = [
    ("extract_datetime", "string"),
    ("rate_value", "number"),
    ("currency", "string"),
    ("shop_currency", "string"),
    ("cancellable", "bool"),
    ("best_flex", "bool"),
    ("vat_incl", "bool"),
    ("city_tax_incl", "bool"),
    ("other_taxes_incl", "bool"),
    ("room_name", "string"),
    ("max_persons", "int"),
    ("meal_type_included", "int"),
    ("message", "string"),
]

# Rate shops recorded per OTA: lowest (bar omitted) and best_flex (bar=true).
RATE_SHOPS: list[tuple[str, bool | None]] = [
    ("lowest", None),
    ("best_flex", True),
]

RATES_TABLE = "lighthouse_rates_flight_recorder"
RATES_COLS = [
    ("snapshot_date", "string"),
    ("subscription_id", "int"),
    ("client_hotel_id", "int"),
    ("client_hotel_name", "string"),
    ("ota", "string"),
    ("hotel_id", "int"),
    ("hotel_name", "string"),
    ("is_client", "bool"),
    ("arrival_date", "string"),
    ("los", "int"),
    ("persons", "int"),
    ("meal_type", "int"),
    ("rate_shop", "string"),
    ("room_type", "string"),
] + _RATE_COMMON_COLS
RATES_KEYS = ["snapshot_date", "subscription_id", "ota", "hotel_id", "arrival_date", "los", "persons", "meal_type", "rate_shop"]

ROOMTYPE_TABLE = "lighthouse_roomtype_rates_flight_recorder"
ROOMTYPE_COLS = RATES_COLS
ROOMTYPE_KEYS = RATES_KEYS + ["room_type"]

PARITY_TABLE = "lighthouse_parity_flight_recorder"
PARITY_COLS = [
    ("snapshot_date", "string"),
    ("subscription_id", "int"),
    ("hotel_id", "int"),
    ("hotel_name", "string"),
    ("arrival_date", "string"),
    ("los", "int"),
    ("parity_currency", "string"),
    ("ota", "string"),
    ("channel", "string"),
    ("is_baserate", "bool"),
    ("position_to_baserate", "int"),
    ("room_type", "string"),
] + _RATE_COMMON_COLS
PARITY_KEYS = ["snapshot_date", "subscription_id", "hotel_id", "arrival_date", "los", "ota", "channel"]

HOTELS_TABLE = "lighthouse_hotels"
HOTELS_COLS = [
    ("subscription_id", "int"),
    ("hotel_id", "int"),
    ("hotel_name", "string"),
    ("stars", "int"),
    ("brand_code", "string"),
    ("subscribed_features", "string"),
]
HOTELS_KEYS = ["subscription_id"]

COMPETITORS_TABLE = "lighthouse_hotel_competitors"
COMPETITORS_COLS = [
    ("subscription_id", "int"),
    ("hotel_id", "int"),
    ("compset_id", "int"),
    ("compset_name", "string"),
    ("competitor_id", "int"),
    ("competitor_name", "string"),
    ("competitor_stars", "int"),
]
COMPETITORS_KEYS = ["subscription_id", "compset_id", "competitor_id"]


# ---------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------

def _sql_val(v, kind="string"):
    """Format a value as a Snowflake SQL literal. None -> NULL; empty strings stay ''."""
    if v is None:
        return "NULL"
    if kind == "string":
        s = str(v).replace("'", "''").replace("\n", " ").replace("\r", " ").replace("\t", " ")
        s = "".join(c for c in s if ord(c) >= 32)[:4000]
        return f"'{s}'"
    if kind == "bool":
        return "TRUE" if v else "FALSE"
    if kind == "int":
        try:
            return str(int(v))
        except (TypeError, ValueError):
            return "NULL"
    if kind == "number":
        try:
            n = float(v)
            if n != n or not (float("-inf") < n < float("inf")):
                return "NULL"
            if n == int(n):
                return str(int(n))
            return f"{n:.10f}".rstrip("0").rstrip(".")
        except (TypeError, ValueError):
            return "NULL"
    return f"'{str(v)}'"


def validate_snowflake_config():
    """Validate Snowflake env vars for upload."""
    required = {
        "SNOWFLAKE_ACCOUNT": SNOWFLAKE_ACCOUNT,
        "SNOWFLAKE_USER": SNOWFLAKE_USER,
        "SNOWFLAKE_WAREHOUSE": SNOWFLAKE_WAREHOUSE,
        "SNOWFLAKE_DATABASE": SNOWFLAKE_DATABASE,
        "SNOWFLAKE_SCHEMA": SNOWFLAKE_SCHEMA,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        log("SNOWFLAKE", f"Missing env: {', '.join(missing)}. Set in .env in this project folder.")
        return False
    if SNOWFLAKE_AUTH_METHOD == "KEYPAIR":
        if not SNOWFLAKE_PRIVATE_KEY and not SNOWFLAKE_PRIVATE_KEY_PATH:
            log("SNOWFLAKE", "SNOWFLAKE_PRIVATE_KEY or SNOWFLAKE_PRIVATE_KEY_PATH required for KEYPAIR.")
            return False
        if SNOWFLAKE_PRIVATE_KEY_PATH and not Path(SNOWFLAKE_PRIVATE_KEY_PATH).exists():
            log("SNOWFLAKE", f"Private key file not found: {SNOWFLAKE_PRIVATE_KEY_PATH}")
            return False
    elif SNOWFLAKE_AUTH_METHOD == "PASSWORD":
        if not SNOWFLAKE_PASSWORD:
            log("SNOWFLAKE", "SNOWFLAKE_PASSWORD required for PASSWORD auth.")
            return False
    else:
        log("SNOWFLAKE", f"SNOWFLAKE_AUTH_METHOD must be KEYPAIR or PASSWORD, got {SNOWFLAKE_AUTH_METHOD}")
        return False
    return True


def _prepare_private_key():
    """Prepare private key bytes for Snowflake KEYPAIR auth."""
    from cryptography.hazmat.primitives import serialization
    key_content = None
    if SNOWFLAKE_PRIVATE_KEY:
        s = SNOWFLAKE_PRIVATE_KEY.strip().replace("\\n", "\n")
        if not s.startswith("-----BEGIN"):
            raise ValueError("SNOWFLAKE_PRIVATE_KEY must be PEM format")
        key_content = s.encode("utf-8")
    elif SNOWFLAKE_PRIVATE_KEY_PATH:
        with open(SNOWFLAKE_PRIVATE_KEY_PATH, "rb") as f:
            key_content = f.read()
    if not key_content:
        raise ValueError("Set SNOWFLAKE_PRIVATE_KEY or SNOWFLAKE_PRIVATE_KEY_PATH")
    passphrase = SNOWFLAKE_PRIVATE_KEY_PASSPHRASE.encode("utf-8") if SNOWFLAKE_PRIVATE_KEY_PASSPHRASE else None
    pk = serialization.load_pem_private_key(key_content, password=passphrase)
    return pk.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def get_snowflake_connection():
    """Open a Snowflake connection using env config."""
    conn_params = {
        "account": SNOWFLAKE_ACCOUNT,
        "user": SNOWFLAKE_USER,
        "warehouse": SNOWFLAKE_WAREHOUSE,
        "database": SNOWFLAKE_DATABASE,
        "schema": SNOWFLAKE_SCHEMA,
        "role": SNOWFLAKE_ROLE if SNOWFLAKE_ROLE else None,
        "login_timeout": 60,
        "network_timeout": 600,
        "insecure_mode": True,
    }
    if SNOWFLAKE_AUTH_METHOD == "KEYPAIR":
        conn_params["private_key"] = _prepare_private_key()
    else:
        conn_params["password"] = SNOWFLAKE_PASSWORD
    return snowflake.connector.connect(**conn_params)


def _dedup_rows(rows, key_cols):
    """Keep the last row per unique key tuple (MERGE requires unique source keys)."""
    by_key = {}
    for r in rows:
        by_key[tuple(r.get(k) for k in key_cols)] = r
    return list(by_key.values())


def replace_snapshot_rows(cursor, db_schema, table, col_spec, key_cols, rows, snapshot_date):
    """Delete all rows for snapshot_date, then insert fresh rows (no upsert)."""
    cursor.execute(
        f"DELETE FROM {db_schema}.{table} WHERE snapshot_date = {_sql_val(snapshot_date, 'string')}"
    )
    rows = _dedup_rows(rows, key_cols)
    if not rows:
        log("SNOWFLAKE", f"{table}: deleted snapshot_date={snapshot_date}, inserted 0 row(s)")
        return 0

    cols = [c for c, _ in col_spec]
    cols_str = ", ".join(cols)
    total = 0
    for batch_start in range(0, len(rows), SNOWFLAKE_UPLOAD_BATCH_SIZE):
        batch = rows[batch_start : batch_start + SNOWFLAKE_UPLOAD_BATCH_SIZE]
        values_str = ",\n        ".join(
            "(" + ", ".join(_sql_val(r.get(c), kind) for c, kind in col_spec) + ")" for r in batch
        )
        cursor.execute(f"INSERT INTO {db_schema}.{table} ({cols_str}) VALUES {values_str}")
        total += len(batch)
    log("SNOWFLAKE", f"{table}: deleted snapshot_date={snapshot_date}, inserted {total} row(s)")
    return total


def merge_rows(cursor, db_schema, table, col_spec, key_cols, rows):
    """MERGE rows into a snapshot table in batches. Returns row count merged."""
    rows = _dedup_rows(rows, key_cols)
    if not rows:
        return 0
    cols = [c for c, _ in col_spec]
    update_cols = [c for c in cols if c not in key_cols]
    cols_str = ", ".join(cols)
    on_str = " AND ".join(f"t.{c} = s.{c}" for c in key_cols)
    set_str = ", ".join(f"{c} = s.{c}" for c in update_cols) + ", loaded_at = CURRENT_TIMESTAMP()"
    insert_vals = ", ".join(f"s.{c}" for c in cols)

    total = 0
    for batch_start in range(0, len(rows), SNOWFLAKE_UPLOAD_BATCH_SIZE):
        batch = rows[batch_start : batch_start + SNOWFLAKE_UPLOAD_BATCH_SIZE]
        values_list = []
        for r in batch:
            parts = [_sql_val(r.get(c), kind) for c, kind in col_spec]
            values_list.append(f"({', '.join(parts)})")
        values_str = ",\n        ".join(values_list)
        cursor.execute(f"""
            MERGE INTO {db_schema}.{table} AS t
            USING (SELECT * FROM VALUES {values_str} AS v({cols_str})) AS s
            ON {on_str}
            WHEN MATCHED THEN UPDATE SET {set_str}
            WHEN NOT MATCHED THEN INSERT ({cols_str}) VALUES ({insert_vals})
        """)
        total += len(batch)
    log("SNOWFLAKE", f"{table}: merged {total} row(s)")
    return total


def replace_rows(cursor, db_schema, table, col_spec, rows, scope_col="subscription_id"):
    """
    Delete all rows for the scoped IDs and re-insert, so removed entries
    (e.g. competitors dropped from a compset) don't linger. Returns row count.
    """
    if not rows:
        return 0
    scope_ids = sorted({r.get(scope_col) for r in rows if r.get(scope_col) is not None})
    cursor.execute(f"DELETE FROM {db_schema}.{table} WHERE {scope_col} IN ({', '.join(str(i) for i in scope_ids)})")

    cols = [c for c, _ in col_spec]
    cols_str = ", ".join(cols)
    total = 0
    for batch_start in range(0, len(rows), SNOWFLAKE_UPLOAD_BATCH_SIZE):
        batch = rows[batch_start : batch_start + SNOWFLAKE_UPLOAD_BATCH_SIZE]
        values_str = ",\n        ".join(
            "(" + ", ".join(_sql_val(r.get(c), kind) for c, kind in col_spec) + ")" for r in batch
        )
        cursor.execute(f"INSERT INTO {db_schema}.{table} ({cols_str}) VALUES {values_str}")
        total += len(batch)
    log("SNOWFLAKE", f"{table}: replaced {total} row(s) for {len(scope_ids)} {scope_col}(s)")
    return total


# ---------------------------------------------------------------------------
# Flattening API records to table rows
# ---------------------------------------------------------------------------

def flatten_hotels(hotels):
    """Flatten Hotels API records to (hotel_rows, competitor_rows)."""
    hotel_rows, competitor_rows = [], []
    for h in hotels:
        sub_id = h.get("subscription_id")
        hotel_id = h.get("id")
        hotel_rows.append({
            "subscription_id": sub_id,
            "hotel_id": hotel_id,
            "hotel_name": h.get("name") or "",
            "stars": h.get("stars"),
            "brand_code": h.get("brand_code") or "",
            "subscribed_features": json.dumps(h.get("subscribed_features", []), separators=(",", ":")),
        })
        competitors_by_id = {c.get("id"): c for c in h.get("competitors", [])}
        for cs in h.get("competitor_sets", []):
            for cid in cs.get("competitor_ids", []):
                comp = competitors_by_id.get(cid, {})
                competitor_rows.append({
                    "subscription_id": sub_id,
                    "hotel_id": hotel_id,
                    "compset_id": cs.get("id"),
                    "compset_name": cs.get("name") or "",
                    "competitor_id": cid,
                    "competitor_name": comp.get("name") or "",
                    "competitor_stars": comp.get("stars"),
                })
    return hotel_rows, competitor_rows


def _rate_common_fields(r):
    return {
        "extract_datetime": r.get("extractDateTime") or None,
        "rate_value": r.get("value"),
        "currency": r.get("currency") or "",
        "shop_currency": r.get("shopCurrency") or "",
        "cancellable": bool(r.get("cancellable")),
        "best_flex": bool(r.get("best_flex")),
        "vat_incl": bool(r.get("vatIncl")),
        "city_tax_incl": bool(r.get("cityTaxIncl")),
        "other_taxes_incl": bool(r.get("otherTaxesIncl")),
        "room_name": r.get("roomName") or "",
        "max_persons": r.get("maxPersons"),
        "meal_type_included": r.get("mealTypeIncluded"),
        "message": r.get("message") or "",
    }


def flatten_rate(r, snapshot_date, subscription_id, client_hotel_id, client_hotel_name, ota,
                 rate_shop="lowest", shop_persons=2, shop_meal_type=0):
    """Flatten one Rate object (from /rates or /roomtyperates) to a table row."""
    hotel_id = r.get("hotelId")
    row = {
        "snapshot_date": snapshot_date,
        "subscription_id": subscription_id,
        "client_hotel_id": client_hotel_id,
        "client_hotel_name": client_hotel_name,
        "ota": ota,
        "hotel_id": hotel_id,
        # competitor records use "name" instead of "hotelName" in some responses
        "hotel_name": r.get("hotelName") or r.get("name") or "",
        "is_client": hotel_id == client_hotel_id,
        "arrival_date": r.get("arrivalDate"),
        "los": r.get("los") or 1,
        "persons": shop_persons,
        "meal_type": shop_meal_type,
        "rate_shop": rate_shop,
        "room_type": r.get("roomType") or "",
    }
    row.update(_rate_common_fields(r))
    return row


@dataclass(frozen=True)
class RatesFetchConfig:
    ota: str
    rate_shop: str
    bar: bool | None
    los: int
    persons: int
    meal_type: int


def _build_rates_fetch_configs(otas: list[str]) -> tuple[list[RatesFetchConfig], list[RatesFetchConfig]]:
    """
    Build /v3/rates fetch configs in priority order, then truncate to MAX_RATES_REQUESTS.
    Extended shops (LOS>1, persons!=2, meal!=0) apply only on configured OTAs (default:
    bookingdotcom). Returns (included, skipped).
    """
    seen: set[tuple] = set()
    ordered: list[RatesFetchConfig] = []

    def add(ota: str, rate_shop: str, bar: bool | None, los: int, persons: int, meal_type: int):
        key = (ota, rate_shop, los, persons, meal_type)
        if key in seen:
            return
        seen.add(key)
        ordered.append(RatesFetchConfig(ota, rate_shop, bar, los, persons, meal_type))

    for ota in otas:
        for rate_shop, bar in RATE_SHOPS:
            add(ota, rate_shop, bar, 1, 2, 0)

    for ota in otas:
        if ota not in LOS_EXTENDED_OTAS:
            continue
        for rate_shop, bar in RATE_SHOPS:
            for los in LOS_VALUES:
                if los == 1:
                    continue
                add(ota, rate_shop, bar, los, 2, 0)

    for ota in otas:
        if ota not in PERSONS_EXTENDED_OTAS:
            continue
        for rate_shop, bar in RATE_SHOPS:
            for persons in PERSONS_VALUES:
                if persons == 2:
                    continue
                add(ota, rate_shop, bar, 1, persons, 0)

    for ota in otas:
        if ota not in MEAL_EXTENDED_OTAS:
            continue
        for rate_shop, bar in RATE_SHOPS:
            for meal_type in MEAL_TYPES:
                if meal_type == 0:
                    continue
                add(ota, rate_shop, bar, 1, 2, meal_type)

    if len(ordered) <= MAX_RATES_REQUESTS:
        if len(ordered) > 20:
            log("recorder", f"Rates fetch plan has {len(ordered)} /v3/rates calls (>20/sub/day Lighthouse limit)")
        return ordered, []

    included = ordered[:MAX_RATES_REQUESTS]
    skipped = ordered[MAX_RATES_REQUESTS:]
    log("recorder", f"Rates fetch plan truncated: {len(included)} included, {len(skipped)} skipped "
                    f"(LIGHTHOUSE_FR_MAX_RATES_REQUESTS={MAX_RATES_REQUESTS})")
    return included, skipped


def flatten_parity(p, snapshot_date, subscription_id):
    """Flatten one Parity object (from /parities) to one row per inner rate (per ota/channel)."""
    rows = []
    for r in p.get("rates", []):
        row = {
            "snapshot_date": snapshot_date,
            "subscription_id": subscription_id,
            "hotel_id": p.get("hotelId"),
            "hotel_name": p.get("hotelName") or "",
            "arrival_date": p.get("arrivalDate"),
            "los": p.get("los") or 1,
            "parity_currency": p.get("currency") or "",
            "ota": r.get("ota") or "",
            "channel": r.get("channel") or "",
            "is_baserate": bool(r.get("isBaserate")),
            "position_to_baserate": r.get("positionToBaserate"),
            "room_type": r.get("roomType") or "",
        }
        row.update(_rate_common_fields(r))
        rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# Recorder run
# ---------------------------------------------------------------------------

def record_snapshot(subscription_ids=None, lookback_days=None, shop_length=None, otas=None,
                    compset_ids=None, skip_upload=False, hotels_only=False, snapshot_date=None):
    """
    Run one flight recorder snapshot: fetch hotels + rates/roomtyperates/parities for
    every subscription and write to Snowflake.
    With hotels_only=True, only the hotels dimension tables are refreshed.

    snapshot_date: YYYY-MM-DD layer key (default: today). fromDate = snapshot_date - lookback_days.
    """
    lookback_days = LOOKBACK_DAYS if lookback_days is None else lookback_days
    shop_length = SHOP_LENGTH if shop_length is None else shop_length
    otas = otas or OTAS
    compset_ids = compset_ids or COMPSET_IDS

    if snapshot_date is None:
        snap = date.today()
    elif isinstance(snapshot_date, date):
        snap = snapshot_date
    else:
        snap = date.fromisoformat(str(snapshot_date).strip())

    snapshot_date = snap.isoformat()
    from_date = (snap - timedelta(days=lookback_days)).isoformat()

    log("recorder", f"Snapshot {snapshot_date}: fromDate={from_date} shopLength={shop_length} "
                    f"otas={otas} compsets={compset_ids}")

    hotels = fetch_hotels()
    hotel_rows, competitor_rows = flatten_hotels(hotels)

    if hotels_only:
        if skip_upload:
            log("recorder", "Skipping Snowflake upload (--skip-upload)")
            return True
        return upload_to_snowflake([], [], [], hotel_rows, competitor_rows, write_snapshots=False)

    if subscription_ids:
        hotels = [h for h in hotels if h.get("subscription_id") in subscription_ids]
        log("recorder", f"Limited to {len(hotels)} subscription(s): {subscription_ids}")

    rates_configs, skipped_configs = _build_rates_fetch_configs(otas)
    if skipped_configs:
        for cfg in skipped_configs[:5]:
            log("recorder", f"  skipped rates shop: {cfg.ota} {cfg.rate_shop} los={cfg.los} "
                            f"persons={cfg.persons} meal={cfg.meal_type}")
        if len(skipped_configs) > 5:
            log("recorder", f"  ... and {len(skipped_configs) - 5} more skipped rates shops")
    log("recorder", f"Rates shops to fetch: {len(rates_configs)} (roomtyperates: {len(otas) * len(RATE_SHOPS)})")

    rates_rows, roomtype_rows, parity_rows = [], [], []
    errors = []

    for h in hotels:
        sub_id = h.get("subscription_id")
        client_id = h.get("id")
        client_name = h.get("name") or ""
        log("recorder", f"--- {client_name} (subscription {sub_id}) ---")

        for cfg in rates_configs:
            try:
                time.sleep(REQUEST_DELAY_SECONDS)
                rates = fetch_rates(
                    sub_id, ota=cfg.ota, from_date=from_date, shop_length=shop_length,
                    compset_ids=compset_ids, bar=cfg.bar, los=cfg.los,
                    persons=cfg.persons, meal_type=cfg.meal_type,
                )
                rates_rows.extend(
                    flatten_rate(
                        r, snapshot_date, sub_id, client_id, client_name, cfg.ota,
                        rate_shop=cfg.rate_shop, shop_persons=cfg.persons, shop_meal_type=cfg.meal_type,
                    )
                    for r in rates
                )
            except Exception as e:
                errors.append(
                    f"rates {sub_id} {cfg.ota} {cfg.rate_shop} los={cfg.los} "
                    f"p={cfg.persons} meal={cfg.meal_type}: {e}"
                )
                log("ERROR", errors[-1])

        for ota in otas:
            for rate_shop, bar in RATE_SHOPS:
                try:
                    time.sleep(REQUEST_DELAY_SECONDS)
                    rt_rates = fetch_roomtype_rates(
                        sub_id, ota=ota, from_date=from_date, shop_length=shop_length,
                        compset_ids=compset_ids, bar=bar, los=1, persons=2, meal_type=0,
                    )
                    roomtype_rows.extend(
                        flatten_rate(
                            r, snapshot_date, sub_id, client_id, client_name, ota,
                            rate_shop=rate_shop, shop_persons=2, shop_meal_type=0,
                        )
                        for r in rt_rates
                    )
                except Exception as e:
                    errors.append(f"roomtyperates {sub_id} {ota} {rate_shop}: {e}")
                    log("ERROR", f"roomtyperates {sub_id} {ota} {rate_shop}: {e}")

        # Parity: one call per subscription (covers all OTAs); past fromDate not allowed.
        try:
            time.sleep(REQUEST_DELAY_SECONDS)
            parities = fetch_parities(sub_id, shop_length=shop_length)
            for p in parities:
                parity_rows.extend(flatten_parity(p, snapshot_date, sub_id))
        except Exception as e:
            errors.append(f"parities {sub_id}: {e}")
            log("ERROR", f"parities {sub_id}: {e}")

    log("recorder", f"Collected rows: rates={len(rates_rows)} roomtyperates={len(roomtype_rows)} "
                    f"parities={len(parity_rows)} (errors: {len(errors)})")

    if skip_upload:
        log("recorder", "Skipping Snowflake upload (--skip-upload)")
        return len(errors) == 0

    ok = upload_to_snowflake(
        rates_rows, roomtype_rows, parity_rows, hotel_rows, competitor_rows,
        snapshot_date=snapshot_date,
    )
    return ok and len(errors) == 0


def upload_to_snowflake(
    rates_rows,
    roomtype_rows,
    parity_rows,
    hotel_rows=None,
    competitor_rows=None,
    snapshot_date=None,
    write_snapshots=True,
):
    """Create tables if needed, refresh hotel dimension tables and replace snapshot rows."""
    if not validate_snowflake_config():
        return False

    db_schema = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}"
    try:
        conn = get_snowflake_connection()
    except Exception as e:
        log("SNOWFLAKE", f"Error connecting: {e}")
        return False

    try:
        cursor = conn.cursor()
        if DDL_PATH.exists():
            try:
                cursor.execute(DDL_PATH.read_text(encoding="utf-8"), num_statements=0)
            except Exception as e:
                if "already exists" not in str(e).lower():
                    log("SNOWFLAKE", f"DDL warning: {e}")
        else:
            log("SNOWFLAKE", f"DDL file not found ({DDL_PATH}); assuming tables exist.")

        if hotel_rows:
            merge_rows(cursor, db_schema, HOTELS_TABLE, HOTELS_COLS, HOTELS_KEYS, hotel_rows)
        if competitor_rows:
            replace_rows(cursor, db_schema, COMPETITORS_TABLE, COMPETITORS_COLS, competitor_rows)

        if snapshot_date is None:
            snapshot_date = date.today().isoformat()

        if write_snapshots:
            replace_snapshot_rows(cursor, db_schema, RATES_TABLE, RATES_COLS, RATES_KEYS, rates_rows, snapshot_date)
            replace_snapshot_rows(cursor, db_schema, ROOMTYPE_TABLE, ROOMTYPE_COLS, ROOMTYPE_KEYS, roomtype_rows, snapshot_date)
            replace_snapshot_rows(cursor, db_schema, PARITY_TABLE, PARITY_COLS, PARITY_KEYS, parity_rows, snapshot_date)
        conn.commit()
        log("SNOWFLAKE", "Upload complete")
        return True
    except Exception as e:
        log("SNOWFLAKE", f"Upload error: {e}")
        return False
    finally:
        try:
            conn.close()
        except Exception:
            pass
