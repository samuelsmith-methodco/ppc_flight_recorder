"""
One-shot backfill of Resy Pace Snowflake tables from Supabase CSV exports.

  python scripts/backfill_resy_from_csv.py --dir PATH_TO_CSVS
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import pandas as pd

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from resy.venues import SEED_VENUES  # noqa: E402
from snowflake_connection import get_connection  # noqa: E402

logger = logging.getLogger(__name__)


def _ts(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True, errors="coerce").dt.tz_localize(None)


def _bool(series: pd.Series) -> pd.Series:
    return series.map(
        lambda v: True
        if str(v).strip().lower() in ("true", "1", "t", "yes")
        else False
        if str(v).strip().lower() in ("false", "0", "f", "no", "")
        else None
        if pd.isna(v)
        else bool(v)
    )


def _load_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=True)


def write_replace(conn, table: str, df: pd.DataFrame) -> int:
    if df.empty:
        logger.info("%s: 0 rows — skip", table)
        return 0
    cur = conn.cursor()
    try:
        cur.execute(f"DELETE FROM {table}")
        # write_pandas wants uppercase column names matching Snowflake
        out = df.copy()
        out.columns = [c.upper() for c in out.columns]
        success, nchunks, nrows, _ = snowflake_write_pandas(conn, out, table.upper())
        if not success:
            raise RuntimeError(f"write_pandas failed for {table}")
        logger.info("%s: wrote %s rows in %s chunk(s)", table, nrows, nchunks)
        return int(nrows or 0)
    finally:
        cur.close()


def snowflake_write_pandas(conn, df: pd.DataFrame, table: str):
    from snowflake.connector.pandas_tools import write_pandas

    return write_pandas(
        conn,
        df,
        table,
        quote_identifiers=False,
        auto_create_table=False,
        use_logical_type=True,
    )


def prep_venue(path: Path) -> pd.DataFrame:
    raw = _load_csv(path)
    seed = {int(v["id"]): v for v in SEED_VENUES}
    rows = []
    for _, r in raw.iterrows():
        vid = int(r["id"])
        s = seed.get(vid, {})
        rows.append(
            {
                "id": vid,
                "resy_venue_id": r.get("resy_venue_id") or s.get("resy_venue_id"),
                "resy_slug": r.get("resy_slug") or s.get("resy_slug"),
                "name": s.get("name") or r.get("name"),
                "timezone": r.get("timezone") or s.get("timezone"),
                "active": _bool(pd.Series([r.get("active")])).iloc[0],
                "toast_restaurant_guid": s.get("toast_restaurant_guid"),
                "toast_include_rc": json.dumps(s["toast_include_rc"]) if s.get("toast_include_rc") else None,
            }
        )
    return pd.DataFrame(rows)


def prep_covers(path: Path) -> pd.DataFrame:
    raw = _load_csv(path)
    df = pd.DataFrame(
        {
            "venue_id": raw["venue_id"].astype(int),
            "service_date": pd.to_datetime(raw["service_date"], errors="coerce").dt.date,
            "service_type": raw["service_type"].astype(int),
            "total_covers": pd.to_numeric(raw["total_covers"], errors="coerce"),
            "reserved_covers": pd.to_numeric(raw["reserved_covers"], errors="coerce"),
            "walkin_covers": pd.to_numeric(raw["walkin_covers"], errors="coerce"),
            "waitlist_covers": pd.to_numeric(raw.get("waitlist_covers"), errors="coerce"),
            "no_show_covers": pd.to_numeric(raw.get("no_show_covers"), errors="coerce"),
            "source": raw.get("source", "resy_covers_report"),
        }
    )
    return df.dropna(subset=["venue_id", "service_date", "service_type"])


def prep_toast(path: Path) -> pd.DataFrame:
    raw = _load_csv(path)
    df = pd.DataFrame(
        {
            "venue_id": raw["venue_id"].astype(int),
            "business_date": pd.to_datetime(raw["business_date"], errors="coerce").dt.date,
            "service_type": raw["service_type"].astype(int),
            "orders": pd.to_numeric(raw.get("orders"), errors="coerce"),
            "covers": pd.to_numeric(raw.get("covers"), errors="coerce"),
            "net_sales": pd.to_numeric(raw.get("net_sales"), errors="coerce"),
            "dine_net": pd.to_numeric(raw.get("dine_net"), errors="coerce"),
            "avg_check_per_cover": pd.to_numeric(raw.get("avg_check_per_cover"), errors="coerce"),
            "clean_covers": pd.to_numeric(raw.get("clean_covers"), errors="coerce"),
            "clean_sales": pd.to_numeric(raw.get("clean_sales"), errors="coerce"),
            "avg_check_clean": pd.to_numeric(raw.get("avg_check_clean"), errors="coerce"),
            "avg_turn_min": pd.to_numeric(raw.get("avg_turn_min"), errors="coerce"),
            "median_turn_min": pd.to_numeric(raw.get("median_turn_min"), errors="coerce"),
        }
    )
    return df.dropna(subset=["venue_id", "business_date", "service_type"])


def prep_guest(path: Path) -> pd.DataFrame:
    raw = _load_csv(path)
    df = pd.DataFrame(
        {
            "guest_key": raw["guest_key"],
            "name": raw.get("name"),
            "email": raw.get("email"),
            "phone": raw.get("phone"),
            "vip": _bool(raw.get("vip")),
            "total_visits": pd.to_numeric(raw.get("total_visits"), errors="coerce"),
            "lifetime_res": pd.to_numeric(raw.get("lifetime_res"), errors="coerce"),
            "lifetime_walkins": pd.to_numeric(raw.get("lifetime_walkins"), errors="coerce"),
            "lifetime_cancels": pd.to_numeric(raw.get("lifetime_cancels"), errors="coerce"),
            "lifetime_noshows": pd.to_numeric(raw.get("lifetime_noshows"), errors="coerce"),
            "last_visit": pd.to_datetime(raw.get("last_visit"), errors="coerce").dt.date,
            "guest_tags": raw.get("guest_tags"),
            "visit_tags": raw.get("visit_tags"),
            "allergy_tags": raw.get("allergy_tags"),
            "special_requests": raw.get("special_requests"),
            "guest_notes": raw.get("guest_notes"),
            "bio": raw.get("bio"),
            "last_seen": _ts(raw.get("last_seen")),
        }
    )
    return df.dropna(subset=["guest_key"]).drop_duplicates(subset=["guest_key"], keep="last")


def prep_ledger(path: Path) -> pd.DataFrame:
    raw = _load_csv(path)
    df = pd.DataFrame(
        {
            "venue_id": raw["venue_id"].astype(int),
            "reservation_id": raw["reservation_id"].astype("int64"),
            "target_date": pd.to_datetime(raw["target_date"], errors="coerce").dt.date,
            "party_size": pd.to_numeric(raw.get("party_size"), errors="coerce"),
            "service": raw.get("service"),
            "res_time": raw.get("res_time"),
            "table_assignment": raw.get("table_assignment"),
            "visit_tags": raw.get("visit_tags"),
            "visit_note": raw.get("visit_note"),
            "last_seen": _ts(raw.get("last_seen")),
            "last_status": raw.get("last_status"),
            "cancelled": _bool(raw.get("cancelled")),
            "cancelled_at": _ts(raw.get("cancelled_at")),
            "vip": _bool(raw.get("vip")),
            "guest_total_visits": pd.to_numeric(raw.get("guest_total_visits"), errors="coerce"),
            "guest_key": raw.get("guest_key"),
            "first_seen": _ts(raw.get("first_seen")),
            "lead_days": pd.to_numeric(raw.get("lead_days"), errors="coerce"),
        }
    )
    return df.dropna(subset=["venue_id", "reservation_id", "target_date"]).drop_duplicates(
        subset=["venue_id", "reservation_id", "target_date"], keep="last"
    )


def prep_events(path: Path) -> pd.DataFrame:
    raw = _load_csv(path)
    df = pd.DataFrame(
        {
            "venue_id": raw["venue_id"].astype(int),
            "reservation_id": raw["reservation_id"].astype("int64"),
            "target_date": pd.to_datetime(raw["target_date"], errors="coerce").dt.date,
            "status": raw["status"],
            "observed_at": _ts(raw.get("observed_at")),
            "prev_seen_at": _ts(raw.get("prev_seen_at")),
        }
    )
    return df.dropna(subset=["venue_id", "reservation_id", "target_date", "status"]).drop_duplicates(
        subset=["venue_id", "reservation_id", "target_date", "status"], keep="first"
    )


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--dir", required=True, help="Folder with Supabase CSV exports")
    args = p.parse_args()
    d = Path(args.dir)
    files = {
        "resy_venue": ("venue_rows.csv", prep_venue),
        "resy_daily_covers_actual": ("daily_covers_actual_rows.csv", prep_covers),
        "resy_toast_daily": ("toast_daily_rows.csv", prep_toast),
        "resy_guest": ("guest_rows.csv", prep_guest),
        "resy_reservation_ledger": ("reservation_ledger_rows.csv", prep_ledger),
        "resy_reservation_status_event": ("reservation_status_event_rows.csv", prep_events),
    }
    missing = [name for name, (fn, _) in files.items() if not (d / fn).exists()]
    if missing:
        logger.error("Missing CSVs for %s in %s", missing, d)
        return 1

    with get_connection() as conn:
        # ensure tables exist
        ddl = _ROOT / "sql" / "resy-pace-flight-recorder-tables.sql"
        cur = conn.cursor()
        try:
            cur.execute(ddl.read_text(encoding="utf-8"), num_statements=0)
        finally:
            cur.close()

        totals = {}
        for table, (fn, prep) in files.items():
            logger.info("Preparing %s from %s", table, fn)
            df = prep(d / fn)
            totals[table] = write_replace(conn, table, df)
        logger.info("Backfill done: %s", totals)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
