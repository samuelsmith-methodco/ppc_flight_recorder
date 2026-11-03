"""Snowflake MERGE for Resy Pace tables, plus the daily capture orchestration."""

from __future__ import annotations

import json
import logging
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from config import (
    RESY_EMAIL,
    RESY_FORCE_ZERO,
    RESY_GUEST_SALT,
    RESY_HORIZON,
    RESY_PASSWORD,
    RESY_REALIZE_LOOKBACK,
    RESY_REQUEST_DELAY_SEC,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_SCHEMA,
    TOAST_DAYS,
    TOAST_STANDARD_CLIENT_ID,
    TOAST_STANDARD_CLIENT_SECRET,
)
from resy.browser import capture_report_headers
from resy.client import (
    COVERS_REPORT,
    RESERVATIONS_REPORT,
    ResyReportAuthError,
    fetch_venues,
    post_report,
    resy_auth,
    rows_from_report,
    venue_analytics_token,
)
from resy.normalize import (
    collect_ledger,
    covers_rows,
    finalize_ledger,
    pace_row,
    parse_reservation,
    realize_row,
)
from resy.toast_client import ToastError, _auth as toast_auth
from resy.toast_client import build_toast_rows, trailing_business_dates
from resy.venues import SEED_VENUES, seed_by_id
from snowflake_connection import execute, execute_query, get_connection

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parent.parent
DDL_PATH = _ROOT / "sql" / "resy-pace-flight-recorder-tables.sql"
BATCH = 40

PACE_UPDATE = [
    "snapshot_ts", "days_out", "covers_on_books", "reservation_count", "party_size_avg",
    "is_eod", "is_realized", "source", "notes",
]
REALIZED_UPDATE = [
    "snapshot_ts", "days_out", "covers_seated", "walkin_covers", "no_show_covers",
    "cancelled_covers_cum", "is_eod", "is_realized", "source",
]
LEDGER_COLS = [
    "party_size", "service", "res_time", "table_assignment", "visit_tags", "visit_note",
    "last_seen", "last_status", "cancelled", "cancelled_at", "vip", "guest_total_visits",
    "guest_key", "first_seen", "lead_days",
]
GUEST_COLS = [
    "name", "email", "phone", "vip", "total_visits", "lifetime_res", "lifetime_walkins",
    "lifetime_cancels", "lifetime_noshows", "last_visit", "guest_tags", "visit_tags",
    "allergy_tags", "special_requests", "guest_notes", "bio", "last_seen",
]


def _sql_val(v: Any, kind: str = "string") -> str:
    if v is None:
        return "NULL"
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
        except (TypeError, ValueError):
            return "NULL"
        if n != n:
            return "NULL"
        return str(int(n)) if n == int(n) else f"{n:.4f}".rstrip("0").rstrip(".")
    s = str(v).replace("'", "''").replace("\n", " ").replace("\r", " ").replace("\t", " ")
    s = "".join(c for c in s if ord(c) >= 32)[:4000]
    return f"'{s}'"


def _kind(col: str) -> str:
    if col in {
        "venue_id", "reservation_id", "days_out", "covers_on_books", "reservation_count",
        "cancelled_covers_cum", "walkin_covers", "covers_seated", "no_show_covers",
        "party_size", "guest_total_visits", "lead_days", "total_visits", "lifetime_res",
        "lifetime_walkins", "lifetime_cancels", "lifetime_noshows", "service_type",
        "total_covers", "reserved_covers", "waitlist_covers", "orders", "covers",
        "clean_covers", "id",
    }:
        return "int"
    if col in {
        "party_size_avg", "net_sales", "dine_net", "avg_check_per_cover", "clean_sales",
        "avg_check_clean", "avg_turn_min", "median_turn_min",
    }:
        return "number"
    if col in {"is_eod", "is_realized", "cancelled", "vip", "active"}:
        return "bool"
    return "string"


def ensure_ddl(conn) -> None:
    if not DDL_PATH.exists():
        raise FileNotFoundError(DDL_PATH)
    cur = conn.cursor()
    try:
        cur.execute(DDL_PATH.read_text(encoding="utf-8"), num_statements=0)
    finally:
        cur.close()


def merge_rows(
    conn,
    table: str,
    rows: list[dict[str, Any]],
    key_cols: list[str],
    update_cols: list[str],
    *,
    insert_only: bool = False,
) -> int:
    if not rows:
        return 0
    cols = list(key_cols)
    for col in update_cols:
        if col not in cols:
            cols.append(col)
    db = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{table}"
    written = 0
    for i in range(0, len(rows), BATCH):
        chunk = rows[i : i + BATCH]
        values = []
        for row in chunk:
            values.append("(" + ", ".join(_sql_val(row.get(c), _kind(c)) for c in cols) + ")")
        col_sql = ", ".join(cols)
        on_sql = " AND ".join(f"t.{c} = s.{c}" for c in key_cols)
        if insert_only:
            action = f"WHEN NOT MATCHED THEN INSERT ({col_sql}) VALUES ({', '.join('s.' + c for c in cols)})"
        else:
            sets = ", ".join(f"{c} = s.{c}" for c in update_cols)
            sets = (sets + ", " if sets else "") + "loaded_at = CURRENT_TIMESTAMP()"
            action = (
                f"WHEN MATCHED THEN UPDATE SET {sets} "
                f"WHEN NOT MATCHED THEN INSERT ({col_sql}) VALUES ({', '.join('s.' + c for c in cols)})"
            )
        sql = f"""
            MERGE INTO {db} AS t
            USING (SELECT * FROM VALUES {', '.join(values)} AS v({col_sql})) AS s
            ON {on_sql}
            {action}
        """
        execute(conn, sql)
        written += len(chunk)
    return written


def _cell(row: dict, name: str) -> Any:
    if name in row:
        return row[name]
    up = name.upper()
    if up in row:
        return row[up]
    return None


def _as_date(v: Any) -> str:
    if hasattr(v, "isoformat"):
        return v.isoformat()[:10]
    return str(v)[:10]


def zero_guard(
    conn,
    table: str,
    rows: list[dict[str, Any]],
    key_cols: list[str],
    fields: list[str],
    *,
    force: bool,
) -> list[dict[str, Any]]:
    if not rows or force:
        return rows
    db = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{table}"
    kept: list[dict[str, Any]] = []
    dropped = 0
    for i in range(0, len(rows), 200):
        chunk = rows[i : i + 200]
        where = []
        for col in key_cols:
            vals = sorted({_sql_val(r.get(col), _kind(col)) for r in chunk})
            where.append(f"{col} IN ({', '.join(vals)})")
        sel = ", ".join(key_cols + fields)
        df = execute_query(conn, f"SELECT {sel} FROM {db} WHERE {' AND '.join(where)}")
        stored_norm: dict[tuple, dict] = {}
        for rec in df.to_dict(orient="records"):
            key = []
            for c in key_cols:
                val = _cell(rec, c)
                if hasattr(val, "isoformat") or (isinstance(val, str) and len(val) >= 10 and val[4] == "-"):
                    key.append(_as_date(val))
                else:
                    try:
                        key.append(int(val))
                    except (TypeError, ValueError):
                        key.append(val)
            stored_norm[tuple(key)] = rec
        for row in chunk:
            key = []
            for c in key_cols:
                val = row.get(c)
                if isinstance(val, str) and len(val) >= 10 and val[4] == "-":
                    key.append(val[:10])
                else:
                    try:
                        key.append(int(val)) if val is not None else key.append(val)
                    except (TypeError, ValueError):
                        key.append(val)
            prev = stored_norm.get(tuple(key))
            regress = False
            if prev:
                for field in fields:
                    if field not in row:
                        continue
                    nv, ov = row.get(field), _cell(prev, field)
                    try:
                        ov_n = float(ov) if ov is not None else None
                    except (TypeError, ValueError):
                        ov_n = None
                    if (nv is None or float(nv or 0) == 0) and ov_n not in (None, 0):
                        logger.warning(
                            "zero-guard drop %s %s %s %s -> %s",
                            table, key, field, ov, nv,
                        )
                        regress = True
                        break
            if regress:
                dropped += 1
            else:
                kept.append(row)
    if dropped:
        logger.warning("zero-guard dropped %s/%s %s rows", dropped, len(rows), table)
    return kept


def _readable_ts(column: str) -> str:
    """Bring back timestamps that were stored as nanosecond counts.

    Those values are centuries out of range. On Windows the Snowflake Arrow
    decoder then raises ``Python int too large to convert to C int`` and the
    whole venue capture stops.
    """
    return (
        f"IFF({column} IS NULL OR {column} < '2100-01-01', {column}, "
        f"TO_TIMESTAMP_NTZ(DATE_PART('epoch_nanosecond', {column}) / 1000000000000000000)) "
        f"AS {column}"
    )


def _prior_ledger(conn, venue_id: int, since: date) -> dict[tuple[str, int], dict[str, Any]]:
    db = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.resy_reservation_ledger"
    df = execute_query(
        conn,
        f"""
        SELECT reservation_id, target_date, {_readable_ts("first_seen")}, last_status,
               {_readable_ts("last_seen")}, cancelled,
               party_size, service, res_time, table_assignment, visit_tags, visit_note,
               vip, guest_total_visits, guest_key, lead_days
        FROM {db}
        WHERE venue_id = %(venue_id)s AND target_date >= %(since)s AND COALESCE(cancelled, FALSE) = FALSE
        """,
        {"venue_id": venue_id, "since": since.isoformat()},
    )
    out: dict[tuple[str, int], dict[str, Any]] = {}
    for rec in df.to_dict(orient="records"):
        target = _as_date(_cell(rec, "target_date"))
        rid = int(_cell(rec, "reservation_id"))
        out[(target, rid)] = {
            "first_seen": _cell(rec, "first_seen"),
            "last_status": _cell(rec, "last_status"),
            "last_seen": _cell(rec, "last_seen"),
            "cancelled": bool(_cell(rec, "cancelled")),
            "party_size": _cell(rec, "party_size"),
            "service": _cell(rec, "service"),
            "res_time": _cell(rec, "res_time"),
            "table_assignment": _cell(rec, "table_assignment"),
            "visit_tags": _cell(rec, "visit_tags"),
            "visit_note": _cell(rec, "visit_note"),
            "vip": _cell(rec, "vip"),
            "guest_total_visits": _cell(rec, "guest_total_visits"),
            "guest_key": _cell(rec, "guest_key"),
            "lead_days": _cell(rec, "lead_days"),
        }
    return out


def _latest_covers(conn, venue_id: int, start: date, end: date, since_snapshot: date) -> dict[str, float]:
    db = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.resy_reservation_snapshot"
    df = execute_query(
        conn,
        f"""
        SELECT target_date, snapshot_date, covers_on_books
        FROM {db}
        WHERE venue_id = %(venue_id)s
          AND target_date >= %(start)s AND target_date <= %(end)s
          AND snapshot_date >= %(since)s
        ORDER BY snapshot_date DESC
        """,
        {
            "venue_id": venue_id,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "since": since_snapshot.isoformat(),
        },
    )
    latest: dict[str, float] = {}
    for rec in df.to_dict(orient="records"):
        target = _as_date(_cell(rec, "target_date"))
        if target not in latest:
            try:
                latest[target] = float(_cell(rec, "covers_on_books") or 0)
            except (TypeError, ValueError):
                latest[target] = 0
    return latest


def _selected_venues(venue_ids: list[int] | None) -> list[dict]:
    seeds = seed_by_id()
    if not venue_ids:
        return [seeds[i] for i in sorted(seeds)]
    missing = [i for i in venue_ids if i not in seeds]
    if missing:
        raise ValueError(f"Unknown venue id(s): {missing}. Known: {sorted(seeds)}")
    return [seeds[i] for i in venue_ids]


def _venue_rows(venues: list[dict]) -> list[dict[str, Any]]:
    rows = []
    for v in venues:
        rc = v.get("toast_include_rc")
        rows.append(
            {
                "id": v["id"],
                "resy_venue_id": v["resy_venue_id"],
                "resy_slug": v["resy_slug"],
                "name": v["name"],
                "timezone": v["timezone"],
                "active": True,
                "toast_restaurant_guid": v.get("toast_restaurant_guid"),
                "toast_include_rc": json.dumps(rc) if rc else None,
            }
        )
    return rows


def _report(client, url, binds, token, slug, email, password, cache: dict[str, dict[str, str]]):
    headers = cache.get(slug)
    try:
        return post_report(client, url, binds, analytics_token=token, captured_headers=headers)
    except ResyReportAuthError:
        logger.warning("Analytics token rejected for %s; falling back to browser login", slug)
        cache[slug] = capture_report_headers(email, password, slug)
        return post_report(client, url, binds, analytics_token=None, captured_headers=cache[slug])


def _fetch_reservations(client, token, slug, email, password, cache, day: date) -> list[dict]:
    payload = _report(
        client,
        RESERVATIONS_REPORT,
        {"year": str(day.year), "dayofyear": str(day.timetuple().tm_yday)},
        token,
        slug,
        email,
        password,
        cache,
    )
    return [parse_reservation(r) for r in rows_from_report(payload)]


def run_capture(
    *,
    horizon: int | None = None,
    realize_lookback: int | None = None,
    toast_days: int | None = None,
    venue_ids: list[int] | None = None,
    dry_run: bool = False,
    skip_toast: bool = False,
    venues_only: bool = False,
    force_zero: bool | None = None,
) -> dict[str, Any]:
    if not RESY_EMAIL or not RESY_PASSWORD:
        return {"status": "error", "error": "RESY_EMAIL / RESY_PASSWORD not configured"}
    horizon = RESY_HORIZON if horizon is None else horizon
    realize_lookback = RESY_REALIZE_LOOKBACK if realize_lookback is None else realize_lookback
    toast_days = TOAST_DAYS if toast_days is None else toast_days
    force = RESY_FORCE_ZERO if force_zero is None else force_zero
    venues = _selected_venues(venue_ids)
    now = datetime.now(timezone.utc)
    stats: dict[str, Any] = {
        "status": "ok",
        "dry_run": dry_run,
        "venues": len(venues),
        "snapshots": 0,
        "ledger": 0,
        "events": 0,
        "guests": 0,
        "covers": 0,
        "toast": 0,
        "skipped_venues": [],
    }
    header_cache: dict[str, dict[str, str]] = {}
    with httpx.Client(timeout=120.0) as http:
        universal = resy_auth(RESY_EMAIL, RESY_PASSWORD, http)
        api_venues = fetch_venues(universal, http)
        stats["api_venues"] = len(api_venues)
        logger.info("Resy auth ok; %s venues visible; syncing %s", len(api_venues), [v["id"] for v in venues])
        if dry_run and venues_only:
            logger.info("DRY venues: %s", [v["name"] for v in venues])
            return stats
        if dry_run:
            for venue in venues:
                token = venue_analytics_token(universal, int(venue["resy_venue_id"]), http)
                today = datetime.now(ZoneInfo(venue["timezone"])).date()
                rows = _fetch_reservations(http, token, venue["resy_slug"], RESY_EMAIL, RESY_PASSWORD, header_cache, today)
                sample = pace_row(venue["id"], today, today, rows, is_eod=True, notes="dry_run", now=now)
                logger.info(
                    "DRY venue %s %s %s reservations covers_on_books=%s",
                    venue["id"], venue["name"], today.isoformat(), sample["covers_on_books"],
                )
                stats["snapshots"] += 1
            return stats

        if not SNOWFLAKE_ACCOUNT_READY():
            return {"status": "error", "error": "Snowflake is not configured (SNOWFLAKE_ACCOUNT / SNOWFLAKE_USER)"}

        with get_connection() as conn:
            ensure_ddl(conn)
            merge_rows(
                conn, "resy_venue", _venue_rows(venues),
                ["id"],
                ["resy_venue_id", "resy_slug", "name", "timezone", "active", "toast_restaurant_guid", "toast_include_rc"],
            )
            if venues_only:
                return stats

            for venue in venues:
                token = venue_analytics_token(universal, int(venue["resy_venue_id"]), http)
                if not token and venue["resy_slug"] not in header_cache:
                    logger.warning("No analytics token for venue %s; will try browser headers on first report", venue["id"])
                try:
                    wrote = _capture_venue(
                        conn, http, venue, token, now,
                        horizon=horizon,
                        realize_lookback=realize_lookback,
                        force=force,
                        header_cache=header_cache,
                    )
                except Exception as exc:
                    logger.exception("Venue %s capture failed: %s", venue["id"], exc)
                    stats["skipped_venues"].append(venue["id"])
                    stats["status"] = "error"
                    stats["error"] = str(exc)
                    continue
                for key in ("snapshots", "ledger", "events", "guests", "covers"):
                    stats[key] += wrote.get(key, 0)
                if wrote.get("aborted"):
                    stats["skipped_venues"].append(venue["id"])

            if not skip_toast:
                if not (TOAST_STANDARD_CLIENT_ID and TOAST_STANDARD_CLIENT_SECRET):
                    logger.warning("Toast credentials missing; skipping resy_toast_daily")
                else:
                    stats["toast"] = _capture_toast(conn, http, venues, toast_days, force)
    if stats["skipped_venues"] and stats["status"] == "ok":
        stats["status"] = "degraded"
    return stats


def SNOWFLAKE_ACCOUNT_READY() -> bool:
    from config import SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER
    return bool(SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER)


def _capture_venue(conn, http, venue, token, now, *, horizon, realize_lookback, force, header_cache) -> dict[str, int]:
    tz = ZoneInfo(venue["timezone"])
    today = datetime.now(tz).date()
    pace_rows: list[dict] = []
    realized_rows: list[dict] = []
    ledger: dict[str, dict] = {}
    guests: dict[str, dict] = {}
    seen: dict[str, set[int]] = {}
    ok_dates: set[str] = set()
    pulls_total = pulls_empty = 0
    salt = RESY_GUEST_SALT or "methodco-resy-pace-v1"

    def pull(day: date) -> list[dict]:
        time.sleep(RESY_REQUEST_DELAY_SEC)
        return _fetch_reservations(
            http, token, venue["resy_slug"], RESY_EMAIL, RESY_PASSWORD, header_cache, day
        )

    for offset in range(0, horizon + 1):
        day = today + timedelta(days=offset)
        rows = pull(day)
        pulls_total += 1
        active_covers = sum(r["ps"] for r in rows if "cancel" not in str(r.get("status") or "").lower())
        if not rows or active_covers == 0:
            pulls_empty += 1
        collect_ledger(
            venue["id"], day, rows, now=now, salt=salt, ledger=ledger, guests=guests,
            seen=seen, ok_dates=ok_dates,
        )
        row = pace_row(
            venue["id"], day, today, rows,
            is_eod=(offset == 0),
            notes="manual_run" if offset == 0 else None,
            now=now,
        )
        pace_rows.append(row)

    for offset in range(1, realize_lookback + 1):
        day = today - timedelta(days=offset)
        rows = pull(day)
        collect_ledger(
            venue["id"], day, rows, now=now, salt=salt, ledger=ledger, guests=guests,
            seen=seen, ok_dates=ok_dates,
        )
        realized_rows.append(realize_row(venue["id"], day, rows, now))

    if not force and pulls_total >= 5 and pulls_empty / pulls_total > 0.40:
        empty_targets = {r["target_date"] for r in pace_rows if not r.get("covers_on_books")}
        latest = _latest_covers(conn, venue["id"], today, today + timedelta(days=horizon), today - timedelta(days=3))
        regress = [d for d in empty_targets if latest.get(d, 0) > 0]
        if len(regress) >= 2:
            logger.error(
                "Circuit breaker venue %s: %s/%s empty and %s dates regress — skipping writes",
                venue["id"], pulls_empty, pulls_total, len(regress),
            )
            return {"aborted": 1}

    pace_rows = zero_guard(
        conn, "resy_reservation_snapshot", pace_rows,
        ["venue_id", "target_date", "snapshot_date"],
        ["covers_on_books", "reservation_count"],
        force=force,
    )
    realized_rows = zero_guard(
        conn, "resy_reservation_snapshot", realized_rows,
        ["venue_id", "target_date", "snapshot_date"],
        ["covers_seated"],
        force=force,
    )
    prior = _prior_ledger(conn, venue["id"], today - timedelta(days=max(realize_lookback, 1)))
    ledger_rows, events = finalize_ledger(ledger, prior, seen, ok_dates, venue["id"], now)

    n_snap = merge_rows(
        conn, "resy_reservation_snapshot", pace_rows,
        ["venue_id", "target_date", "snapshot_date"], PACE_UPDATE,
    )
    n_snap += merge_rows(
        conn, "resy_reservation_snapshot", realized_rows,
        ["venue_id", "target_date", "snapshot_date"], REALIZED_UPDATE,
    )
    n_led = merge_rows(
        conn, "resy_reservation_ledger", ledger_rows,
        ["venue_id", "reservation_id", "target_date"], LEDGER_COLS,
    )
    n_ev = merge_rows(
        conn, "resy_reservation_status_event", events,
        ["venue_id", "reservation_id", "target_date", "status"],
        ["observed_at", "prev_seen_at"],
        insert_only=True,
    )
    n_guest = merge_rows(conn, "resy_guest", list(guests.values()), ["guest_key"], GUEST_COLS)

    cover_out: list[dict] = []
    months = [(today.year, today.month)]
    if today.day <= 4:
        prev = today.replace(day=1) - timedelta(days=1)
        months.append((prev.year, prev.month))
    for year, month in months:
        time.sleep(RESY_REQUEST_DELAY_SEC)
        payload = _report(
            http, COVERS_REPORT, {"year": str(year), "month": str(month)},
            token, venue["resy_slug"], RESY_EMAIL, RESY_PASSWORD, header_cache,
        )
        cover_out.extend(covers_rows(venue["id"], rows_from_report(payload, "daily_covers")))
    cover_out = zero_guard(
        conn, "resy_daily_covers_actual", cover_out,
        ["venue_id", "service_date", "service_type"],
        ["total_covers", "reserved_covers"],
        force=force,
    )
    n_cov = merge_rows(
        conn, "resy_daily_covers_actual", cover_out,
        ["venue_id", "service_date", "service_type"],
        ["total_covers", "reserved_covers", "walkin_covers", "waitlist_covers", "no_show_covers", "source"],
    )
    logger.info(
        "venue %s %s: snapshots=%s ledger=%s events=%s guests=%s covers=%s",
        venue["id"], venue["name"], n_snap, n_led, n_ev, n_guest, n_cov,
    )
    return {"snapshots": n_snap, "ledger": n_led, "events": n_ev, "guests": n_guest, "covers": n_cov}


def _capture_toast(conn, http, venues, toast_days: int, force: bool) -> int:
    days = trailing_business_dates(max(toast_days, 0))
    if not days:
        return 0
    try:
        token = toast_auth(http, TOAST_STANDARD_CLIENT_ID, TOAST_STANDARD_CLIENT_SECRET)
    except ToastError as exc:
        logger.error("Toast auth failed: %s", exc)
        return 0
    total = 0
    for venue in venues:
        guid = venue.get("toast_restaurant_guid")
        if not guid:
            continue
        try:
            rows = build_toast_rows(
                venue["id"], guid, venue.get("toast_include_rc"), days, http, token,
                TOAST_STANDARD_CLIENT_ID, TOAST_STANDARD_CLIENT_SECRET,
            )
        except Exception as exc:
            logger.exception("Toast venue %s failed: %s", venue["id"], exc)
            continue
        rows = zero_guard(
            conn, "resy_toast_daily", rows,
            ["venue_id", "business_date", "service_type"],
            ["net_sales"],
            force=force,
        )
        total += merge_rows(
            conn, "resy_toast_daily", rows,
            ["venue_id", "business_date", "service_type"],
            [
                "orders", "covers", "net_sales", "dine_net", "avg_check_per_cover",
                "clean_covers", "clean_sales", "avg_check_clean", "avg_turn_min", "median_turn_min",
            ],
        )
    logger.info("toast_daily rows written: %s", total)
    return total
