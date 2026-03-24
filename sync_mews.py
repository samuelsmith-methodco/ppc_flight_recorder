"""
PMS Mews Flight Recorder — daily snapshot + optional day-over-day diff.

  cd ppc_flight_recorder
  pip install -r requirements.txt
  copy env.example.txt to .env (MEWS_* + Snowflake)
  python sync_mews.py --date 2025-03-21
  python sync_mews.py --date 2025-03-21 --diff

Two UpdatedUtc windows:
  - ti_slices  (MEWS_SNAPSHOT_DAYS_BACK, typically 365d) — core transactional APIs:
      reservations/getAll, productServiceOrders/getAll, orderItems/getAll,
      payments/getAll, bills/getAll, availabilityBlocks/getAll.
  - all_slices (MEWS_ALL_FETCH_DATA_START_DATE → end of snapshot_date UTC) — everything else:
      services/getAll, reference data, rates, customers, vouchers, restrictions, etc.
      (Uses ``--date`` / snapshot_date, not “today” when backfilling historical snapshots.)

API chunking (Mews limits):
  - Time filters: prefer UpdatedUtc for incremental-style snapshots (not CollidingUtc / CreatedUtc / ConsumedUtc).
  - reservations/getAll/2023-06-06: UpdatedUtc chunked by MEWS_TIME_SLICE_DAYS (≤ 3M1D per request).
  - availabilityBlocks/getAll: UpdatedUtc interval ≤ 100 hours (we use 99-hour chunks).
  - Wide UpdatedUtc filters: split into MEWS_TIME_SLICE_DAYS sub-ranges (capped at Mews max 3M1D per request).
  - Snapshot window: UpdatedUtc from (snapshot_date − BACK) through the full snapshot_date (UTC); EndUtc is next UTC midnight (exclusive), so nothing on snapshot_date is omitted.
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from time import perf_counter, sleep
from typing import Any, Dict, List, Optional, Set
from zoneinfo import ZoneInfo

from config import (
    MEWS_ACCESS_TOKEN,
    MEWS_ALL_FETCH_DATA_START_DATE,
    MEWS_AVAILABILITY_BLOCK_HOURS,
    MEWS_CLIENT_TOKEN,
    MEWS_ENTERPRISE_ID,
    MEWS_MAX_BATCH_IDS,
    MEWS_PAGE_SIZE,
    MEWS_SNAPSHOT_DAYS_BACK,
    MEWS_TIME_SLICE_DAYS,
)
from mews.client import MewsApiPermissionError, MewsClient
from mews.normalize import rows_for_snapshot, singleton_row
from mews_storage import (
    append_daily_snapshot_rows,
    begin_streaming_snapshot,
    compute_diff_rows,
    load_generated_schema,
    load_latest_daily_on_or_before,
    run_diff_for_logical,
    write_daily_snapshot,
    write_diff_daily,
)
from snowflake_connection import get_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)


def _progress(sync_t0: float, msg: str, *args: Any) -> None:
    """Elapsed seconds since sync start; prefix for grep-friendly progress lines."""
    logger.info("[progress] +%6.1fs  " + msg, perf_counter() - sync_t0, *args)


# Visual log separators (terminal / log tail readability).
_LOG_VIS_W = 78


def _log_vis_line(char: str) -> None:
    logger.info(char * _LOG_VIS_W)


def _log_enterprise_begin(
    ent_idx: int, ent_total: int, enterprise_id: str, chain_id: Optional[str]
) -> None:
    _log_vis_line("=")
    logger.info(
        " Enterprise %s/%s  |  id=%s  |  chain_id=%s ",
        ent_idx + 1,
        ent_total,
        enterprise_id,
        chain_id or "(none)",
    )
    _log_vis_line("=")


def _log_enterprise_end(ent_idx: int, ent_total: int, enterprise_id: str) -> None:
    _log_vis_line("=")
    logger.info(
        " End enterprise %s/%s  |  id=%s ",
        ent_idx + 1,
        ent_total,
        enterprise_id,
    )
    _log_vis_line("=")


def _progress_api_start(sync_t0: float, msg: str, *args: Any) -> None:
    """Orchestration line only; Mews HTTP detail uses [progress] in mews/client.py (+/-/= rows)."""
    _progress(sync_t0, msg, *args)


def _progress_api_done(sync_t0: float, msg: str, *args: Any) -> None:
    _progress(sync_t0, msg, *args)


def _progress_api_note(sync_t0: float, msg: str, *args: Any) -> None:
    """Single progress line framed by + / - (e.g. skipped API)."""
    _log_vis_line("+")
    _progress(sync_t0, msg, *args)
    _log_vis_line("-")


# Mews: UpdatedUtc interval must not exceed 3M1D (≈ three months + one day).
UPDATED_UTC_MAX_INTERVAL_DAYS = 90
AVAIL_HOURS = MEWS_AVAILABILITY_BLOCK_HOURS


def effective_updated_utc_slice_days(slice_days: int) -> int:
    """MEWS_TIME_SLICE_DAYS, capped so each request stays within Mews 3M1D."""
    if slice_days <= 0:
        return UPDATED_UTC_MAX_INTERVAL_DAYS
    return min(slice_days, UPDATED_UTC_MAX_INTERVAL_DAYS)


def utc_iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def utc_iso_pricing(dt: datetime) -> str:
    """UTC instant for rates/getPricing — Mews contract: ISO 8601 in UTC (sample uses .000Z)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + ".000Z"


def _snap_end(snapshot_date: date) -> datetime:
    return datetime.combine(snapshot_date, time.max, tzinfo=timezone.utc)


def _snap_start(snapshot_date: date) -> datetime:
    return datetime.combine(snapshot_date, time.min, tzinfo=timezone.utc)


def updated_utc_span_bounds(snapshot_date: date, days_back: int) -> tuple[datetime, datetime]:
    """
    UTC half-open interval [start, end_exclusive) for Mews UpdatedUtc filters.

    - start: snapshot_date at 00:00:00 UTC, minus ``days_back`` calendar days.
    - end_exclusive: first instant of the day *after* snapshot_date (UTC), so the
      entire snapshot_date (all 24h UTC) is included; nothing on snapshot_date is cut off.
    """
    start = _snap_start(snapshot_date) - timedelta(days=days_back)
    end_exclusive = _snap_start(snapshot_date) + timedelta(days=1)
    return start, end_exclusive


def snapshot_utc_window(snapshot_date: date, days_back: int) -> Dict[str, str]:
    """Single UpdatedUtc interval covering BACK days before snapshot through end of snapshot_date (UTC)."""
    start, end_exclusive = updated_utc_span_bounds(snapshot_date, days_back)
    return {"StartUtc": utc_iso(start), "EndUtc": utc_iso(end_exclusive)}


def utc_time_slices(
    snapshot_date: date, days_back: int, slice_days: int
) -> List[Dict[str, str]]:
    """Contiguous UTC windows; each slice length ≤ 3M1D (see effective_updated_utc_slice_days)."""
    start, end_exclusive = updated_utc_span_bounds(snapshot_date, days_back)
    eff = effective_updated_utc_slice_days(slice_days)
    out: List[Dict[str, str]] = []
    cur = start
    while cur < end_exclusive:
        nxt = min(cur + timedelta(days=eff), end_exclusive)
        out.append({"StartUtc": utc_iso(cur), "EndUtc": utc_iso(nxt)})
        cur = nxt
    if not out:
        return [snapshot_utc_window(snapshot_date, days_back)]
    return out


def enterprise_time_zone_identifier(cfg: Dict[str, Any]) -> Optional[str]:
    """IANA zone from configuration/get (Enterprise.TimeZoneIdentifier) for time-unit boundaries."""
    ent = cfg.get("Enterprise") or {}
    if not isinstance(ent, dict):
        return None
    tid = ent.get("TimeZoneIdentifier") or ent.get("IanaTimeZoneId")
    if tid:
        return str(tid).strip()
    return None


def _parse_iso8601_duration_clock(s: str) -> Optional[tuple[int, int, int]]:
    """P0M0DT16H0M0S → (16, 0, 0). None if no time part."""
    m = re.search(r"T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?", s)
    if not m:
        return None
    h = int(m.group(1) or 0)
    mi = int(m.group(2) or 0)
    sec = int(float(m.group(3) or 0))
    return h, mi, sec


def _parse_simple_clock(s: str) -> Optional[tuple[int, int, int]]:
    """HH:MM or HH:MM:SS."""
    parts = s.strip().split(":")
    if len(parts) < 2:
        return None
    try:
        return int(parts[0]), int(parts[1]), int(float(parts[2])) if len(parts) > 2 else 0
    except ValueError:
        return None


def service_time_unit_start_hms(service: Optional[Dict[str, Any]]) -> tuple[int, int, int]:
    """When a new service day begins: StartTime / Data.Value.StartOffset (often P0M0DT16H0M0S)."""
    if not service:
        return 0, 0, 0
    raw = service.get("StartTime")
    if raw:
        s = str(raw)
        if "T" in s or s.startswith("P"):
            hms = _parse_iso8601_duration_clock(s)
            if hms is not None and hms != (0, 0, 0):
                return hms
        else:
            sc = _parse_simple_clock(s)
            if sc is not None:
                return sc
    data = service.get("Data") or {}
    if isinstance(data, dict):
        val = data.get("Value") or {}
        if isinstance(val, dict):
            for key in ("StartOffset", "OccupancyStartOffset"):
                off = val.get(key)
                if off:
                    hms = _parse_iso8601_duration_clock(str(off))
                    if hms is not None:
                        return hms
    return 0, 0, 0


def service_time_unit_kind(service: Optional[Dict[str, Any]]) -> str:
    if not service:
        return "day"
    data = service.get("Data") or {}
    if not isinstance(data, dict):
        return "day"
    val = data.get("Value") or {}
    if isinstance(val, dict):
        for key in ("TimeUnit", "TimeUnitPeriod"):
            t = val.get(key)
            if isinstance(t, str) and t.strip():
                return t.strip().lower()
    return "day"


def pricing_time_unit_bounds_utc(
    snapshot_date: date,
    tz_name: Optional[str],
    service: Optional[Dict[str, Any]],
) -> tuple[datetime, datetime]:
    """
    rates/getPricing (Connector API): both values are UTC instants for the **start** of the first
    and **start** of the last time unit in the interval (not end-of-day). Max span: 367h / 367d / 24mo
    per service time unit. Service StartTime / StartOffset defines the clock in enterprise TZ.
    """
    try:
        tz = ZoneInfo(tz_name) if tz_name else ZoneInfo("UTC")
    except Exception:
        tz = ZoneInfo("UTC")
    h, mi, se = service_time_unit_start_hms(service)
    kind = service_time_unit_kind(service)
    y, m = snapshot_date.year, snapshot_date.month
    if m == 12:
        last_d = date(y, 12, 31)
    else:
        last_d = date(y, m + 1, 1) - timedelta(days=1)

    first_local = datetime(y, m, 1, h, mi, se, tzinfo=tz)
    if kind == "month":
        # Single calendar month: one month-sized unit — first & last both at month start (API max 24 months)
        last_local = first_local
    else:
        # Day (etc.): last = start of the final day in the snapshot month (same clock as first)
        last_local = datetime(last_d.year, last_d.month, last_d.day, h, mi, se, tzinfo=tz)
    return (
        first_local.astimezone(timezone.utc),
        last_local.astimezone(timezone.utc),
    )


def fetch_rules_with_extent_fallback(
    client: MewsClient,
    base: Dict[str, Any],
    service_ids: List[str],
) -> List[Dict[str, Any]]:
    """rules/getAll: full Extent may 401 on RuleActions — retry without RuleActions."""
    extents: List[Dict[str, bool]] = [
        {
            "RuleActions": True,
            "Rates": True,
            "RateGroups": True,
            "ResourceCategories": True,
            "BusinessSegments": True,
        },
        {
            "Rates": True,
            "RateGroups": True,
            "ResourceCategories": True,
            "BusinessSegments": True,
        },
    ]
    for ei, ext in enumerate(extents):
        out: List[Dict[str, Any]] = []
        denied = False
        for sids in chunked_string_ids([str(s) for s in service_ids], MEWS_MAX_BATCH_IDS):
            cursor: Optional[str] = None
            while True:
                lim: Dict[str, Any] = {"Count": MEWS_PAGE_SIZE}
                if cursor:
                    lim["Cursor"] = cursor
                payload = {**base, "ServiceIds": sids, "Extent": ext, "Limitation": lim}
                try:
                    data = client.post("rules/getAll", payload)
                except MewsApiPermissionError:
                    logger.warning(
                        "rules/getAll: extent not permitted (%s/%s) — trying lighter extent",
                        ei + 1,
                        len(extents),
                    )
                    denied = True
                    break
                chunk = data.get("Rules")
                if isinstance(chunk, list):
                    out.extend([x for x in chunk if isinstance(x, dict)])
                next_cur = data.get("Cursor")
                if not next_cur:
                    break
                cursor = next_cur
                sleep(0.05)
            if denied:
                break
        if not denied:
            if ei > 0 and out:
                logger.info("rules/getAll: loaded with reduced Extent (no RuleActions)")
            return out
    logger.warning("rules/getAll skipped — token cannot use any attempted Extent")
    return []


def api_base_for_enterprise(client: MewsClient, enterprise_id: str) -> Dict[str, Any]:
    """Connector requests scoped to one property (portfolio / chain tokens)."""
    return {**client.base_payload(), "EnterpriseIds": [enterprise_id]}


def api_base_for_all_enterprises(client: MewsClient, enterprise_ids: List[str]) -> Dict[str, Any]:
    """Connector requests scoped to every property returned by enterprises/getAll (portfolio token)."""
    return {**client.base_payload(), "EnterpriseIds": list(enterprise_ids)}


def chunked_string_ids(ids: List[str], max_batch: int) -> List[List[str]]:
    if not ids:
        return []
    return [ids[i : i + max_batch] for i in range(0, len(ids), max_batch)]


def fetch_all_time_slices_batched_service_ids(
    client: MewsClient,
    endpoint: str,
    key: str,
    base: Dict[str, Any],
    service_ids: List[str],
    time_key: str,
    slices: List[Dict[str, str]],
    extra: Optional[Dict[str, Any]] = None,
    max_batch: int = MEWS_MAX_BATCH_IDS,
) -> List[Dict[str, Any]]:
    """Repeat fetch_all_time_slices for each ServiceIds chunk; merge and dedupe by Id."""
    if not service_ids:
        return []
    merged: List[Dict[str, Any]] = []
    for chunk in chunked_string_ids(service_ids, max_batch):
        body = {**base, "ServiceIds": chunk}
        if extra:
            body = {**body, **extra}
        merged.extend(client.fetch_all_time_slices(endpoint, key, body, time_key, slices))
    by_id: Dict[str, Dict[str, Any]] = {}
    for r in merged:
        if isinstance(r, dict) and r.get("Id"):
            by_id[str(r["Id"])] = r
    return list(by_id.values())


def fetch_all_batched_id_param(
    client: MewsClient,
    endpoint: str,
    list_key: str,
    base: Dict[str, Any],
    ids: List[str],
    param_name: str,
    max_batch: int = MEWS_MAX_BATCH_IDS,
) -> List[Dict[str, Any]]:
    """Paginated fetch_all for each chunk of id array (Mews caps array length per request)."""
    out: List[Dict[str, Any]] = []
    for chunk in chunked_string_ids(ids, max_batch):
        out.extend(client.fetch_all(endpoint, list_key, {**base, param_name: chunk}))
    return out


def collect_enterprise_ids_for_sync(
    all_enterprises: List[Dict[str, Any]],
    cfg_fallback: Optional[Dict[str, Any]],
) -> List[str]:
    """When enterprises/getAll returns rows, sync every Id. Otherwise resolve a single id from config/env."""
    ids: List[str] = []
    for e in all_enterprises:
        if isinstance(e, dict) and e.get("Id"):
            ids.append(str(e["Id"]))
    if ids:
        return ids
    if cfg_fallback is not None:
        single = resolve_enterprise_id(cfg_fallback, all_enterprises)
        return [single] if single else []
    return []


@dataclass
class SyncContext:
    client: MewsClient
    snapshot_date: date
    enterprise_ids: List[str]
    # api_base: ClientToken + AccessToken + EnterpriseIds (all properties in one sync)
    api_base: Dict[str, Any]
    chain_id: Optional[str] = None
    time_zone_identifier: Optional[str] = None
    all_enterprises: List[Dict[str, Any]] = field(default_factory=list)
    services_rows: List[Dict[str, Any]] = field(default_factory=list)
    service_ids: List[str] = field(default_factory=list)
    bookable_service_id: Optional[str] = None
    bookable_service_ids: List[str] = field(default_factory=list)
    rate_id: Optional[str] = None
    pricing_service_id: Optional[str] = None
    services_by_id: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    resource_category_ids: List[str] = field(default_factory=list)
    resource_feature_ids: List[str] = field(default_factory=list)
    customer_ids: List[str] = field(default_factory=list)
    message_thread_ids: List[str] = field(default_factory=list)
    service_order_ids: List[str] = field(default_factory=list)
    reservation_ids: List[str] = field(default_factory=list)


def resolve_chain_id(cfg: Dict[str, Any]) -> Optional[str]:
    """Chain ID from configuration/get (Enterprise.ChainId); do not use env."""
    ent = cfg.get("Enterprise") or {}
    if isinstance(ent, dict) and ent.get("ChainId"):
        return str(ent["ChainId"])
    cid = cfg.get("ChainId")
    if cid:
        return str(cid)
    return None


def log_enterprises_list(enterprises: List[Dict[str, Any]]) -> None:
    """Log enterprise Id and Name for each row from enterprises/getAll."""
    if not enterprises:
        logger.info(
            "Enterprises list: (empty — no rows from enterprises/getAll, or token is enterprise-scoped)"
        )
        return
    logger.info("Enterprises list (%s):", len(enterprises))
    for ent in enterprises:
        if not isinstance(ent, dict):
            logger.info("  - (non-dict row)")
            continue
        eid = str(ent.get("Id") or "").strip() or "(missing Id)"
        name = ent.get("Name")
        if name is None or (isinstance(name, str) and not str(name).strip()):
            name = ent.get("LegalName") or ent.get("ShortName") or "(no name)"
        else:
            name = str(name).strip()
        logger.info("  - id=%s  name=%s", eid, name)


def fetch_enterprises_all(client: MewsClient) -> List[Dict[str, Any]]:
    """Paginated enterprises/getAll. Empty list on 401/403 (wrong token scope)."""
    try:
        return client.fetch_all("enterprises/getAll", "Enterprises", client.base_payload())
    except RuntimeError as e:
        msg = str(e)
        if "401" in msg or "403" in msg:
            logger.warning(
                "enterprises/getAll skipped (token may be enterprise-scoped): %s",
                msg[:200],
            )
            return []
        raise


def resolve_enterprise_id(cfg: Dict[str, Any], enterprises: List[Dict[str, Any]]) -> str:
    """Prefer MEWS_ENTERPRISE_ID, then configuration Enterprise.Id, then single/list match."""
    if MEWS_ENTERPRISE_ID:
        eid = MEWS_ENTERPRISE_ID.strip()
        logger.info(
            "snapshot_enterprise_id from MEWS_ENTERPRISE_ID (.env): %s",
            eid,
        )
        return eid
    ent = cfg.get("Enterprise") or {}
    if isinstance(ent, dict) and ent.get("Id"):
        eid = str(ent["Id"])
        ename = ent.get("Name") or ent.get("LegalName") or ""
        logger.info(
            "snapshot_enterprise_id from configuration/get Enterprise.Id: %s%s",
            eid,
            f" ({ename})" if ename else "",
        )
        return eid
    eid = cfg.get("EnterpriseId")
    if eid:
        eid = str(eid)
        logger.info(
            "snapshot_enterprise_id from configuration/get top-level EnterpriseId: %s",
            eid,
        )
        return eid
    if len(enterprises) == 1 and isinstance(enterprises[0], dict) and enterprises[0].get("Id"):
        eid = str(enterprises[0]["Id"])
        logger.info(
            "snapshot_enterprise_id from enterprises/getAll (single row): %s",
            eid,
        )
        return eid
    if len(enterprises) > 1:
        logger.warning(
            "Multiple enterprises returned (%s); set MEWS_ENTERPRISE_ID to choose scope for snapshot rows.",
            len(enterprises),
        )
    for row in enterprises:
        if isinstance(row, dict) and row.get("Id"):
            eid = str(row["Id"])
            logger.info(
                "snapshot_enterprise_id from enterprises/getAll (first row with Id): %s",
                eid,
            )
            return eid
    return ""


def warn_if_snapshot_enterprise_not_in_list(
    snapshot_enterprise_id: str, enterprises: List[Dict[str, Any]]
) -> None:
    """If enterprises/getAll returned rows, warn when resolved id is not among them."""
    if not snapshot_enterprise_id or not enterprises:
        return
    ids = {str(r.get("Id")) for r in enterprises if isinstance(r, dict) and r.get("Id")}
    if snapshot_enterprise_id in ids:
        return
    logger.warning(
        "snapshot_enterprise_id=%s is not in the enterprises/getAll list (%s properties). "
        "It usually comes from MEWS_ENTERPRISE_ID or configuration/get — verify .env and token scope.",
        snapshot_enterprise_id,
        len(enterprises),
    )


def fetch_services(ctx: SyncContext, ti_slices: List[Dict[str, str]], sync_t0: float) -> None:
    _progress_api_start(
        sync_t0,
        "Mews: services/getAll (%s UpdatedUtc slice(s), MEWS_ALL_FETCH_DATA_START_DATE window) …",
        len(ti_slices),
    )
    base = ctx.api_base
    rows = ctx.client.fetch_all_time_slices(
        "services/getAll",
        "Services",
        {**base, "Limitation": {"Count": MEWS_PAGE_SIZE}},
        "UpdatedUtc",
        ti_slices,
    )
    ctx.services_rows = rows
    ctx.services_by_id = {
        str(r["Id"]): r for r in rows if isinstance(r, dict) and r.get("Id")
    }
    ctx.service_ids = [r["Id"] for r in rows if isinstance(r, dict) and r.get("Id")]
    ctx.bookable_service_ids = []
    ctx.bookable_service_id = None
    for r in rows:
        if not isinstance(r, dict):
            continue
        data = r.get("Data") or {}
        if isinstance(data, dict) and data.get("Discriminator") == "Bookable":
            bid = r.get("Id")
            if bid:
                ctx.bookable_service_ids.append(str(bid))
    if ctx.bookable_service_ids:
        ctx.bookable_service_id = ctx.bookable_service_ids[0]
    _progress_api_done(
        sync_t0,
        "services/getAll done: %s service(s), bookable_service_id=%s (count=%s)",
        len(ctx.service_ids),
        ctx.bookable_service_id or "(none)",
        len(ctx.bookable_service_ids),
    )
    if not ctx.service_ids:
        logger.warning(
            "services/getAll returned no rows in this UpdatedUtc window — "
            "raise MEWS_SNAPSHOT_DAYS_BACK if you need the full active service list."
        )
    # Do not fall back to service_ids[0]: getAvailability only accepts a bookable service; other
    # types (e.g. Event) return HTTP 400 Invalid ServiceId.


def fetch_resource_meta(ctx: SyncContext, sync_t0: float) -> None:
    base = ctx.api_base
    if not ctx.service_ids:
        return
    _progress_api_start(sync_t0, "Mews: resourceCategories/getAll …")
    rcat: List[Dict[str, Any]] = []
    for chunk in chunked_string_ids([str(s) for s in ctx.service_ids], MEWS_MAX_BATCH_IDS):
        rcat.extend(
            ctx.client.fetch_all(
                "resourceCategories/getAll",
                "ResourceCategories",
                {**base, "ServiceIds": chunk},
            )
        )
    ctx.resource_category_ids = [r["Id"] for r in rcat if isinstance(r, dict) and r.get("Id")]
    _progress_api_done(sync_t0, "resourceCategories/getAll: %s row(s)", len(rcat))

    _progress_api_start(sync_t0, "Mews: resourceFeatures/getAll …")
    rfeat: List[Dict[str, Any]] = []
    for chunk in chunked_string_ids([str(s) for s in ctx.service_ids], MEWS_MAX_BATCH_IDS):
        rfeat.extend(
            ctx.client.fetch_all(
                "resourceFeatures/getAll",
                "ResourceFeatures",
                {**base, "ServiceIds": chunk},
            )
        )
    ctx.resource_feature_ids = [r["Id"] for r in rfeat if isinstance(r, dict) and r.get("Id")]
    _progress_api_done(sync_t0, "resourceFeatures/getAll: %s row(s)", len(rfeat))

    _progress(
        sync_t0,
        "resource meta: %s categories, %s features",
        len(ctx.resource_category_ids),
        len(ctx.resource_feature_ids),
    )


def fetch_rates_for_pricing(ctx: SyncContext, ti_slices: List[Dict[str, str]], sync_t0: float) -> None:
    base = ctx.api_base
    if not ctx.service_ids:
        _progress_api_note(sync_t0, "rates/getAll (pricing): skipped (no services)")
        return
    _progress_api_start(sync_t0, "Mews: rates/getAll for pricing (%s UpdatedUtc slice(s)) …", len(ti_slices))
    rates: List[Dict[str, Any]] = []
    for chunk in chunked_string_ids([str(s) for s in ctx.service_ids], MEWS_MAX_BATCH_IDS):
        rates.extend(
            ctx.client.fetch_all_time_slices(
                "rates/getAll",
                "Rates",
                {
                    **base,
                    "ServiceIds": chunk,
                    "Extent": {
                        "Rates": True,
                        "RateGroups": True,
                        "AvailabilityBlockAssignments": True,
                    },
                    "Limitation": {"Count": MEWS_PAGE_SIZE},
                },
                "UpdatedUtc",
                ti_slices,
            )
        )
    by_rate: Dict[str, Dict[str, Any]] = {}
    for r in rates:
        if isinstance(r, dict) and r.get("Id"):
            by_rate[str(r["Id"])] = r
    rates = list(by_rate.values())
    if rates and rates[0].get("Id"):
        ctx.rate_id = rates[0]["Id"]
        sid = rates[0].get("ServiceId") or ctx.bookable_service_id
        ctx.pricing_service_id = str(sid) if sid else None
    _progress_api_done(
        sync_t0,
        "rates/getAll (pricing): %s rate row(s), rate_id=%s",
        len(rates),
        ctx.rate_id or "(none)",
    )


def fetch_reservations_updated_utc(
    ctx: SyncContext, ti_slices: List[Dict[str, str]]
) -> List[Dict[str, Any]]:
    """reservations/getAll/2023-06-06 — UpdatedUtc over MEWS_SNAPSHOT_DAYS_BACK + snapshot day (same slice list as other entities)."""
    base = ctx.api_base
    return ctx.client.fetch_all_time_slices(
        "reservations/getAll/2023-06-06",
        "Reservations",
        {
            **base,
            "Extent": {
                "Reservations": True,
                "ReservationGroups": False,
                "Resources": False,
                "Customers": False,
            },
        },
        "UpdatedUtc",
        ti_slices,
    )


def availability_block_chunks(ctx: SyncContext) -> List[Dict[str, Any]]:
    """availabilityBlocks/getAll: UpdatedUtc interval must not exceed 100 hours."""
    base = ctx.api_base
    start, end_exclusive = updated_utc_span_bounds(ctx.snapshot_date, MEWS_SNAPSHOT_DAYS_BACK)
    out: List[Dict[str, Any]] = []
    cur = start
    while cur < end_exclusive:
        nxt = min(cur + timedelta(hours=AVAIL_HOURS), end_exclusive)
        body = {
            **base,
            "UpdatedUtc": {"StartUtc": utc_iso(cur), "EndUtc": utc_iso(nxt)},
            "Extent": {
                "AvailabilityBlocks": True,
                "Adjustments": True,
                "ServiceOrders": False,
                "Rates": False,
            },
        }
        out.extend(ctx.client.fetch_all("availabilityBlocks/getAll", "AvailabilityBlocks", body))
        cur = nxt
    return out


def _persist_entity_rows(
    logical: str,
    rows: List[Dict[str, Any]],
    snapshot_date: date,
    enterprise_id: Optional[str],
    conn,
    do_diff: bool,
    as_of: bool,
) -> None:
    if not rows:
        return
    if as_of:
        t0 = perf_counter()
        logger.info("[progress] Snowflake: %s — as-of diff (load latest prior snapshot) …", logical)
        prev_df = load_latest_daily_on_or_before(
            conn, logical, snapshot_date - timedelta(days=1), enterprise_id
        )
        has_prior = prev_df is not None and not prev_df.empty
        diff_rows = compute_diff_rows(logical, rows, prev_df) if has_prior else []
        if not has_prior or diff_rows:
            logger.info(
                "[progress] Snowflake: %s — %s → write snapshot (%s row(s))%s",
                logical,
                "no prior snapshot" if not has_prior else f"{len(diff_rows)} change(s)",
                len(rows),
                (
                    f" + diff ({len(diff_rows)} change(s))"
                    if (diff_rows and do_diff)
                    else ""
                ),
            )
            write_daily_snapshot(conn, logical, rows, snapshot_date, enterprise_id)
            if diff_rows and do_diff:
                write_diff_daily(conn, logical, diff_rows, snapshot_date, enterprise_id)
        else:
            logger.info(
                "[progress] Snowflake: %s — no changes vs prior snapshot → skip write (%.1fs)",
                logical,
                perf_counter() - t0,
            )
        return

    t_wr = perf_counter()
    logger.info("[progress] Snowflake: %s — DELETE prior snapshot + write %s row(s) …", logical, len(rows))
    write_daily_snapshot(conn, logical, rows, snapshot_date, enterprise_id)
    logger.info(
        "[progress] Snowflake: %s — write done in %.1fs",
        logical,
        perf_counter() - t_wr,
    )
    if do_diff and rows:
        t_df = perf_counter()
        logger.info("[progress] Snowflake: %s — diff vs prior day …", logical)
        run_diff_for_logical(conn, logical, rows, snapshot_date, enterprise_id)
        logger.info("[progress] Snowflake: %s — diff done in %.1fs", logical, perf_counter() - t_df)


def run_order_items_streaming_to_snowflake(
    conn: Optional[Any],
    client: MewsClient,
    base: Dict[str, Any],
    ti_slices: List[Dict[str, str]],
    ctx: SyncContext,
    snapshot_date: date,
    dry_run: bool,
    no_snowflake: bool,
    fb_eid: Optional[str],
    sync_t0: float,
) -> None:
    """orderitems/getAll: DELETE once per enterprise, then append each API page (low RAM).

    Deduplicates by OrderItem Id across pages/slices (first occurrence wins).
    """
    logical = "order_items"
    schema = load_generated_schema().get(logical, [])
    dry = bool(dry_run or no_snowflake or conn is None)
    seen_ids: Set[str] = set()
    api_row_count = 0

    def on_page(page: List[Dict[str, Any]]) -> None:
        nonlocal api_row_count
        if dry:
            api_row_count += sum(1 for x in page if isinstance(x, dict))
            return
        recs: List[Dict[str, Any]] = []
        for r in page:
            if not isinstance(r, dict) or not r.get("Id"):
                continue
            rid = str(r["Id"])
            if rid in seen_ids:
                continue
            seen_ids.add(rid)
            recs.append(r)
        if not recs:
            return
        rows = rows_for_snapshot(
            recs,
            schema,
            snapshot_date,
            None,
            enterprise_id_from_record=True,
            services_by_id=ctx.services_by_id,
            fallback_enterprise_id=fb_eid,
        )
        by_e: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for row in rows:
            eid = row.get("enterprise_id")
            if eid:
                by_e[str(eid)].append(row)
        for eid, sub in by_e.items():
            n = append_daily_snapshot_rows(conn, logical, sub, snapshot_date, eid)
            if n:
                _progress(
                    sync_t0,
                    "Snowflake: %s — appended %s row(s) enterprise_id=%s (streaming)",
                    logical,
                    n,
                    eid,
                )

    if not dry:
        begin_streaming_snapshot(conn, logical, snapshot_date, list(ctx.enterprise_ids))
        _progress(
            sync_t0,
            "Snowflake: %s — streaming load: DELETE per enterprise done; fetching orderitems pages…",
            logical,
        )

    client.fetch_all_time_slices(
        "orderitems/getAll",
        "OrderItems",
        {**base, "AccountingStates": ["Open", "Closed"]},
        "UpdatedUtc",
        ti_slices,
        on_page=on_page,
    )

    if dry:
        logger.info("[progress] DRY: %s — counted ~%s API row(s) (streaming, no Snowflake)", logical, api_row_count)
    else:
        _progress(
            sync_t0,
            "Snowflake: %s — streaming load finished (unique Id(s) ≈ %s)",
            logical,
            len(seen_ids),
        )


def save_entity(
    logical: str,
    records: List[Dict[str, Any]],
    snapshot_date: date,
    enterprise_id: Optional[str],
    conn,
    do_diff: bool,
    dry_run: bool,
    no_snowflake: bool,
    singleton: bool = False,
    as_of: bool = False,
    enterprise_id_from_record: bool = False,
    services_by_id: Optional[Dict[str, Dict[str, Any]]] = None,
    fallback_enterprise_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Write daily snapshot (and optional diff).

    as_of=True  — "change-only" pattern (like keyword/neg-keyword tables):
        1. Load the latest prior snapshot on or before yesterday.
        2. Compute diff.  If no prior exists OR diff is non-empty → write the
           snapshot for today AND the diff rows.  If identical → skip both.
    as_of=False — always write today's snapshot; diff is controlled by do_diff.

    enterprise_id_from_record — set Snowflake enterprise_id from each Mews row (multi-enterprise
    Connector scope); group rows and persist once per enterprise.
    """
    schema = load_generated_schema().get(logical, [])
    if singleton and records and isinstance(records[0], dict):
        rows = singleton_row(
            records[0],
            schema,
            snapshot_date,
            enterprise_id,
            enterprise_id_from_record=enterprise_id_from_record,
            services_by_id=services_by_id,
            fallback_enterprise_id=fallback_enterprise_id,
        )
    else:
        rows = rows_for_snapshot(
            records,
            schema,
            snapshot_date,
            enterprise_id,
            singleton=singleton,
            enterprise_id_from_record=enterprise_id_from_record,
            services_by_id=services_by_id,
            fallback_enterprise_id=fallback_enterprise_id,
        )
    if dry_run or no_snowflake:
        logger.info("DRY: %s rows=%s", logical, len(rows))
        return rows

    if enterprise_id_from_record:
        by_e: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for row in rows:
            eid = row.get("enterprise_id")
            if eid is None or str(eid).strip() == "":
                logger.warning(
                    "save_entity(%s): row missing enterprise_id (entity_id=%s), skipped",
                    logical,
                    row.get("entity_id"),
                )
                continue
            by_e[str(eid)].append(row)
        for eid in sorted(by_e.keys()):
            _persist_entity_rows(
                logical, by_e[eid], snapshot_date, eid, conn, do_diff, as_of
            )
        return rows

    _persist_entity_rows(logical, rows, snapshot_date, enterprise_id, conn, do_diff, as_of)
    return rows


def run_sync(
    snapshot_date: date,
    do_diff: bool,
    dry_run: bool,
    no_snowflake: bool,
) -> None:
    if not MEWS_CLIENT_TOKEN or not MEWS_ACCESS_TOKEN:
        logger.error("Set MEWS_CLIENT_TOKEN and MEWS_ACCESS_TOKEN in .env")
        sys.exit(1)

    sync_t0 = perf_counter()
    _log_vis_line("#")
    _progress(
        sync_t0,
        "starting sync snapshot_date=%s dry_run=%s no_snowflake=%s",
        snapshot_date,
        dry_run,
        no_snowflake,
    )
    _log_vis_line("#")

    client = MewsClient()
    _progress_api_start(sync_t0, "Mews: enterprises/getAll …")
    all_enterprises = fetch_enterprises_all(client)
    _progress_api_done(sync_t0, "enterprises/getAll: %s row(s)", len(all_enterprises))

    cfg_fallback: Optional[Dict[str, Any]] = None
    if not all_enterprises:
        _progress_api_start(
            sync_t0,
            "configuration/get (no enterprises list — enterprise-scoped token?) …",
        )
        cfg_fallback = client.post_traced("configuration/get", client.base_payload())
        _progress_api_done(sync_t0, "configuration/get done")

    enterprise_ids = collect_enterprise_ids_for_sync(all_enterprises, cfg_fallback)
    if not enterprise_ids:
        logger.error(
            "No enterprises to sync (enterprises/getAll empty and could not resolve MEWS_ENTERPRISE_ID / configuration)."
        )
        sys.exit(1)

    logger.info(
        "Mews sync scope: %s enterprise(s) — %s",
        len(enterprise_ids),
        enterprise_ids,
    )
    log_enterprises_list(all_enterprises)

    _u_start, _u_end_excl = updated_utc_span_bounds(snapshot_date, MEWS_SNAPSHOT_DAYS_BACK)
    logger.info(
        "UpdatedUtc window (UTC): %s .. %s (EndUtc exclusive; full day %s included)",
        utc_iso(_u_start),
        utc_iso(_u_end_excl),
        snapshot_date.isoformat(),
    )

    ti_slices = utc_time_slices(
        snapshot_date,
        MEWS_SNAPSHOT_DAYS_BACK,
        MEWS_TIME_SLICE_DAYS,
    )
    _progress(
        sync_t0,
        "ti_slices (core APIs): %s slice(s) (MEWS_TIME_SLICE_DAYS=%s, MEWS_SNAPSHOT_DAYS_BACK=%s)",
        len(ti_slices),
        MEWS_TIME_SLICE_DAYS,
        MEWS_SNAPSHOT_DAYS_BACK,
    )

    all_fetch_start = date.fromisoformat(MEWS_ALL_FETCH_DATA_START_DATE)
    # UpdatedUtc window: from MEWS_ALL_FETCH_DATA_START_DATE (00:00 UTC) through end of snapshot_date
    # (exclusive EndUtc = midnight after snapshot). Uses snapshot_date, not the machine clock.
    if snapshot_date >= all_fetch_start:
        all_days_back = (snapshot_date - all_fetch_start).days
    else:
        logger.warning(
            "snapshot_date %s is before MEWS_ALL_FETCH_DATA_START_DATE %s — "
            "all_slices uses snapshot day only (0 days back from start)",
            snapshot_date,
            all_fetch_start,
        )
        all_days_back = 0
    all_slices = utc_time_slices(snapshot_date, all_days_back, MEWS_TIME_SLICE_DAYS)
    _all_start, _all_end = updated_utc_span_bounds(snapshot_date, all_days_back)
    logger.info(
        "all_slices (non-core APIs): %s slice(s) — UpdatedUtc %s .. %s "
        "(MEWS_ALL_FETCH_DATA_START_DATE=%s, snapshot_date=%s)",
        len(all_slices),
        utc_iso(_all_start),
        utc_iso(_all_end),
        MEWS_ALL_FETCH_DATA_START_DATE,
        snapshot_date.isoformat(),
    )

    def _run_sync_all_enterprises(conn: Optional[Any]) -> None:
        api_base = api_base_for_all_enterprises(client, enterprise_ids)
        _progress_api_start(sync_t0, "configuration/get (all EnterpriseIds) …")
        cfg = client.post_traced("configuration/get", api_base)
        _progress_api_done(sync_t0, "configuration/get OK (%s enterprise id(s) in scope)", len(enterprise_ids))
        chain_id = resolve_chain_id(cfg)
        ctx = SyncContext(
            client=client,
            snapshot_date=snapshot_date,
            enterprise_ids=list(enterprise_ids),
            api_base=api_base,
            chain_id=chain_id,
            time_zone_identifier=enterprise_time_zone_identifier(cfg),
            all_enterprises=all_enterprises,
        )
        _log_vis_line("=")
        _progress(
            sync_t0,
            "Mews sync: all enterprises in one pass — chain_id=%s enterprise count=%s",
            chain_id or "(none)",
            len(enterprise_ids),
        )
        _log_vis_line("=")
        fetch_services(ctx, all_slices, sync_t0)
        fetch_resource_meta(ctx, sync_t0)
        fetch_rates_for_pricing(ctx, all_slices, sync_t0)
        base = api_base
        fb_eid = enterprise_ids[0] if enterprise_ids else None

        def _save(
            conn,
            logical: str,
            recs: List[Dict[str, Any]],
            singleton: bool = False,
            skip_diff: bool = False,
            as_of: bool = False,
            enterprise_id_from_record: bool = True,
        ) -> None:
            save_entity(
                logical,
                recs,
                snapshot_date,
                fb_eid if not enterprise_id_from_record else None,
                conn,
                do_diff=do_diff and not skip_diff,
                dry_run=dry_run,
                no_snowflake=no_snowflake,
                singleton=singleton,
                as_of=as_of,
                enterprise_id_from_record=enterprise_id_from_record,
                services_by_id=ctx.services_by_id,
                fallback_enterprise_id=fb_eid,
            )

        def _run_body(conn) -> None:
            _log_vis_line("*")
            _progress(
                sync_t0,
                "sync body: %s — fetching each entity then Snowflake DELETE + write_pandas",
                "dry-run (no DB)" if conn is None else "Snowflake session",
            )
            _log_vis_line("*")

            _TOTAL_API_STEPS = 59
            _step_n = [0]

            def _step(label: str) -> None:
                _step_n[0] += 1
                _progress(sync_t0, "[%s/%s] %s", _step_n[0], _TOTAL_API_STEPS, label)

            # 1) Configuration (singleton = full JSON root)
            _step("configuration (save)")
            _save(conn, "configuration", [cfg], singleton=True, as_of=True)

            # 2) Reference data
            for logical, endpoint, key in [
                ("countries", "countries/getAll", "Countries"),
                ("currencies", "currencies/getAll", "Currencies"),
                ("tax_environments", "taxEnvironments/getAll", "TaxEnvironments"),
                ("taxations", "taxations/getAll", "Taxations"),
                ("languages", "languages/getAll", "Languages"),
            ]:
                _step(endpoint)
                recs = client.fetch_all(endpoint, key, base)
                _save(conn, logical, recs, as_of=True)

            # 3) Enterprises — global entity (entity_id IS the enterprise Id → no enterprise_id column)
            _step("enterprises (save)")
            save_entity(
                "enterprises", ctx.all_enterprises, snapshot_date, None, conn,
                do_diff=do_diff, dry_run=dry_run, no_snowflake=no_snowflake,
                as_of=True,
                enterprise_id_from_record=False,
            )

            for logical, endpoint, key in [
                ("companies", "companies/getAll", "Companies"),
                ("company_contracts", "companyContracts/getAll", "CompanyContracts"),
                ("departments", "departments/getAll", "Departments"),
                ("counters", "counters/getAll", "Counters"),
                ("outlets", "outlets/getAll", "Outlets"),
                ("resources", "resources/getAll", "Resources"),
            ]:
                _step(endpoint)
                recs = client.fetch_all(endpoint, key, base)
                _save(conn, logical, recs, as_of=True)

            _step("resourceBlocks/getAll (ti_slices)")
            _save(
                conn,
                "resource_blocks",
                client.fetch_all_time_slices(
                    "resourceBlocks/getAll",
                    "ResourceBlocks",
                    base,
                    "UpdatedUtc",
                    ti_slices,
                ),
                skip_diff=True,
            )
            _step("tasks/getAll (ti_slices)")
            _save(
                conn,
                "tasks",
                client.fetch_all_time_slices(
                    "tasks/getAll",
                    "Tasks",
                    base,
                    "CreatedUtc",  # API: no UpdatedUtc on tasks/getAll
                    ti_slices,
                ),
                skip_diff=True,
            )

            # 4) Services + resource graph (services already fetched in fetch_services → ctx.services_rows)
            _step("services (save from fetch_services)")
            _save(conn, "services", ctx.services_rows, as_of=True)
            _step("resourceCategories/getAll")
            if ctx.service_ids:
                rcat_merged: List[Dict[str, Any]] = []
                for chunk in chunked_string_ids([str(s) for s in ctx.service_ids], MEWS_MAX_BATCH_IDS):
                    rcat_merged.extend(
                        client.fetch_all(
                            "resourceCategories/getAll",
                            "ResourceCategories",
                            {**base, "ServiceIds": chunk},
                        )
                    )
                _save(conn, "resource_categories", rcat_merged, as_of=True)
            _step("resourceCategoryAssignments/getAll")
            if ctx.resource_category_ids:
                _save(
                    conn,
                    "resource_category_assignments",
                    fetch_all_batched_id_param(
                        client,
                        "resourceCategoryAssignments/getAll",
                        "ResourceCategoryAssignments",
                        base,
                        [str(x) for x in ctx.resource_category_ids],
                        "ResourceCategoryIds",
                    ),
                    as_of=True,
                )
            _step("resourceCategoryImageAssignments/getAll")
            if ctx.resource_category_ids:
                _save(
                    conn,
                    "resource_category_image_assignments",
                    fetch_all_batched_id_param(
                        client,
                        "resourceCategoryImageAssignments/getAll",
                        "ResourceCategoryImageAssignments",
                        base,
                        [str(x) for x in ctx.resource_category_ids],
                        "ResourceCategoryIds",
                    ),
                    as_of=True,
                )
            _step("resourceFeatures/getAll")
            if ctx.service_ids:
                rfeat_merged: List[Dict[str, Any]] = []
                for chunk in chunked_string_ids([str(s) for s in ctx.service_ids], MEWS_MAX_BATCH_IDS):
                    rfeat_merged.extend(
                        client.fetch_all(
                            "resourceFeatures/getAll",
                            "ResourceFeatures",
                            {**base, "ServiceIds": chunk},
                        )
                    )
                _save(conn, "resource_features", rfeat_merged, as_of=True)
            _step("resourceFeatureAssignments/getAll")
            if ctx.resource_feature_ids:
                _save(
                    conn,
                    "resource_feature_assignments",
                    fetch_all_batched_id_param(
                        client,
                        "resourceFeatureAssignments/getAll",
                        "ResourceFeatureAssignments",
                        base,
                        [str(x) for x in ctx.resource_feature_ids],
                        "ResourceFeatureIds",
                    ),
                    as_of=True,
                )

            # 5) Exports (getAll only — no binary)
            _step("exports/getAll")
            ex = client.post_optional_traced(
                "exports/getAll", {**base, "Limitation": {"Count": MEWS_PAGE_SIZE}}
            )
            if ex:
                _save(conn, "exports", ex.get("Exports") or [], as_of=True)

            # 6) Finance
            for logical, endpoint, key, extra in [
                ("exchange_rates", "exchangerates/getAll", "ExchangeRates", {}),
                ("cashiers", "cashiers/getAll", "Cashiers", {}),
                ("accounting_categories", "accountingcategories/getAll", "AccountingCategories", {}),
            ]:
                _step(endpoint)
                recs = client.fetch_all(endpoint, key, {**base, **extra})
                _save(conn, logical, recs, as_of=True)

            _step("cashiertransactions/getAll (all_slices)")
            _save(
                conn,
                "cashier_transactions",
                client.fetch_all_time_slices(
                    "cashiertransactions/getAll",
                    "CashierTransactions",
                    base,
                    "CreatedUtc",  # API: only CreatedUtc (no UpdatedUtc)
                    all_slices,
                ),
                as_of=True,
            )

            # Core PMS entities — ti_slices (MEWS_SNAPSHOT_DAYS_BACK window)
            _step("reservations/getAll (ti_slices)")
            res = fetch_reservations_updated_utc(ctx, ti_slices)
            _save(conn, "reservations", res, skip_diff=True)
            ctx.reservation_ids = [str(r["Id"]) for r in res if isinstance(r, dict) and r.get("Id")]
            ctx.service_order_ids.extend(ctx.reservation_ids)

            _step("productServiceOrders/getAll (ti_slices)")
            if ctx.service_ids:
                pso = fetch_all_time_slices_batched_service_ids(
                    client,
                    "productServiceOrders/getAll",
                    "ProductServiceOrders",
                    base,
                    [str(s) for s in ctx.service_ids],
                    "UpdatedUtc",
                    ti_slices,
                )
                _save(conn, "product_service_orders", pso, skip_diff=True)
                ctx.service_order_ids.extend(
                    [str(p["Id"]) for p in pso if isinstance(p, dict) and p.get("Id")]
                )

            _step("orderitems/getAll (ti_slices, page streaming → Snowflake)")
            run_order_items_streaming_to_snowflake(
                conn, client, base, ti_slices, ctx, snapshot_date, dry_run, no_snowflake, fb_eid, sync_t0
            )

            _step("payments/getAll (ti_slices)")
            _save(
                conn,
                "payments",
                client.fetch_all_time_slices(
                    "payments/getAll",
                    "Payments",
                    base,
                    "UpdatedUtc",
                    ti_slices,
                ),
                skip_diff=True,
            )

            _step("bills/getAll (ti_slices)")
            _save(
                conn,
                "bills",
                client.fetch_all_time_slices(
                    "bills/getAll",
                    "Bills",
                    base,
                    "UpdatedUtc",
                    ti_slices,
                ),
                skip_diff=True,
            )

            _step("outletitems/getAll (all_slices)")
            _save(
                conn,
                "outlet_items",
                client.fetch_all_time_slices(
                    "outletitems/getAll",
                    "OutletItems",
                    base,
                    "UpdatedUtc",
                    all_slices,
                ),
                as_of=True,
            )
            _step("creditcards/getAll (ti_slices)")
            _save(
                conn,
                "credit_cards",
                client.fetch_all_time_slices(
                    "creditcards/getAll",
                    "CreditCards",
                    base,
                    "UpdatedUtc",
                    ti_slices,
                ),
                skip_diff=True,
            )
            _step("paymentrequests/getAll")
            _save(conn,
                "payment_requests",
                client.fetch_all("paymentrequests/getAll", "PaymentRequests", base),
                as_of=True,
            )

            # 7) Customers + preauthorizations
            _step("customers/getAll (ti_slices)")
            _save(
                conn,
                "customers",
                client.fetch_all_time_slices(
                    "customers/getAll",
                    "Customers",
                    {
                        **base,
                        "ActivityStates": ["Active"],
                        "Extent": {"Customers": True, "Documents": False, "Addresses": False},
                    },
                    "UpdatedUtc",
                    ti_slices,
                ),
                skip_diff=True,
            )
            _step("customers/search")
            search_r = client.post_optional_traced(
                "customers/search",
                {
                    **base,
                    "Extent": {"Customers": True, "Documents": False, "Addresses": False},
                },
            )
            if search_r:
                cust_list = search_r.get("Customers") or []
                flat: List[Dict[str, Any]] = []
                for item in cust_list:
                    if isinstance(item, dict) and "Customer" in item:
                        flat.append(item["Customer"])
                    elif isinstance(item, dict):
                        flat.append(item)
                ctx.customer_ids = [str(c.get("Id")) for c in flat if c.get("Id")]
                _save(conn, "customers_search", flat, as_of=True)

            _step("preauthorizations/getAllByCustomers")
            if ctx.customer_ids:
                _save(
                    conn,
                    "preauthorizations",
                    fetch_all_batched_id_param(
                        client,
                        "preauthorizations/getAllByCustomers",
                        "Preauthorizations",
                        base,
                        [str(c) for c in ctx.customer_ids],
                        "CustomerIds",
                    ),
                    as_of=True,
                )

            # 8) Loyalty
            _step("loyaltyPrograms/getAll")
            _save(conn, "loyalty_programs", client.fetch_all("loyaltyPrograms/getAll", "LoyaltyPrograms", base), as_of=True)
            _step("loyaltyMemberships/getAll")
            _save(conn, "loyalty_memberships", client.fetch_all("loyaltyMemberships/getAll", "LoyaltyMemberships", base), as_of=True)

            # 9) Availability (chunked) + getAvailability singleton
            _step("availabilityBlocks/getAll (ti_slices)")
            _save(conn, "availability_blocks", availability_block_chunks(ctx), skip_diff=True)

            _step("services/getAvailability")
            if ctx.bookable_service_ids:
                day_start = _snap_start(snapshot_date) - timedelta(days=1)
                day_start = day_start.replace(hour=23, minute=0, second=0, microsecond=0)
                u = {
                    "FirstTimeUnitStartUtc": utc_iso(day_start),
                    "LastTimeUnitStartUtc": utc_iso(day_start + timedelta(days=5)),
                }
                gas: List[Dict[str, Any]] = []
                for bsid in ctx.bookable_service_ids:
                    ga = client.post_optional_traced(
                        "services/getAvailability",
                        {**base, "ServiceId": bsid, **u},
                    )
                    if ga:
                        gas.append(ga)
                if gas:
                    _save(conn, "services_get_availability", gas, singleton=len(gas) == 1, as_of=True)
                else:
                    logger.info(
                        "services/getAvailability skipped (invalid ServiceId or not allowed for this token)"
                    )

            _step("products/getAll")
            _save(conn, "products", client.fetch_all("products/getAll", "Products", base), as_of=True)
            _step("rules/getAll")
            if ctx.service_ids:
                _save(
                    conn,
                    "rules",
                    fetch_rules_with_extent_fallback(client, base, ctx.service_ids),
                    as_of=True,
                )
            _step("businessSegments/getAll")
            _save(conn, "business_segments", client.fetch_all("businessSegments/getAll", "BusinessSegments", base), as_of=True)
            _step("rates/getAll (all_slices)")
            if ctx.service_ids:
                _save(
                    conn,
                    "rates",
                    fetch_all_time_slices_batched_service_ids(
                        client,
                        "rates/getAll",
                        "Rates",
                        base,
                        [str(s) for s in ctx.service_ids],
                        "UpdatedUtc",
                        all_slices,
                        extra={
                            "Extent": {
                                "Rates": True,
                                "RateGroups": True,
                                "AvailabilityBlockAssignments": True,
                            },
                        },
                    ),
                    as_of=True,
                )
            _step("companionships/getAll (ti_slices)")
            _save(
                conn,
                "companionships",
                client.fetch_all_time_slices(
                    "companionships/getAll",
                    "Companionships",
                    base,
                    "UpdatedUtc",
                    ti_slices,
                ),
                as_of=True,
                skip_diff=True,
            )
            _step("resourceAccessTokens/getAll (all_slices)")
            _save(
                conn,
                "resource_access_tokens",
                client.fetch_all_time_slices(
                    "resourceAccessTokens/getAll",
                    "ResourceAccessTokens",
                    base,
                    "UpdatedUtc",
                    all_slices,
                ),
                as_of=True,
            )

            _step("rates/getPricing")
            if ctx.rate_id:
                psvc = (
                    ctx.services_by_id.get(ctx.pricing_service_id)
                    if ctx.pricing_service_id
                    else None
                )
                if (not psvc or service_time_unit_start_hms(psvc) == (0, 0, 0)) and ctx.bookable_service_id:
                    psvc = ctx.services_by_id.get(ctx.bookable_service_id) or psvc
                pricing_first, pricing_last = pricing_time_unit_bounds_utc(
                    snapshot_date, ctx.time_zone_identifier, psvc
                )
                pu = {
                    "FirstTimeUnitStartUtc": utc_iso_pricing(pricing_first),
                    "LastTimeUnitStartUtc": utc_iso_pricing(pricing_last),
                }
                pr = client.post_optional_traced(
                    "rates/getPricing", {**base, "RateId": ctx.rate_id, **pu}
                )
                if pr:
                    _save(conn, "rates_get_pricing", [pr], singleton=True, as_of=True)

            _step("restrictions/getAll (all_slices)")
            if ctx.service_ids:
                _save(
                    conn,
                    "restrictions",
                    fetch_all_time_slices_batched_service_ids(
                        client,
                        "restrictions/getAll",
                        "Restrictions",
                        base,
                        [str(s) for s in ctx.service_ids],
                        "UpdatedUtc",
                        all_slices,
                    ),
                    as_of=True,
                )
            _step("vouchers/getAll (all_slices)")
            if ctx.service_ids:
                _save(
                    conn,
                    "vouchers",
                    fetch_all_time_slices_batched_service_ids(
                        client,
                        "vouchers/getAll",
                        "Vouchers",
                        base,
                        [str(s) for s in ctx.service_ids],
                        "UpdatedUtc",
                        all_slices,
                        extra={
                            "Extent": {
                                "Vouchers": True,
                                "VoucherCodes": True,
                                "VoucherAssignments": True,
                                "Companies": False,
                                "Rates": False,
                            },
                        },
                    ),
                    as_of=True,
                )

            # 10) Messages
            _step("messageThreads/getAll (all_slices)")
            threads = client.fetch_all_time_slices(
                "messageThreads/getAll",
                "MessageThreads",
                base,
                "UpdatedUtc",
                all_slices,
            )
            _save(conn, "message_threads", threads, as_of=True)
            ctx.message_thread_ids = [t["Id"] for t in threads if isinstance(t, dict) and t.get("Id")]
            _step("messages/getAll")
            if ctx.message_thread_ids:
                _save(
                    conn,
                    "messages",
                    fetch_all_batched_id_param(
                        client,
                        "messages/getAll",
                        "Messages",
                        base,
                        [str(t) for t in ctx.message_thread_ids],
                        "MessageThreadIds",
                    ),
                    as_of=True,
                )

            # 11) Sources + reservation groups
            _step("sourceassignments/getAll (ti_slices)")
            _save(
                conn,
                "source_assignments",
                client.fetch_all_time_slices(
                    "sourceassignments/getAll",
                    "SourceAssignments",
                    base,
                    "UpdatedUtc",
                    ti_slices,
                ),
                as_of=True,
                skip_diff=True,
            )
            _step("sources/getAll")
            _save(conn, "sources", client.fetch_all("sources/getAll", "Sources", base), as_of=True)
            _step("reservationGroups/getAll (all_slices)")
            _save(
                conn,
                "reservation_groups",
                client.fetch_all_time_slices(
                    "reservationGroups/getAll",
                    "ReservationGroups",
                    base,
                    "UpdatedUtc",
                    all_slices,
                ),
                as_of=True,
            )

            # 12) Routing + devices + orders
            _step("routingRules/getAll")
            _save(conn, "routing_rules", client.fetch_all("routingRules/getAll", "RoutingRules", base), as_of=True)

            _step("devices/getAll")
            dev = client.fetch_all("devices/getAll", "Devices", base)
            _save(conn, "devices", dev, as_of=True)
            cmd_ids: List[str] = []
            for d in dev:
                if isinstance(d, dict):
                    for c in d.get("Commands") or []:
                        if isinstance(c, dict) and c.get("Id"):
                            cmd_ids.append(c["Id"])
            _step("commands/getAllByIds")
            if cmd_ids:
                cmds_out: List[Dict[str, Any]] = []
                for chunk in chunked_string_ids([str(c) for c in cmd_ids], MEWS_MAX_BATCH_IDS):
                    cmd_resp = client.post_traced(
                        "commands/getAllByIds", {**base, "CommandIds": chunk}
                    )
                    cmds_out.extend(cmd_resp.get("Commands") or [])
                _save(conn, "commands", cmds_out, as_of=True)

            _step("serviceOrderNotes/getAll")
            if ctx.service_order_ids:
                _save(
                    conn,
                    "service_order_notes",
                    fetch_all_batched_id_param(
                        client,
                        "serviceOrderNotes/getAll",
                        "ServiceOrderNotes",
                        base,
                        [str(x) for x in ctx.service_order_ids],
                        "ServiceOrderIds",
                    ),
                    as_of=True,
                )

            # # reservation_prices: optional pricing probe skipped (needs category/rate/age); non-goal PDF

        _run_body(conn)

        _progress(
            sync_t0,
            "finished all enterprises (%s id(s)) snapshot_date=%s",
            len(enterprise_ids),
            snapshot_date,
        )
        logger.info("Mews sync completed for %s enterprise(s) @ %s", len(enterprise_ids), snapshot_date)
        _log_vis_line("=")

    if dry_run or no_snowflake:
        _run_sync_all_enterprises(None)
    else:
        _log_vis_line(".")
        _progress(sync_t0, "opening Snowflake connection …")
        with get_connection() as conn:
            _progress(sync_t0, "Snowflake connected")
            _log_vis_line(".")
            _run_sync_all_enterprises(conn)

    _log_vis_line("#")
    _progress(
        sync_t0,
        "Mews sync finished: %s enterprise(s) snapshot_date=%s",
        len(enterprise_ids),
        snapshot_date,
    )
    logger.info("Mews sync run completed: %s enterprise(s) @ %s", len(enterprise_ids), snapshot_date)
    _log_vis_line("#")


def main() -> None:
    p = argparse.ArgumentParser(description="PMS Mews Flight Recorder sync")
    p.add_argument("--date", type=str, help="Snapshot date YYYY-MM-DD (default: today UTC)")
    p.add_argument("--diff", action="store_true", help="Compute pms_mews_*_diff_daily vs previous day")
    p.add_argument("--dry-run", action="store_true", help="Fetch only; do not write Snowflake")
    p.add_argument("--no-snowflake", action="store_true", help="Skip Snowflake (fetch + log row counts)")
    args = p.parse_args()
    if args.date:
        snapshot_date = date.fromisoformat(args.date)
    else:
        snapshot_date = datetime.now(timezone.utc).date()
    run_sync(snapshot_date, do_diff=args.diff, dry_run=args.dry_run, no_snowflake=args.no_snowflake)


if __name__ == "__main__":
    main()