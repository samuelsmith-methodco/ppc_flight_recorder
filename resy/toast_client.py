"""Toast Standard API → resy_toast_daily rows (dinner/daytime turns)."""

from __future__ import annotations

import logging
import statistics
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import httpx

logger = logging.getLogger(__name__)

HOST = "https://ws-api.toasttab.com"
ET = ZoneInfo("America/New_York")


class ToastError(RuntimeError):
    pass


def _auth(client: httpx.Client, client_id: str, client_secret: str) -> str:
    r = client.post(
        f"{HOST}/authentication/v1/authentication/login",
        json={"clientId": client_id, "clientSecret": client_secret, "userAccessType": "TOAST_MACHINE_CLIENT"},
    )
    if r.status_code >= 400:
        raise ToastError(f"Toast auth HTTP {r.status_code}")
    token = (r.json().get("token") or {}).get("accessToken")
    if not token:
        raise ToastError("Toast auth response missing accessToken")
    return str(token)


def _orders(client: httpx.Client, token: str, guid: str, business_date: str) -> tuple[str, list[dict[str, Any]]]:
    """business_date is YYYYMMDD. Returns (possibly refreshed token, orders)."""
    page = 1
    out: list[dict[str, Any]] = []
    while True:
        r = client.get(
            f"{HOST}/orders/v2/ordersBulk",
            params={"businessDate": business_date, "pageSize": 100, "page": page},
            headers={"Authorization": f"Bearer {token}", "Toast-Restaurant-External-ID": guid},
        )
        if r.status_code == 401:
            raise ToastError("unauthorized")
        if r.status_code == 404:
            return token, out
        if r.status_code == 429 or r.status_code >= 500:
            time.sleep(min(30, 2 ** min(page, 4)))
            r = client.get(
                f"{HOST}/orders/v2/ordersBulk",
                params={"businessDate": business_date, "pageSize": 100, "page": page},
                headers={"Authorization": f"Bearer {token}", "Toast-Restaurant-External-ID": guid},
            )
        if r.status_code == 404:
            return token, out
        r.raise_for_status()
        batch = r.json()
        if not isinstance(batch, list) or not batch:
            return token, out
        out.extend(batch)
        if len(batch) < 100:
            return token, out
        page += 1


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    return float(statistics.median(values))


def _service_type(opened: str | None) -> int:
    if not opened:
        return 1
    try:
        dt = datetime.fromisoformat(opened.replace("Z", "+00:00"))
    except ValueError:
        return 1
    hour = dt.astimezone(ET).hour
    return 2 if hour >= 16 else 1


def build_toast_rows(
    venue_id: int,
    guid: str,
    include_rc: list[str] | None,
    days: list[date],
    client: httpx.Client,
    token: str,
    client_id: str,
    client_secret: str,
) -> list[dict[str, Any]]:
    agg: dict[str, dict[str, Any]] = {}
    for day in days:
        bd = day.strftime("%Y%m%d")
        try:
            token, orders = _orders(client, token, guid, bd)
        except ToastError as exc:
            if "unauthorized" in str(exc):
                token = _auth(client, client_id, client_secret)
                token, orders = _orders(client, token, guid, bd)
            else:
                logger.warning("Toast venue %s %s: %s", venue_id, bd, exc)
                continue
        except httpx.HTTPError as exc:
            logger.warning("Toast venue %s %s: %s", venue_id, bd, exc)
            continue
        for order in orders:
            if order.get("voided") or order.get("deleted"):
                continue
            rc = (order.get("revenueCenter") or {}).get("guid")
            if include_rc and rc not in include_rc:
                continue
            net = 0.0
            for check in order.get("checks") or []:
                if check.get("voided") or check.get("deleted"):
                    continue
                net += float(check.get("amount") or 0)
            opened = order.get("openedDate") or order.get("createdDate")
            svc = _service_type(opened)
            key = f"{bd}|{svc}"
            bucket = agg.setdefault(
                key,
                {
                    "date": day.isoformat(),
                    "svc": svc,
                    "orders": 0,
                    "covers": 0,
                    "net": 0.0,
                    "dnet": 0.0,
                    "ccov": 0,
                    "csales": 0.0,
                    "turns": [],
                },
            )
            bucket["net"] += net
            covers = int(order.get("numberOfGuests") or 0)
            if covers > 0 and order.get("table") and order.get("openedDate") and order.get("closedDate"):
                if order.get("duration") is not None:
                    turn = float(order["duration"]) / 60.0
                else:
                    turn = (
                        datetime.fromisoformat(order["closedDate"].replace("Z", "+00:00"))
                        - datetime.fromisoformat(order["openedDate"].replace("Z", "+00:00"))
                    ).total_seconds() / 60.0
                if not (10 <= turn <= 360):
                    turn = None
                is_event = covers >= 12
                bucket["orders"] += 1
                bucket["covers"] += covers
                bucket["dnet"] += net
                if not is_event:
                    bucket["ccov"] += covers
                    bucket["csales"] += net
                    if turn is not None:
                        bucket["turns"].append(turn)
    rows: list[dict[str, Any]] = []
    for bucket in agg.values():
        turns: list[float] = bucket["turns"]
        covers = bucket["covers"]
        ccov = bucket["ccov"]
        rows.append(
            {
                "venue_id": venue_id,
                "business_date": bucket["date"],
                "service_type": bucket["svc"],
                "orders": bucket["orders"],
                "covers": covers,
                "net_sales": round(bucket["net"], 2),
                "dine_net": round(bucket["dnet"], 2),
                "avg_check_per_cover": round(bucket["dnet"] / covers, 2) if covers else None,
                "clean_covers": ccov,
                "clean_sales": round(bucket["csales"], 2),
                "avg_check_clean": round(bucket["csales"] / ccov, 2) if ccov else None,
                "avg_turn_min": round(sum(turns) / len(turns), 1) if turns else None,
                "median_turn_min": round(_median(turns), 1) if turns else None,
            }
        )
    return rows


def trailing_business_dates(n: int, end: date | None = None) -> list[date]:
    """Last n completed business dates ending yesterday (UTC date), matching toast.mjs."""
    today = end or datetime.now(timezone.utc).date()
    return [today - timedelta(days=i) for i in range(1, n + 1)]
