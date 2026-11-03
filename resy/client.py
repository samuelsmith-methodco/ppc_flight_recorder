"""Resy OS auth and analytics report client (httpx)."""

from __future__ import annotations

import json
import logging
from typing import Any
from urllib.parse import urlencode

import httpx

logger = logging.getLogger(__name__)

AUTH_BASE = "https://auth.resy.com"
AUTH_URL = f"{AUTH_BASE}/1/auth"
VENUES_URL = f"{AUTH_BASE}/1/venues"
VENUE_AUTH_URL = f"{AUTH_BASE}/1/auth/venue"
RESERVATIONS_REPORT = "https://api.resy.com/3/analytics/report/core/Reservations"
COVERS_REPORT = "https://api.resy.com/3/analytics/report/core/covers"


class ResyAuthError(RuntimeError):
    pass


class ResyReportAuthError(ResyAuthError):
    """Analytics token was rejected by the report endpoint."""


def resy_auth(email: str, password: str, client: httpx.Client) -> str:
    r = client.post(
        AUTH_URL,
        json={"email": email, "legacy": True, "password": password},
        headers={"content-type": "application/json", "accept": "application/json"},
    )
    if r.status_code >= 400:
        raise ResyAuthError(f"Resy auth HTTP {r.status_code}")
    body = r.json()
    tok = body.get("token") if isinstance(body, dict) else None
    if not tok:
        raise ResyAuthError("Resy auth response missing token")
    return str(tok)


def fetch_venues(universal_token: str, client: httpx.Client) -> list[dict[str, Any]]:
    r = client.get(
        VENUES_URL,
        headers={
            "x-resy-universal-auth": universal_token,
            "accept": "application/json",
        },
    )
    r.raise_for_status()
    body: Any = r.json()
    if isinstance(body, list):
        return [v for v in body if isinstance(v, dict)]
    if isinstance(body, dict):
        for key in ("venues", "data", "results"):
            v = body.get(key)
            if isinstance(v, list):
                return [x for x in v if isinstance(x, dict)]
    raise ResyAuthError(f"Unexpected /1/venues response shape: {type(body).__name__}")


def venue_analytics_token(universal_token: str, resy_venue_id: int, client: httpx.Client) -> str | None:
    r = client.post(
        VENUE_AUTH_URL,
        json={"venue_id": resy_venue_id},
        headers={
            "x-resy-universal-auth": universal_token,
            "content-type": "application/json",
            "accept": "application/json",
        },
    )
    if r.status_code in (401, 403):
        logger.info("Resy venue %s auth denied HTTP %s", resy_venue_id, r.status_code)
        return None
    r.raise_for_status()
    body = r.json()
    os_tok = (body.get("os_tokens") or {}) if isinstance(body, dict) else {}
    analytics = os_tok.get("analytics")
    return str(analytics) if analytics else None


def _report_headers(analytics_token: str | None, captured: dict[str, str] | None) -> dict[str, str]:
    if captured:
        headers = dict(captured)
        headers.setdefault("content-type", "application/x-www-form-urlencoded")
        headers.setdefault("accept", "application/json")
        return headers
    if not analytics_token:
        raise ResyReportAuthError("no analytics token and no captured browser headers")
    return {
        "x-resy-services-auth": analytics_token,
        "content-type": "application/x-www-form-urlencoded",
        "accept": "application/json",
        "origin": "https://os.resy.com",
    }


def post_report(
    client: httpx.Client,
    url: str,
    binds: dict[str, str],
    *,
    analytics_token: str | None = None,
    captured_headers: dict[str, str] | None = None,
) -> Any:
    body = urlencode({"struct_binds": json.dumps(binds)})
    r = client.post(url, content=body, headers=_report_headers(analytics_token, captured_headers))
    if r.status_code in (401, 403):
        raise ResyReportAuthError(f"report HTTP {r.status_code}")
    r.raise_for_status()
    return r.json()


def rows_from_report(payload: Any, section_name: str | None = None) -> list[dict[str, Any]]:
    """Flatten a Resy analytics report into header→value dicts."""
    arr = payload if isinstance(payload, list) else [payload]
    section = None
    if section_name:
        section = next(
            (s for s in arr if isinstance(s, dict) and (s.get("config") or {}).get("name") == section_name),
            None,
        )
    if section is None:
        section = arr[0] if arr else None
    if not isinstance(section, dict):
        return []
    raw_rows = ((section.get("data") or {}).get("rows")) or []
    out: list[dict[str, Any]] = []
    for row in raw_rows:
        if not isinstance(row, dict):
            continue
        cols = row.get("cols") or []
        item: dict[str, Any] = {}
        for col in cols:
            if not isinstance(col, dict):
                continue
            header = (col.get("header") or {}).get("name")
            if header:
                item[str(header)] = col.get("value")
        if item:
            out.append(item)
    return out
