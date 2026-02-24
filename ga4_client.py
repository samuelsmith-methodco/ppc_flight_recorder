"""
PPC Flight Recorder – GA4 traffic acquisition via Apps Script (standalone).
One API call per project (type=traffic_acquisition_daily_all) returns all dimensions.
Deployment returns 302 to script.googleusercontent.com/.../echo which accepts GET only;
we POST then GET the Location URL with query params.
"""

import logging
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode, urljoin

import httpx

from config import GA4_MARKETING_API_URL

logger = logging.getLogger(__name__)


def _normalize_date(d: str) -> str:
    if not d or len(d) < 8:
        return d
    if len(d) == 8 and d.isdigit():
        return f"{d[:4]}-{d[4:6]}-{d[6:8]}"
    return d.split("T")[0] if "T" in d else d


def _safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _call_app_script(client: httpx.Client, payload: Dict[str, Any]) -> httpx.Response:
    """POST to Apps Script; on 302 the redirect URL accepts GET only, so GET with payload as query params."""
    response = client.post(GA4_MARKETING_API_URL, json=payload)
    if response.status_code == 302:
        location = response.headers.get("Location")
        if location:
            redirect_url = urljoin(GA4_MARKETING_API_URL, location)
            query_params = {
                "type": payload.get("type", "traffic_acquisition_daily_all"),
                "startDate": payload.get("startDate", ""),
                "endDate": payload.get("endDate", ""),
                "project": payload.get("project", ""),
            }
            if payload.get("type") == "traffic_acquisition_daily" and payload.get("dimensionName"):
                query_params["dimensionName"] = payload["dimensionName"]
            query = urlencode(query_params)
            get_url = redirect_url + ("&" if "?" in redirect_url else "?") + query
            response = client.get(get_url)
    return response


def _row_to_storage(project: str, r: Dict[str, Any], dimension_type: str, report_type: str = "traffic_acquisition") -> Optional[Dict[str, Any]]:
    """Normalize one API row to storage shape (project, acquisition_date, report_type, dimension_type, dimension_value, ...)."""
    date_val = r.get("date")
    date_str = _normalize_date(str(date_val)) if date_val else None
    if not date_str:
        return None
    dimension_value = r.get("dimensionValue", "Unknown")
    return {
        "project": project,
        "acquisition_date": date_str,
        "report_type": report_type,
        "dimension_type": dimension_type,
        "dimension_value": dimension_value,
        "sessions": int(r.get("sessions") or 0),
        "engaged_sessions": int(r.get("engagedSessions") or 0),
        "total_revenue": float(r.get("totalRevenue") or 0),
        "event_count": int(r.get("eventCount") or 0),
        "key_events": int(r.get("keyEvents") or 0),
        "active_users": int(r.get("activeUsers") or 0),
        "average_session_duration_sec": _safe_float(r.get("averageSessionDuration")),
        "engagement_rate": _safe_float(r.get("engagementRate")),
        "bounce_rate": _safe_float(r.get("bounceRate")),
    }


def fetch_traffic_acquisition_daily_sync(
    start_date: str,
    end_date: str,
    project: str,
    dimension_names: Optional[List[str]] = None,
    ga4_filters: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Fetch GA4 traffic acquisition for one project in one API call (all dimensions)."""
    if not GA4_MARKETING_API_URL:
        logger.warning("GA4_MARKETING_API_URL not set; skipping GA4 fetch")
        return []
    start_date = _normalize_date(start_date)
    end_date = _normalize_date(end_date)
    payload = {
        "type": "traffic_acquisition_daily_all",
        "startDate": start_date,
        "endDate": end_date,
        "project": project,
    }
    if ga4_filters:
        payload["ga4Filters"] = ga4_filters
    timeout = httpx.Timeout(60.0, read=180.0)
    with httpx.Client(timeout=timeout, follow_redirects=False) as client:
        try:
            response = _call_app_script(client, payload)
            if response.status_code != 200:
                logger.warning("traffic_acquisition_daily_all %s returned %s: %s", project, response.status_code, response.text[:200])
                return []
            data = response.json()
            if isinstance(data, dict) and "error" in data:
                logger.warning("traffic_acquisition_daily_all %s error: %s", project, data.get("error"))
                return []
            rows = data if isinstance(data, list) else []
            out = []
            for r in rows:
                dim = r.get("dimensionName") or r.get("dimension_type")
                if not dim:
                    continue
                normalized = _row_to_storage(project, r, dim, report_type="traffic_acquisition")
                if normalized:
                    out.append(normalized)
            logger.info("GA4 traffic_acquisition_daily_all: %s rows for %s", len(out), project)
            return out
        except Exception as e:
            logger.warning("traffic_acquisition_daily_all %s: %s", project, e)
            return []


def _fetch_report_type_sync(
    client: httpx.Client,
    start_date: str,
    end_date: str,
    project: str,
    report_type: str,
    ga4_filters: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Fetch one GA4 report type (traffic_acquisition_daily_all, user_acquisition_daily_all, acquisition_overview_daily_all)."""
    type_map = {
        "traffic_acquisition": "traffic_acquisition_daily_all",
        "user_acquisition": "user_acquisition_daily_all",
        "acquisition_overview": "acquisition_overview_daily_all",
    }
    payload = {
        "type": type_map.get(report_type, "traffic_acquisition_daily_all"),
        "startDate": start_date,
        "endDate": end_date,
        "project": project,
    }
    if ga4_filters:
        payload["ga4Filters"] = ga4_filters
    response = _call_app_script(client, payload)
    if response.status_code != 200:
        logger.warning("GA4 %s %s returned %s", report_type, project, response.status_code)
        return []
    data = response.json()
    if isinstance(data, dict) and "error" in data:
        logger.warning("GA4 %s %s error: %s", report_type, project, data.get("error"))
        return []
    rows = data if isinstance(data, list) else []
    out = []
    for r in rows:
        dim = r.get("dimensionName") or r.get("dimension_type")
        if not dim:
            continue
        normalized = _row_to_storage(project, r, dim, report_type=report_type)
        if normalized:
            out.append(normalized)
    return out


def fetch_ga4_acquisition_all_sync(
    start_date: str,
    end_date: str,
    project: str,
    ga4_filters: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Fetch all GA4 acquisition data for one project: traffic_acquisition, user_acquisition, acquisition_overview (3 API calls per project)."""
    if not GA4_MARKETING_API_URL:
        logger.warning("GA4_MARKETING_API_URL not set; skipping GA4 fetch")
        return []
    start_date = _normalize_date(start_date)
    end_date = _normalize_date(end_date)
    timeout = httpx.Timeout(60.0, read=180.0)
    out: List[Dict[str, Any]] = []
    with httpx.Client(timeout=timeout, follow_redirects=False) as client:
        for report_type in ("traffic_acquisition", "user_acquisition", "acquisition_overview"):
            try:
                rows = _fetch_report_type_sync(client, start_date, end_date, project, report_type, ga4_filters)
                out.extend(rows)
            except Exception as e:
                logger.warning("GA4 %s %s: %s", report_type, project, e)
    logger.info("GA4 acquisition all: %s rows for %s", len(out), project)
    return out
