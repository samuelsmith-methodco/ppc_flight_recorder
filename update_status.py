"""
Persist daily sync outcomes into Leonardo's shared Snowflake ``app_settings``.

Keys match Leonardo Update Health:
  daily_update_last_run:{job_id}
JSON value:
  {"last_success_at": "...", "last_attempt_at": "...", "status": "success"|"failed", "error": null|"..."}

No run history / log rows — status snapshot only for Leonardo Settings → Update Health.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional

from snowflake_connection import execute, execute_query, get_connection

logger = logging.getLogger(__name__)

SETTING_KEY_PREFIX = "daily_update_last_run:"

# Stable job ids — must match Leonardo DAILY_UPDATE_JOBS catalog.
JOB_RESY = "ppc_fr_resy"
JOB_IDEAS = "ppc_fr_ideas"
JOB_LIGHTHOUSE = "ppc_fr_lighthouse"
JOB_PPC = "ppc_fr_ppc"


def _setting_key(job_id: str) -> str:
    return f"{SETTING_KEY_PREFIX}{job_id}"


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _now_naive_utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _load_existing_success_at(conn: Any, job_id: str) -> Optional[str]:
    try:
        df = execute_query(
            conn,
            "SELECT setting_value FROM app_settings WHERE setting_key = %(k)s",
            {"k": _setting_key(job_id)},
        )
        if df is None or df.empty:
            return None
        raw = df.iloc[0].get("SETTING_VALUE") or df.iloc[0].get("setting_value")
        if raw is None or not str(raw).strip():
            return None
        data = json.loads(str(raw))
        if isinstance(data, dict):
            val = data.get("last_success_at")
            return str(val) if val else None
    except Exception as exc:
        logger.warning("Failed to load prior update status for %s: %s", job_id, exc)
    return None


def record_update_run(
    job_id: str,
    *,
    success: bool,
    error_message: Optional[str] = None,
) -> None:
    """Persist last-run outcome for a Flight Recorder job (best-effort; never raises)."""
    normalized = (job_id or "").strip()
    if not normalized:
        return
    now = _now_iso()
    try:
        with get_connection() as conn:
            prior_success = None if success else _load_existing_success_at(conn, normalized)
            payload: dict[str, Any] = {
                "last_attempt_at": now,
                "status": "success" if success else "failed",
                "error": (error_message or None) if not success else None,
                "last_success_at": now if success else prior_success,
            }
            execute(
                conn,
                """
                MERGE INTO app_settings AS t
                USING (SELECT %(k)s AS setting_key, %(v)s AS setting_value) AS s
                ON t.setting_key = s.setting_key
                WHEN MATCHED THEN UPDATE SET setting_value = s.setting_value, updated_at = %(updated_at)s
                WHEN NOT MATCHED THEN INSERT (setting_key, setting_value, updated_at)
                    VALUES (s.setting_key, s.setting_value, %(updated_at)s)
                """,
                {
                    "k": _setting_key(normalized),
                    "v": json.dumps(payload),
                    "updated_at": _now_naive_utc(),
                },
            )
        logger.info(
            "Recorded update status job_id=%s status=%s",
            normalized,
            "success" if success else "failed",
        )
    except Exception as exc:
        logger.warning("Failed to record update run for %s: %s", normalized, exc)
