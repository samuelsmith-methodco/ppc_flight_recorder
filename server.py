"""
PPC Flight Recorder – FastAPI server with daily sync scheduler.

Runs Uvicorn on port 9001. Scheduler runs daily sync (Google Ads + GA4 + diffs) at a set time.

  cd ppc_flight_recorder
  pip install -r requirements.txt
  uvicorn server:app --host 0.0.0.0 --port 9001

  Optional .env: SYNC_SCHEDULE_TIMEZONE, SYNC_SCHEDULE_HOUR, SYNC_SCHEDULE_MINUTE (default 9:30 PM America/New_York).
"""

import logging
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from fastapi import Body, FastAPI, HTTPException
from pydantic import BaseModel

from config import (
    PPC_PROJECTS,
    SAVE_GA4_ON_DAILY_SYNC,
    SYNC_SCHEDULE_HOUR,
    SYNC_SCHEDULE_MINUTE,
    SYNC_SCHEDULE_TIMEZONE,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

# Scheduler and state (set in lifespan)
_scheduler = None
_last_sync_result: Optional[dict] = None


def _run_daily_sync() -> None:
    """Run daily sync for yesterday, all projects; GA4 + diffs only if SAVE_GA4_ON_DAILY_SYNC is set. Called by scheduler."""
    global _last_sync_result
    from sync import run_sync

    today = date.today()
    # Sync: yesterday only
    dates_to_sync = [today - timedelta(days=1)]
    projects = [p.strip() for p in PPC_PROJECTS.split(",") if p.strip()] or ["the-pinch"]
    completed = []
    last_error = None
    for snapshot_date in dates_to_sync:
        try:
            run_sync(
                snapshot_date=snapshot_date,
                projects=projects,
                run_ga4=SAVE_GA4_ON_DAILY_SYNC,
            )
            completed.append(snapshot_date.isoformat())
            logger.info("Scheduled daily sync completed for %s", snapshot_date.isoformat())
        except Exception as e:
            last_error = str(e)
            logger.exception("Scheduled daily sync failed for %s: %s", snapshot_date.isoformat(), e)
            _last_sync_result = {
                "status": "error",
                "snapshot_date": snapshot_date.isoformat(),
                "completed_dates": completed,
                "error": last_error,
            }
            raise
    _last_sync_result = {
        "status": "ok",
        "snapshot_dates": completed,
        "projects": projects,
    }
    logger.info("Scheduled daily sync completed for all dates: %s", ", ".join(completed))


def _get_scheduler():
    from apscheduler.schedulers.background import BackgroundScheduler

    sched = BackgroundScheduler(timezone=SYNC_SCHEDULE_TIMEZONE)
    sched.add_job(
        _run_daily_sync,
        trigger="cron",
        hour=SYNC_SCHEDULE_HOUR,
        minute=SYNC_SCHEDULE_MINUTE,
        id="daily_sync",
    )
    return sched


def _format_time_until(next_run: Optional[datetime]) -> str:
    """Return human-readable string: e.g. '5h 23m' or '23 minutes' or '< 1 minute'."""
    if not next_run:
        return "unknown"
    now = datetime.now(timezone.utc)
    next_utc = next_run.astimezone(timezone.utc) if next_run.tzinfo else next_run.replace(tzinfo=timezone.utc)
    delta = next_utc - now
    total_seconds = max(0, delta.total_seconds())
    hours = int(total_seconds // 3600)
    minutes = int((total_seconds % 3600) // 60)
    if hours >= 1:
        return f"{hours}h {minutes}m" if minutes else f"{hours} hours"
    if minutes >= 1:
        return f"{minutes} minutes"
    return "< 1 minute"


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _scheduler
    _scheduler = _get_scheduler()
    _scheduler.start()
    job = _scheduler.get_job("daily_sync")
    next_run = job.next_run_time if job else None
    time_left = _format_time_until(next_run)
    next_iso = next_run.isoformat() if next_run else "?"
    logger.info(
        "Scheduler started: daily sync at %02d:%02d %s (yesterday%s); next run in %s (%s)",
        SYNC_SCHEDULE_HOUR,
        SYNC_SCHEDULE_MINUTE,
        SYNC_SCHEDULE_TIMEZONE,
        ", GA4 + diffs" if SAVE_GA4_ON_DAILY_SYNC else "",
        time_left,
        next_iso,
    )
    yield
    if _scheduler:
        _scheduler.shutdown(wait=False)
        _scheduler = None
    logger.info("Scheduler stopped.")


app = FastAPI(
    title="PPC Flight Recorder",
    description="Daily sync of Google Ads and GA4 data to Snowflake, with optional manual trigger.",
    lifespan=lifespan,
)


@app.get("/health")
def health():
    """Health check for load balancers / readiness."""
    return {"status": "ok", "service": "ppc-flight-recorder"}


@app.get("/schedule")
def schedule():
    """Return current schedule, next run time, and hours/minutes until next run."""
    if not _scheduler:
        return {"scheduler": "not_running", "schedule": None}
    job = _scheduler.get_job("daily_sync")
    next_run = job.next_run_time if job else None
    next_run_iso = next_run.isoformat() if next_run else None
    time_left = _format_time_until(next_run)
    return {
        "scheduler": "running",
        "schedule": {
            "timezone": SYNC_SCHEDULE_TIMEZONE,
            "hour": SYNC_SCHEDULE_HOUR,
            "minute": SYNC_SCHEDULE_MINUTE,
            "next_run": next_run_iso,
            "next_run_in": time_left,
        },
        "last_sync": _last_sync_result,
    }


class SyncRequest(BaseModel):
    date: Optional[str] = None  # YYYY-MM-DD; default yesterday
    control_state_only: Optional[bool] = False  # If true, update only control_state_daily and control_diff_daily
    control_state_keyword_only: Optional[bool] = False  # If true, update only keyword and negative keyword snapshots/diffs
    control_state_adgroup_only: Optional[bool] = False  # If true, update only ad group snapshot and diff
    control_state_device_only: Optional[bool] = False  # If true, update only device targeting (ppc_ad_group_device_modifier_daily, _diff_daily)
    control_state_conversions_only: Optional[bool] = False  # If true, update only conversion definitions (ppc_conversion_action_daily, _diff_daily)


@app.post("/sync")
def trigger_sync(body: Optional[SyncRequest] = Body(None)):
    """
    Run sync once (same as daily job: GA4 + diffs), or control-state-only, or control-state-keyword-only, or control-state-adgroup-only, or control-state-device-only, or control-state-conversions-only.
    Optional body: {"date": "YYYY-MM-DD", "control_state_only": true} or {"control_state_device_only": true} or {"control_state_conversions_only": true} etc.
    """
    from sync import run_sync

    if body and body.date:
        try:
            snapshot_date = date.fromisoformat(body.date)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date; use YYYY-MM-DD")
    else:
        snapshot_date = date.today() - timedelta(days=1)

    control_state_only = bool(body and body.control_state_only)
    control_state_keyword_only = bool(body and body.control_state_keyword_only)
    control_state_adgroup_only = bool(body and body.control_state_adgroup_only)
    control_state_device_only = bool(body and body.control_state_device_only)
    control_state_conversions_only = bool(body and body.control_state_conversions_only)
    projects = [p.strip() for p in PPC_PROJECTS.split(",") if p.strip()] or ["the-pinch"]
    try:
        run_sync(
            snapshot_date=snapshot_date,
            projects=projects,
            run_ga4=SAVE_GA4_ON_DAILY_SYNC and not (control_state_only or control_state_keyword_only or control_state_adgroup_only or control_state_device_only or control_state_conversions_only),
            control_state_only=control_state_only,
            control_state_keyword_only=control_state_keyword_only,
            control_state_adgroup_only=control_state_adgroup_only,
            control_state_device_only=control_state_device_only,
            control_state_conversions_only=control_state_conversions_only,
        )
        return {
            "status": "ok",
            "snapshot_date": snapshot_date.isoformat(),
            "projects": projects,
            "control_state_only": control_state_only,
            "control_state_keyword_only": control_state_keyword_only,
            "control_state_adgroup_only": control_state_adgroup_only,
            "control_state_device_only": control_state_device_only,
            "control_state_conversions_only": control_state_conversions_only,
        }
    except Exception as e:
        logger.exception("Manual sync failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
