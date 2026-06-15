"""
PPC Flight Recorder – FastAPI server with daily sync scheduler.

Runs Uvicorn on port 9001. Schedulers:
  - PPC (+ optional Mews): default 9:30 PM — syncs yesterday only
  - IDeaS G3: default 9:00 AM — syncs yesterday + today delivery dates
  - Lighthouse rates: default 9:30 AM — records today's rates/parity snapshot

  cd ppc_flight_recorder
  pip install -r requirements.txt
  uvicorn server:app --host 0.0.0.0 --port 9001

  Optional .env: SYNC_SCHEDULE_*, IDEAS_SYNC_SCHEDULE_*, IDEAS_SFTP_*, MEWS_*, etc.
"""

import logging
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from fastapi import Body, FastAPI, HTTPException
from pydantic import BaseModel

from config import (
    IDEAS_SFTP_PASSWORD,
    IDEAS_SFTP_USERNAME,
    IDEAS_SYNC_SCHEDULE_HOUR,
    IDEAS_SYNC_SCHEDULE_MINUTE,
    IDEAS_SYNC_SCHEDULE_TIMEZONE,
    LIGHTHOUSE_SYNC_SCHEDULE_HOUR,
    LIGHTHOUSE_SYNC_SCHEDULE_MINUTE,
    LIGHTHOUSE_SYNC_SCHEDULE_TIMEZONE,
    RUN_LIGHTHOUSE_SYNC_ON_SCHEDULE,
    MEWS_ACCESS_TOKEN,
    MEWS_CLIENT_TOKEN,
    MEWS_SYNC_DO_DIFF_ON_SCHEDULE,
    PPC_PROJECTS,
    RUN_IDEAS_SYNC_ON_SCHEDULE,
    RUN_MEWS_SYNC_AFTER_DAILY_SYNC,
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
_last_ideas_sync_result: Optional[dict] = None
_last_lighthouse_sync_result: Optional[dict] = None


def _run_mews_after_ppc(snapshot_date: date, *, partial_ppc_sync: bool = False) -> None:
    """Run PMS Mews flight recorder for the same snapshot_date as the PPC sync (sync_mews.run_sync)."""
    if partial_ppc_sync:
        logger.info(
            "Skipping Mews post-sync for %s (partial PPC sync — control-state-only / keyword-only / …)",
            snapshot_date.isoformat(),
        )
        return
    if not RUN_MEWS_SYNC_AFTER_DAILY_SYNC:
        logger.info("Mews post-sync disabled (RUN_MEWS_SYNC_AFTER_DAILY_SYNC unset/false)")
        return
    if not (MEWS_CLIENT_TOKEN and MEWS_ACCESS_TOKEN):
        logger.info(
            "Skipping Mews post-sync for %s (MEWS_CLIENT_TOKEN / MEWS_ACCESS_TOKEN not set)",
            snapshot_date.isoformat(),
        )
        return
    import sync_mews

    logger.info(
        "Starting Mews PMS flight recorder sync for snapshot_date=%s (do_diff=%s)",
        snapshot_date.isoformat(),
        MEWS_SYNC_DO_DIFF_ON_SCHEDULE,
    )
    try:
        sync_mews.run_sync(
            snapshot_date,
            do_diff=MEWS_SYNC_DO_DIFF_ON_SCHEDULE,
            dry_run=False,
            no_snowflake=False,
        )
    except SystemExit as e:
        # sync_mews.run_sync uses sys.exit on fatal misconfig; do not kill Uvicorn.
        code = e.code if isinstance(e.code, int) else 1
        if code != 0:
            logger.error("Mews sync exited with code %s", code)
            raise RuntimeError(f"Mews sync failed (sys.exit {code})") from None
    logger.info("Mews PMS flight recorder sync completed for %s", snapshot_date.isoformat())


def _run_daily_sync() -> None:
    """Run daily sync for yesterday, all projects; GA4 + diffs only if SAVE_GA4_ON_DAILY_SYNC is set. Called by scheduler."""
    global _last_sync_result
    from sync import run_sync

    today = date.today()
    # Sync: yesterday only
    dates_to_sync = [today - timedelta(days=1)]
    projects = [p.strip() for p in PPC_PROJECTS.split(",") if p.strip()] or ["the_pinch_charleston"]
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
            _run_mews_after_ppc(snapshot_date, partial_ppc_sync=False)
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


def _run_ideas_daily_sync() -> None:
    """Run IDeaS sync for yesterday and today delivery dates (delete + insert per archive)."""
    global _last_ideas_sync_result
    from ideas_storage import default_delivery_dates, run_sync

    if not IDEAS_SFTP_USERNAME or not IDEAS_SFTP_PASSWORD:
        logger.info("Skipping IDeaS scheduled sync (IDEAS_SFTP_USERNAME / IDEAS_SFTP_PASSWORD not set)")
        _last_ideas_sync_result = {"status": "skipped", "reason": "missing SFTP credentials"}
        return

    delivery_dates = default_delivery_dates()
    logger.info("Starting IDeaS scheduled sync for delivery dates: %s", ", ".join(delivery_dates))
    try:
        result = run_sync(delivery_dates=delivery_dates, no_update_existing=False)
        _last_ideas_sync_result = result
        if result.get("status") == "error":
            logger.error("IDeaS scheduled sync aborted: %s", result.get("error"))
            return
        logger.info(
            "IDeaS scheduled sync completed: processed=%s skipped=%s",
            result.get("archives_processed"),
            result.get("archives_skipped"),
        )
    except Exception as exc:
        logger.exception("IDeaS scheduled sync failed: %s", exc)
        _last_ideas_sync_result = {"status": "error", "error": str(exc), "delivery_dates": delivery_dates}
        return


def _run_lighthouse_daily_sync() -> None:
    """Run Lighthouse rates flight recorder snapshot (all subscriptions) for today."""
    global _last_lighthouse_sync_result
    from sync_lighthouse import run_sync

    logger.info("Starting Lighthouse scheduled sync")
    result = run_sync()
    _last_lighthouse_sync_result = result
    if result.get("status") == "error":
        logger.error("Lighthouse scheduled sync failed: %s", result.get("error"))
        return
    logger.info("Lighthouse scheduled sync completed for snapshot_date=%s", result.get("snapshot_date"))


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
    if RUN_IDEAS_SYNC_ON_SCHEDULE:
        sched.add_job(
            _run_ideas_daily_sync,
            trigger="cron",
            hour=IDEAS_SYNC_SCHEDULE_HOUR,
            minute=IDEAS_SYNC_SCHEDULE_MINUTE,
            id="ideas_daily_sync",
            timezone=IDEAS_SYNC_SCHEDULE_TIMEZONE,
        )
    if RUN_LIGHTHOUSE_SYNC_ON_SCHEDULE:
        sched.add_job(
            _run_lighthouse_daily_sync,
            trigger="cron",
            hour=LIGHTHOUSE_SYNC_SCHEDULE_HOUR,
            minute=LIGHTHOUSE_SYNC_SCHEDULE_MINUTE,
            id="lighthouse_daily_sync",
            timezone=LIGHTHOUSE_SYNC_SCHEDULE_TIMEZONE,
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
    ideas_job = _scheduler.get_job("ideas_daily_sync") if RUN_IDEAS_SYNC_ON_SCHEDULE else None
    ideas_next = ideas_job.next_run_time if ideas_job else None
    logger.info(
        "Scheduler started: PPC daily sync at %02d:%02d %s (yesterday%s); next in %s (%s)",
        SYNC_SCHEDULE_HOUR,
        SYNC_SCHEDULE_MINUTE,
        SYNC_SCHEDULE_TIMEZONE,
        ", GA4 + diffs" if SAVE_GA4_ON_DAILY_SYNC else "",
        time_left,
        next_iso,
    )
    if RUN_IDEAS_SYNC_ON_SCHEDULE:
        logger.info(
            "Scheduler started: IDeaS daily sync at %02d:%02d %s (yesterday + today); next in %s (%s)",
            IDEAS_SYNC_SCHEDULE_HOUR,
            IDEAS_SYNC_SCHEDULE_MINUTE,
            IDEAS_SYNC_SCHEDULE_TIMEZONE,
            _format_time_until(ideas_next),
            ideas_next.isoformat() if ideas_next else "?",
        )
    if RUN_LIGHTHOUSE_SYNC_ON_SCHEDULE:
        lh_job = _scheduler.get_job("lighthouse_daily_sync")
        lh_next = lh_job.next_run_time if lh_job else None
        logger.info(
            "Scheduler started: Lighthouse daily sync at %02d:%02d %s (today snapshot); next in %s (%s)",
            LIGHTHOUSE_SYNC_SCHEDULE_HOUR,
            LIGHTHOUSE_SYNC_SCHEDULE_MINUTE,
            LIGHTHOUSE_SYNC_SCHEDULE_TIMEZONE,
            _format_time_until(lh_next),
            lh_next.isoformat() if lh_next else "?",
        )
    yield
    if _scheduler:
        _scheduler.shutdown(wait=False)
        _scheduler = None
    logger.info("Scheduler stopped.")


app = FastAPI(
    title="PPC Flight Recorder",
    description="Daily sync of Google Ads, GA4, Mews PMS, and IDeaS G3 data to Snowflake.",
    lifespan=lifespan,
)


@app.get("/health")
def health():
    """Health check for load balancers / readiness."""
    return {"status": "ok", "service": "ppc-flight-recorder"}


@app.get("/schedule")
def schedule():
    """Return current schedules, next run times, and last sync results."""
    if not _scheduler:
        return {"scheduler": "not_running", "schedule": None}
    job = _scheduler.get_job("daily_sync")
    next_run = job.next_run_time if job else None
    next_run_iso = next_run.isoformat() if next_run else None
    time_left = _format_time_until(next_run)
    ideas_payload = None
    if RUN_IDEAS_SYNC_ON_SCHEDULE:
        ideas_job = _scheduler.get_job("ideas_daily_sync")
        ideas_next = ideas_job.next_run_time if ideas_job else None
        ideas_payload = {
            "timezone": IDEAS_SYNC_SCHEDULE_TIMEZONE,
            "hour": IDEAS_SYNC_SCHEDULE_HOUR,
            "minute": IDEAS_SYNC_SCHEDULE_MINUTE,
            "next_run": ideas_next.isoformat() if ideas_next else None,
            "next_run_in": _format_time_until(ideas_next),
            "delivery_dates": "yesterday_and_today",
            "last_sync": _last_ideas_sync_result,
        }
    lighthouse_payload = None
    if RUN_LIGHTHOUSE_SYNC_ON_SCHEDULE:
        lh_job = _scheduler.get_job("lighthouse_daily_sync")
        lh_next = lh_job.next_run_time if lh_job else None
        lighthouse_payload = {
            "timezone": LIGHTHOUSE_SYNC_SCHEDULE_TIMEZONE,
            "hour": LIGHTHOUSE_SYNC_SCHEDULE_HOUR,
            "minute": LIGHTHOUSE_SYNC_SCHEDULE_MINUTE,
            "next_run": lh_next.isoformat() if lh_next else None,
            "next_run_in": _format_time_until(lh_next),
            "snapshot_date": "today",
            "last_sync": _last_lighthouse_sync_result,
        }
    return {
        "scheduler": "running",
        "schedule": {
            "timezone": SYNC_SCHEDULE_TIMEZONE,
            "hour": SYNC_SCHEDULE_HOUR,
            "minute": SYNC_SCHEDULE_MINUTE,
            "next_run": next_run_iso,
            "next_run_in": time_left,
            "snapshot_date": "yesterday",
        },
        "ideas_schedule": ideas_payload,
        "lighthouse_schedule": lighthouse_payload,
        "last_sync": _last_sync_result,
    }


class SyncRequest(BaseModel):
    date: Optional[str] = None  # YYYY-MM-DD; default yesterday
    control_state_only: Optional[bool] = False  # If true, update only control_state_daily and control_diff_daily
    control_state_keyword_only: Optional[bool] = False  # If true, update only keyword and negative keyword snapshots/diffs
    control_state_adgroup_only: Optional[bool] = False  # If true, update only ad group snapshot and diff
    control_state_device_only: Optional[bool] = False  # If true, update only device targeting (ppc_ad_group_device_modifier_daily, _diff_daily)
    control_state_conversions_only: Optional[bool] = False  # If true, update only conversion definitions (ppc_conversion_action_daily, _diff_daily)


class IdeasSyncRequest(BaseModel):
    date: Optional[str] = None  # YYYY-MM-DD delivery date; default yesterday + today
    from_date: Optional[str] = None
    no_update_existing: Optional[bool] = False


@app.post("/sync/ideas")
def trigger_ideas_sync(body: Optional[IdeasSyncRequest] = Body(None)):
    """
    Run IDeaS G3 flight recorder sync (SFTP → Snowflake, delete + insert per snapshot).
    Default: yesterday and today delivery dates. Optional body: {"date": "YYYY-MM-DD"}.
    """
    from ideas.sftp_client import normalize_date
    from ideas_storage import default_delivery_dates, run_sync

    if not IDEAS_SFTP_USERNAME or not IDEAS_SFTP_PASSWORD:
        raise HTTPException(status_code=400, detail="IDEAS_SFTP_USERNAME / IDEAS_SFTP_PASSWORD not configured")

    delivery_dates: Optional[list[str]] = None
    from_date: Optional[str] = None
    no_update_existing = bool(body and body.no_update_existing)

    if body and body.date and body.from_date:
        raise HTTPException(status_code=400, detail="Use only one of date or from_date")
    if body and body.date:
        delivery_dates = [normalize_date(body.date)]
    elif body and body.from_date:
        from_date = normalize_date(body.from_date)
    else:
        delivery_dates = default_delivery_dates()

    try:
        result = run_sync(
            delivery_dates=delivery_dates,
            from_date=from_date,
            no_update_existing=no_update_existing,
        )
        global _last_ideas_sync_result
        _last_ideas_sync_result = result
        return result
    except Exception as e:
        logger.exception("Manual IDeaS sync failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


class LighthouseSyncRequest(BaseModel):
    hotels_only: Optional[bool] = False  # If true, refresh only lighthouse_hotels / lighthouse_hotel_competitors
    subscription_ids: Optional[list[int]] = None  # Limit to specific Lighthouse subscription IDs
    snapshot_date: Optional[str] = None  # YYYY-MM-DD Snowflake layer (default: today)
    lookback_days: Optional[int] = None  # Override LIGHTHOUSE_FR_LOOKBACK_DAYS
    shop_length: Optional[int] = None  # Override LIGHTHOUSE_FR_SHOP_LENGTH (API max 365)
    otas: Optional[list[str]] = None
    compset_ids: Optional[list[int]] = None


@app.post("/sync/lighthouse")
def trigger_lighthouse_sync(body: Optional[LighthouseSyncRequest] = Body(None)):
    """
    Run Lighthouse rates flight recorder snapshot (API -> Snowflake).
    Optional body: {"snapshot_date": "2026-06-10", "lookback_days": 364, "shop_length": 365}.
    """
    from sync_lighthouse import run_sync

    global _last_lighthouse_sync_result
    try:
        result = run_sync(
            subscription_ids=body.subscription_ids if body else None,
            hotels_only=bool(body and body.hotels_only),
            lookback_days=body.lookback_days if body else None,
            shop_length=body.shop_length if body else None,
            otas=body.otas if body else None,
            compset_ids=body.compset_ids if body else None,
            snapshot_date=body.snapshot_date if body else None,
        )
        _last_lighthouse_sync_result = result
        if result.get("status") == "error":
            raise HTTPException(status_code=500, detail=result.get("error") or "Lighthouse sync failed")
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Manual Lighthouse sync failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


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
    projects = [p.strip() for p in PPC_PROJECTS.split(",") if p.strip()] or ["the_pinch_charleston"]
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
        partial = bool(
            control_state_only
            or control_state_keyword_only
            or control_state_adgroup_only
            or control_state_device_only
            or control_state_conversions_only
        )
        _run_mews_after_ppc(snapshot_date, partial_ppc_sync=partial)
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
