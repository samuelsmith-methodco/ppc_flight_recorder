"""
Lighthouse Rates Flight Recorder — daily Lighthouse API v3 -> Snowflake sync.

Self-contained: API client and recorder live in lighthouse/ (client.py, recorder.py),
Snowflake DDL in sql/lighthouse-rates-flight-recorder-tables.sql (auto-applied).
Each run records one snapshot layer of rates / roomtype rates / parities for all
subscriptions, plus refreshes the lighthouse_hotels / lighthouse_hotel_competitors
dimension tables.

  cd ppc_flight_recorder
  python sync_lighthouse.py                          # full daily snapshot (env defaults)
  python sync_lighthouse.py --snapshot-date 2026-06-10
  python sync_lighthouse.py --lookback-days 364 --shop-length 365   # ~365 days through today
  python sync_lighthouse.py --hotels-only            # refresh hotel dimension tables only
  python sync_lighthouse.py --subscription-ids 164110
"""

from __future__ import annotations

import argparse
import logging
from datetime import date
from typing import Optional

from config import LIGHTHOUSE_RATE_API_TOKEN

logger = logging.getLogger(__name__)


def _parse_snapshot_date(value: Optional[str]) -> date:
    if value is None:
        return date.today()
    try:
        return date.fromisoformat(value.strip())
    except ValueError as exc:
        raise ValueError(f"Invalid snapshot date '{value}'. Use YYYY-MM-DD.") from exc


def run_sync(
    subscription_ids: Optional[list[int]] = None,
    hotels_only: bool = False,
    skip_upload: bool = False,
    lookback_days: Optional[int] = None,
    shop_length: Optional[int] = None,
    otas: Optional[list[str]] = None,
    compset_ids: Optional[list[int]] = None,
    snapshot_date: Optional[str] = None,
) -> dict:
    """
    Run one Lighthouse flight recorder snapshot. Returns a result dict for
    the server's last-sync status (status: ok | error).

    lookback_days / shop_length / otas / compset_ids override env defaults when set.
    snapshot_date overrides the Snowflake layer date (default: today); fromDate is
    anchored to snapshot_date minus lookback_days.
    """
    try:
        snap = _parse_snapshot_date(snapshot_date)
    except ValueError as exc:
        return {"status": "error", "snapshot_date": snapshot_date or "", "error": str(exc)}

    snapshot_date_iso = snap.isoformat()
    if not LIGHTHOUSE_RATE_API_TOKEN:
        return {
            "status": "error",
            "snapshot_date": snapshot_date_iso,
            "error": "LIGHTHOUSE_RATE_API_TOKEN not configured (set it in .env)",
        }
    try:
        from lighthouse.recorder import record_snapshot

        ok = record_snapshot(
            subscription_ids=subscription_ids,
            skip_upload=skip_upload,
            hotels_only=hotels_only,
            lookback_days=lookback_days,
            shop_length=shop_length,
            otas=otas,
            compset_ids=compset_ids,
            snapshot_date=snapshot_date_iso,
        )
        result = {
            "status": "ok" if ok else "error",
            "snapshot_date": snapshot_date_iso,
            "hotels_only": hotels_only,
            "subscription_ids": subscription_ids,
            "lookback_days": lookback_days,
            "shop_length": shop_length,
            "otas": otas,
            "compset_ids": compset_ids,
        }
        if not ok:
            result["error"] = "recorder finished with errors (see logs)"
        return result
    except Exception as exc:
        logger.exception("Lighthouse sync failed: %s", exc)
        return {"status": "error", "snapshot_date": snapshot_date_iso, "error": str(exc)}


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description="Lighthouse rates flight recorder — API to Snowflake")
    parser.add_argument(
        "--subscription-ids",
        type=lambda s: [int(x) for x in s.split(",") if x.strip()],
        default=None,
        help="Limit to specific subscription IDs, e.g. 164110,164111",
    )
    parser.add_argument("--hotels-only", action="store_true", help="Refresh hotel dimension tables only")
    parser.add_argument("--skip-upload", action="store_true", help="Dry run: fetch only, skip Snowflake upload")
    parser.add_argument(
        "--snapshot-date",
        type=str,
        default=None,
        help="Snowflake snapshot layer date YYYY-MM-DD (default: today). "
        "Deletes and re-inserts rows for this snapshot_date.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=None,
        help="Days before snapshot_date for rates fromDate (overrides LIGHTHOUSE_FR_LOOKBACK_DAYS). "
        "Use 364 with --shop-length 365 for ~365 arrival days through snapshot_date.",
    )
    parser.add_argument(
        "--shop-length",
        type=int,
        default=None,
        help="Days of arrival dates from fromDate (overrides LIGHTHOUSE_FR_SHOP_LENGTH; API max 365)",
    )
    parser.add_argument(
        "--otas",
        type=lambda s: [x.strip() for x in s.split(",") if x.strip()],
        default=None,
        help="Comma-separated OTAs, e.g. bookingdotcom,expedia,branddotcom",
    )
    parser.add_argument(
        "--compset-ids",
        type=lambda s: [int(x) for x in s.split(",") if x.strip()],
        default=None,
        help="Comma-separated compset IDs, e.g. 1",
    )
    args = parser.parse_args()

    result = run_sync(
        subscription_ids=args.subscription_ids,
        hotels_only=args.hotels_only,
        skip_upload=args.skip_upload,
        lookback_days=args.lookback_days,
        shop_length=args.shop_length,
        otas=args.otas,
        compset_ids=args.compset_ids,
        snapshot_date=args.snapshot_date,
    )
    if result.get("status") != "ok":
        logger.error("Sync failed: %s", result.get("error"))
        return 1
    logger.info("Done: %s", result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
