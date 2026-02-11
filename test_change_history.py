"""
Test script: fetch all change history (ChangeEvent) from Google Ads and update the DB.

The API only allows querying the last 30 days. Use --days to backfill that range,
or --date for a single day (default: yesterday).

Usage:
  python test_change_history.py
  python test_change_history.py --date 2026-02-11
  python test_change_history.py --days 7
  python test_change_history.py --days 30 --project the-pinch
  python test_change_history.py --dry-run
"""

import argparse
import logging
from datetime import date, timedelta
from typing import List, Optional

from config import PPC_PROJECTS, get_google_ads_customer_id, normalize_customer_id
from google_ads_client import fetch_change_events
from snowflake_connection import get_connection
from storage import upsert_change_events_daily

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def run_test_fetch_all_change_history(
    project: Optional[str] = None,
    single_date: Optional[date] = None,
    days: Optional[int] = None,
    dry_run: bool = False,
) -> None:
    """
    Fetch change events from Google Ads for the given date(s) and upsert into ppc_change_event_daily.
    API allows only the last 30 days; days is capped at 30.
    """
    if single_date is not None:
        date_list: List[date] = [single_date]
    elif days is not None:
        end = date.today() - timedelta(days=1)
        days = min(int(days), 30)
        start = end - timedelta(days=days - 1)
        date_list = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    else:
        date_list = [date.today() - timedelta(days=1)]

    projects: List[str] = [p.strip() for p in (project or PPC_PROJECTS).split(",") if p.strip()]
    if not projects:
        logger.warning("No projects configured (PPC_PROJECTS or --project)")
        return

    total_events = 0
    for proj in projects:
        customer_id = normalize_customer_id(get_google_ads_customer_id(proj))
        if not customer_id:
            logger.warning("Skipping project %s: no customer_id", proj)
            continue
        for d in date_list:
            date_str = d.isoformat()
            events = fetch_change_events(project=proj, snapshot_date=date_str, google_ads_filters=None)
            total_events += len(events)
            if dry_run:
                logger.info("[dry-run] project=%s date=%s events=%s", proj, date_str, len(events))
                continue
            if events:
                with get_connection() as conn:
                    upsert_change_events_daily(d, customer_id, events, conn=conn)
                logger.info("project=%s date=%s upserted %s change events", proj, date_str, len(events))
            else:
                if not dry_run:
                    with get_connection() as conn:
                        upsert_change_events_daily(d, customer_id, [], conn=conn)
                logger.info("project=%s date=%s no change events", proj, date_str)

    logger.info("Done. Total change events fetched: %s", total_events)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch Google Ads change history (ChangeEvent) and update ppc_change_event_daily."
    )
    parser.add_argument("--project", type=str, default=None, help="Project name (default: all from PPC_PROJECTS)")
    parser.add_argument("--date", type=str, default=None, help="Single date YYYY-MM-DD (default: yesterday)")
    parser.add_argument("--days", type=int, default=None, help="Backfill last N days (max 30; default: 1 if no --date)")
    parser.add_argument("--dry-run", action="store_true", help="Fetch only, do not write to DB")
    args = parser.parse_args()

    single_date: Optional[date] = None
    if args.date:
        try:
            single_date = date.fromisoformat(args.date)
        except ValueError:
            logger.error("Invalid --date; use YYYY-MM-DD")
            return

    run_test_fetch_all_change_history(
        project=args.project,
        single_date=single_date,
        days=args.days,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
