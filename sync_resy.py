"""
Resy Pace flight recorder — Resy analytics + Toast turns -> Snowflake.

  cd ppc_flight_recorder
  python sync_resy.py --dry-run
  python sync_resy.py --venue-id 1 --horizon 7 --toast-days 1
  python sync_resy.py --venues-only
"""

from __future__ import annotations

import argparse
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def run_sync(
    *,
    horizon: Optional[int] = None,
    realize_lookback: Optional[int] = None,
    toast_days: Optional[int] = None,
    venue_ids: Optional[list[int]] = None,
    dry_run: bool = False,
    skip_toast: bool = False,
    venues_only: bool = False,
    force_zero: Optional[bool] = None,
) -> dict:
    try:
        from resy.recorder import run_capture

        return run_capture(
            horizon=horizon,
            realize_lookback=realize_lookback,
            toast_days=toast_days,
            venue_ids=venue_ids,
            dry_run=dry_run,
            skip_toast=skip_toast,
            venues_only=venues_only,
            force_zero=force_zero,
        )
    except Exception as exc:
        logger.exception("Resy sync failed: %s", exc)
        return {"status": "error", "error": str(exc)}


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description="Resy Pace flight recorder — analytics reports to Snowflake")
    parser.add_argument("--dry-run", action="store_true", help="Auth and sample today's book; do not write Snowflake")
    parser.add_argument("--venues-only", action="store_true", help="Upsert resy_venue rows only")
    parser.add_argument("--skip-toast", action="store_true", help="Skip Toast resy_toast_daily")
    parser.add_argument("--force-zero", action="store_true", help="Allow a zero to overwrite a stored nonzero")
    parser.add_argument("--horizon", type=int, default=None, help="Days forward including today (default RESY_HORIZON or 90)")
    parser.add_argument("--realize-lookback", type=int, default=None, help="Completed days to realize (default 3)")
    parser.add_argument("--toast-days", type=int, default=None, help="Trailing Toast business days (default 3)")
    parser.add_argument(
        "--venue-id",
        type=lambda s: [int(x) for x in s.split(",") if x.strip()],
        default=None,
        help="Pace venue ids, e.g. 1 or 1,2",
    )
    args = parser.parse_args()
    from update_status import JOB_RESY, record_update_run

    result = run_sync(
        horizon=args.horizon,
        realize_lookback=args.realize_lookback,
        toast_days=args.toast_days,
        venue_ids=args.venue_id,
        dry_run=args.dry_run,
        skip_toast=args.skip_toast,
        venues_only=args.venues_only,
        force_zero=True if args.force_zero else None,
    )
    logging.getLogger(__name__).info("result %s", {k: v for k, v in result.items() if k != "error" or result.get("status") != "ok"})
    if result.get("status") == "error":
        logging.getLogger(__name__).error("%s", result.get("error"))
        if not args.dry_run:
            record_update_run(
                JOB_RESY,
                success=False,
                error_message=str(result.get("error") or "Resy sync failed"),
            )
        return 1
    if not args.dry_run:
        record_update_run(JOB_RESY, success=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
