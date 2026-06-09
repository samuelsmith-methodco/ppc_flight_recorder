"""
IDeaS G3 Flight Recorder — daily SFTP → Snowflake sync.

  cd ppc_flight_recorder
  pip install -r requirements.txt
  copy env.example.txt to .env (Snowflake + IDEAS_SFTP_*)
  python sync_ideas.py                    # yesterday + today delivery dates
  python sync_ideas.py --date 2026-06-08  # one delivery date
  python sync_ideas.py --from-date 2026-05-24
  python sync_ideas.py --test-sftp
"""

from __future__ import annotations

import argparse
import logging
import sys

from config import (
    IDEAS_SFTP_PROTOCOL,
    IDEAS_SFTP_REMOTE_DIR,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_SCHEMA,
    SNOWFLAKE_USER,
    SNOWFLAKE_WAREHOUSE,
)
from ideas.sftp_client import (
    IdeasSftpError,
    connect_client_with_retries,
    normalize_date,
    normalize_remote_dir,
    public_ip,
)
from ideas_storage import default_delivery_dates, run_sync

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)


def validate_config() -> bool:
    missing = [
        name
        for name, val in {
            "SNOWFLAKE_ACCOUNT": SNOWFLAKE_ACCOUNT,
            "SNOWFLAKE_USER": SNOWFLAKE_USER,
            "SNOWFLAKE_WAREHOUSE": SNOWFLAKE_WAREHOUSE,
            "SNOWFLAKE_DATABASE": SNOWFLAKE_DATABASE,
            "SNOWFLAKE_SCHEMA": SNOWFLAKE_SCHEMA,
        }.items()
        if not val
    ]
    if missing:
        logger.error("Missing env vars: %s", ", ".join(missing))
        return False
    return True


def test_sftp(protocol: str) -> int:
    from config import IDEAS_SFTP_HOST, IDEAS_SFTP_REMOTE_DIR, IDEAS_SFTP_USERNAME

    print(f"Host: {IDEAS_SFTP_HOST}")
    print(f"Username: {IDEAS_SFTP_USERNAME}")
    print(f"Public IP: {public_ip()}")
    print(f"Protocol: {protocol}")
    print()
    try:
        client = connect_client_with_retries(protocol)
    except IdeasSftpError as exc:
        print(f"FAILED after retries: {exc}", file=sys.stderr)
        return 1
    except ConnectionError as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1
    try:
        remote_dir = normalize_remote_dir(IDEAS_SFTP_REMOTE_DIR)
        names = client.list_names(remote_dir)
        print(f"OK: connected to {remote_dir}")
        print(f"Found {len(names)} file(s)")
        for name in names[:10]:
            print(f"  {name}")
        if len(names) > 10:
            print(f"  ... and {len(names) - 10} more")
        return 0
    finally:
        client.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="IDeaS G3 flight recorder — SFTP to Snowflake")
    parser.add_argument("--date", help="One delivery date (YYYY-MM-DD or YYYYMMDD)")
    parser.add_argument(
        "--from-date",
        dest="from_date",
        help="Delivery dates from this date onward, inclusive",
    )
    parser.add_argument("--remote-dir", default=IDEAS_SFTP_REMOTE_DIR)
    parser.add_argument(
        "--protocol",
        choices=["auto", "sftp", "ftps"],
        default=IDEAS_SFTP_PROTOCOL if IDEAS_SFTP_PROTOCOL in {"auto", "sftp", "ftps"} else "auto",
    )
    parser.add_argument(
        "--no-update-existing",
        action="store_true",
        help="Skip archives whose snapshot already exists in Snowflake",
    )
    parser.add_argument("--test-sftp", action="store_true", help="Test SFTP connection only")
    args = parser.parse_args()

    if args.test_sftp:
        return test_sftp(args.protocol)

    if not validate_config():
        return 1

    if args.date and args.from_date:
        logger.error("Use only one of --date or --from-date")
        return 1

    delivery_dates: list[str] | None = None
    from_date: str | None = None

    if args.date:
        delivery_dates = [normalize_date(args.date)]
    elif args.from_date:
        from_date = normalize_date(args.from_date)
    else:
        delivery_dates = default_delivery_dates()

    if delivery_dates:
        logger.info("Delivery dates: %s", ", ".join(delivery_dates))
    elif from_date:
        logger.info("From delivery date: %s (inclusive)", from_date)

    result = run_sync(
        delivery_dates=delivery_dates,
        from_date=from_date,
        remote_dir=args.remote_dir,
        protocol=args.protocol,
        no_update_existing=args.no_update_existing,
    )

    if result.get("status") == "error":
        logger.error("Sync aborted: %s", result.get("error"))
        return 1

    logger.info(
        "Done. processed=%s skipped=%s",
        result["archives_processed"],
        result["archives_skipped"],
    )
    for table, count in sorted(result.get("row_totals", {}).items()):
        logger.info("  %s: %d rows", table, count)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
