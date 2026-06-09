"""
IDeaS flight recorder — load SFTP archives into Snowflake (delete + insert per snapshot).
"""

from __future__ import annotations

import io
import logging
import tarfile
from datetime import date, timedelta
from typing import Any

import snowflake.connector

from config import SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA
from ideas.ideas_psv_mappings import (
    parse_archive_filename,
    parse_psv_filename,
    rows_for_psv,
    ymd_to_date,
)
from ideas.sftp_client import (
    IdeasSftpError,
    download_archive_bytes,
    list_remote_archives,
    normalize_date,
)
from snowflake_connection import get_connection

logger = logging.getLogger(__name__)

BATCH_SIZE = 500

EXTRACT_TABLES = (
    "ideas_flight_recorder_informational_daily",
    "ideas_flight_recorder_room_type_daily",
    "ideas_flight_recorder_room_class_daily",
    "ideas_flight_recorder_room_class_configuration",
    "ideas_flight_recorder_market_segment_daily",
    "ideas_flight_recorder_market_segment_configuration",
    "ideas_flight_recorder_forecast_group_wash_remaining_demand_daily",
    "ideas_flight_recorder_hotel_level_daily",
    "ideas_flight_recorder_pricing_daily",
    "ideas_flight_recorder_forecast_arrivals_departures_daily",
    "ideas_flight_recorder_channel_forecast_daily",
    "ideas_flight_recorder_saved_group_pricing_evaluations",
    "ideas_flight_recorder_benefit_measurement_monthly",
    "ideas_flight_recorder_ldb_projections_weekly",
)


def fq_table(table: str) -> str:
    return f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{table}"


def default_delivery_dates(today: date | None = None) -> list[str]:
    """Delivery dates for scheduled sync: yesterday and today (YYYYMMDD)."""
    ref = today or date.today()
    return [(ref - timedelta(days=1)).strftime("%Y%m%d"), ref.strftime("%Y%m%d")]


def extract_snapshot_exists(
    cursor,
    property_code: str,
    file_prepare_date,
    file_prepare_time: str,
) -> bool:
    cursor.execute(
        f"""
        SELECT 1
        FROM {fq_table("ideas_flight_recorder_informational_daily")}
        WHERE property_code = %s
          AND file_prepare_date = %s
          AND file_prepare_time = %s
        LIMIT 1
        """,
        (property_code, file_prepare_date, file_prepare_time),
    )
    return cursor.fetchone() is not None


def delete_extract_snapshot(
    cursor,
    property_code: str,
    file_prepare_date,
    file_prepare_time: str,
) -> int:
    total_deleted = 0
    try:
        for table in EXTRACT_TABLES:
            cursor.execute(
                f"""
                DELETE FROM {fq_table(table)}
                WHERE property_code = %s
                  AND file_prepare_date = %s
                  AND file_prepare_time = %s
                """,
                (property_code, file_prepare_date, file_prepare_time),
            )
            total_deleted += cursor.rowcount
    except snowflake.connector.errors.ProgrammingError as exc:
        if "does not exist" in str(exc):
            raise RuntimeError(
                "IDeaS tables not found in Snowflake. "
                "Run sql/ideas-flight-recorder-tables.sql first."
            ) from exc
        raise
    return total_deleted


def upsert_property(cursor, property_code: str, property_name: str | None) -> None:
    if not property_code:
        return
    cursor.execute(
        f"""
        MERGE INTO {fq_table("ideas_flight_recorder_property")} AS t
        USING (SELECT %s AS property_code, %s AS property_name) AS s
        ON t.property_code = s.property_code
        WHEN MATCHED AND s.property_name IS NOT NULL THEN
            UPDATE SET property_name = s.property_name, updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT (property_code, property_name) VALUES (s.property_code, s.property_name)
        """,
        (property_code, property_name),
    )


def insert_rows(cursor, table: str, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0

    columns = list(rows[0].keys())
    col_sql = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    sql = f"INSERT INTO {fq_table(table)} ({col_sql}) VALUES ({placeholders})"

    values = [[row.get(col) for col in columns] for row in rows]
    for offset in range(0, len(values), BATCH_SIZE):
        cursor.executemany(sql, values[offset : offset + BATCH_SIZE])
    return len(values)


def iter_psv_from_tar_bytes(data: bytes) -> list[tuple[str, str]]:
    items: list[tuple[str, str]] = []
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
        for member in archive.getmembers():
            if not member.isfile() or not member.name.lower().endswith(".psv"):
                continue
            extracted = archive.extractfile(member)
            if extracted is None:
                continue
            content = extracted.read().decode("utf-8", errors="replace")
            name = member.name.replace("\\", "/").rsplit("/", 1)[-1]
            items.append((name, content))
    return items


def load_psv_file(cursor, psv_name: str, content: str) -> dict[str, int]:
    meta = parse_psv_filename(psv_name)
    if meta is None:
        logger.warning("SKIP unknown PSV filename: %s", psv_name)
        return {}

    table, rows, also_property = rows_for_psv(meta, content)
    if also_property and rows:
        upsert_property(cursor, rows[0].get("property_code"), rows[0].get("property_name"))

    count = insert_rows(cursor, table, rows)
    logger.info("  %s: %d rows -> %s", psv_name, count, table)
    return {table: count}


def load_archive(cursor, archive_label: str, psv_files: list[tuple[str, str]]) -> dict[str, int]:
    if not psv_files:
        logger.warning("No PSV files in %s", archive_label)
        return {}

    first_meta = None
    for name, _ in psv_files:
        first_meta = parse_psv_filename(name)
        if first_meta:
            break
    if first_meta is None:
        logger.warning("Could not parse PSV metadata in %s", archive_label)
        return {}

    fpd = ymd_to_date(first_meta.file_prepare_date)
    deleted = delete_extract_snapshot(
        cursor,
        first_meta.property_code,
        fpd,
        first_meta.file_prepare_time,
    )
    if deleted:
        logger.info(
            "DEL  property=%s snapshot=%s %s (%d existing rows)",
            first_meta.property_code,
            first_meta.file_prepare_date,
            first_meta.file_prepare_time,
            deleted,
        )

    totals: dict[str, int] = {}
    logger.info("LOAD %s (%d PSV files)", archive_label, len(psv_files))
    for psv_name, content in sorted(psv_files, key=lambda item: item[0]):
        for table, count in load_psv_file(cursor, psv_name, content).items():
            totals[table] = totals.get(table, 0) + count
    return totals


def load_tar_bytes(cursor, archive_name: str, data: bytes) -> dict[str, int]:
    return load_archive(cursor, archive_name, iter_psv_from_tar_bytes(data))


def _sync_error_result(
    error: str,
    *,
    delivery_dates: list[str] | None,
    archives_processed: int = 0,
    archives_skipped: int = 0,
    row_totals: dict[str, int] | None = None,
) -> dict[str, Any]:
    return {
        "status": "error",
        "error": error,
        "archives_processed": archives_processed,
        "archives_skipped": archives_skipped,
        "delivery_dates": delivery_dates or [],
        "row_totals": row_totals or {},
    }


def run_sync(
    *,
    delivery_dates: list[str] | None = None,
    remote_dir: str | None = None,
    protocol: str | None = None,
    no_update_existing: bool = False,
    from_date: str | None = None,
) -> dict[str, Any]:
    """
    Download IDeaS archives from SFTP and load to Snowflake.

    Scheduled default: delivery_dates = [yesterday, today] (YYYYMMDD in archive name).
    Each archive: DELETE existing snapshot rows, then INSERT fresh data.
    """
    if delivery_dates is None and from_date is None:
        delivery_dates = default_delivery_dates()

    normalized_dates: list[str] | None = None
    if delivery_dates:
        normalized_dates = [normalize_date(d) for d in delivery_dates]

    if normalized_dates:
        try:
            archive_names = list_remote_archives(
                remote_dir=remote_dir,
                date_filters=normalized_dates,
                protocol=protocol,
            )
        except IdeasSftpError as exc:
            logger.error("IDeaS SFTP error listing archives, aborting sync: %s", exc)
            return _sync_error_result(str(exc), delivery_dates=normalized_dates)
    elif from_date:
        try:
            archive_names = list_remote_archives(
                remote_dir=remote_dir,
                from_date=normalize_date(from_date),
                protocol=protocol,
            )
        except IdeasSftpError as exc:
            logger.error("IDeaS SFTP error listing archives, aborting sync: %s", exc)
            return _sync_error_result(str(exc), delivery_dates=[])
    else:
        try:
            archive_names = list_remote_archives(remote_dir=remote_dir, protocol=protocol)
        except IdeasSftpError as exc:
            logger.error("IDeaS SFTP error listing archives, aborting sync: %s", exc)
            return _sync_error_result(str(exc), delivery_dates=[])

    if not archive_names:
        logger.warning("No remote IDeaS archives matched.")
        return {
            "status": "ok",
            "archives_processed": 0,
            "archives_skipped": 0,
            "delivery_dates": normalized_dates or [],
            "row_totals": {},
        }

    logger.info(
        "IDeaS sync: %d archive(s) for delivery dates %s",
        len(archive_names),
        normalized_dates or f"from {from_date}" if from_date else "all",
    )

    grand_totals: dict[str, int] = {}
    archives_processed = 0
    archives_skipped = 0

    with get_connection() as conn:
        cursor = conn.cursor()
        try:
            for archive_name in archive_names:
                if no_update_existing:
                    meta = parse_archive_filename(archive_name)
                    if meta and extract_snapshot_exists(
                        cursor,
                        meta.property_code,
                        ymd_to_date(meta.file_prepare_date),
                        meta.file_prepare_time,
                    ):
                        logger.info("SKIP %s (already loaded)", archive_name)
                        archives_skipped += 1
                        continue

                logger.info("GET  %s", archive_name)
                try:
                    data = download_archive_bytes(
                        archive_name=archive_name,
                        remote_dir=remote_dir,
                        protocol=protocol,
                    )
                except IdeasSftpError as exc:
                    logger.error(
                        "IDeaS SFTP error downloading %s, aborting sync: %s",
                        archive_name,
                        exc,
                    )
                    return _sync_error_result(
                        str(exc),
                        delivery_dates=normalized_dates or [],
                        archives_processed=archives_processed,
                        archives_skipped=archives_skipped,
                        row_totals=grand_totals,
                    )
                totals = load_tar_bytes(cursor, archive_name, data)
                if totals:
                    archives_processed += 1
                    for table, count in totals.items():
                        grand_totals[table] = grand_totals.get(table, 0) + count
        finally:
            cursor.close()

    return {
        "status": "ok",
        "archives_processed": archives_processed,
        "archives_skipped": archives_skipped,
        "delivery_dates": normalized_dates or [],
        "row_totals": grand_totals,
    }
