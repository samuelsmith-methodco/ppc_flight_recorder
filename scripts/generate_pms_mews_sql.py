"""
Generate sql/pms-mews-flight-recorder-tables.sql from mews/csv_samples/*.csv
and minimal placeholder DDL for endpoints without CSV samples.

Run from repo root:
  cd ppc_flight_recorder && python scripts/generate_pms_mews_sql.py
"""
from __future__ import annotations

import csv
import re
from pathlib import Path
import json
from typing import Dict, List, Set

ROOT = Path(__file__).resolve().parents[1]
CSV_DIR = ROOT / "mews" / "csv_samples"
OUT_SQL = ROOT / "sql" / "pms-mews-flight-recorder-tables.sql"
OUT_SCHEMA_JSON = ROOT / "mews" / "generated_schema.json"

# CSV filenames (under csv_samples) merged into one logical Snowflake table
TABLE_GROUPS: Dict[str, List[str]] = {
    "configuration": ["01_configuration_get.csv"],
    "countries": ["01_countries_getAll.csv"],
    "currencies": ["01_currencies_getAll.csv"],
    "tax_environments": ["01_taxEnvironments_getAll.csv"],
    "taxations": ["01_taxations_getAll.csv"],
    "languages": ["01_languages_getAll.csv"],
    "customers": ["02_customers_getAll.csv"],
    "enterprises": ["04_enterprises_getAll.csv"],
    "companies": ["04_companies_getAll.csv"],
    "departments": ["04_departments_getAll.csv"],
    "counters": ["04_counters_getAll.csv"],
    "outlets": ["04_outlets_getAll.csv"],
    "resources": ["04_resources_getAll.csv"],
    "resource_blocks": ["04_resourceBlocks_getAll.csv"],
    "tasks": ["04_tasks_getAll.csv"],
    "services": ["04_services_getAll.csv", "08_services_getAll.csv", "10_services_getAll.csv"],
    "resource_categories": ["04_resourceCategories_getAll.csv"],
    "resource_category_assignments": ["04_resourceCategoryAssignments_getAll.csv"],
    "resource_category_image_assignments": ["04_resourceCategoryImageAssignments_getAll.csv"],
    "resource_features": ["04_resourceFeatures_getAll.csv"],
    "resource_feature_assignments": ["04_resourceFeatureAssignments_getAll.csv"],
    "exports": ["05_exports_getAll.csv"],
    "exchange_rates": ["06_exchangerates_getAll.csv"],
    "cashiers": ["06_cashiers_getAll.csv"],
    "accounting_categories": ["06_accountingcategories_getAll.csv"],
    "order_items": ["06_orderitems_getAll.csv"],
    "bills": ["06_bills_getAll.csv"],
    "credit_cards": ["06_creditcards_getAll.csv"],
    "payment_requests": ["06_paymentrequests_getAll.csv"],
    "availability_blocks": ["08_availabilityBlocks_getAll.csv"],
    "services_get_availability": ["08_services_getAvailability.csv"],
    "business_segments": ["08_businessSegments_getAll.csv"],
    "rates": ["08_rates_getAll.csv"],
    "companionships": ["08_companionships_getAll.csv"],
    "restrictions": ["08_restrictions_getAll.csv"],
    "vouchers": ["08_vouchers_getAll.csv"],
    "loyalty_programs": ["07_loyaltyPrograms_getAll.csv"],
    "loyalty_memberships": ["07_loyaltyMemberships_getAll.csv"],
    "reservations": ["10_reservations_getAll.csv"],
    "source_assignments": ["10_sourceassignments_getAll.csv"],
    "sources": ["10_sources_getAll.csv"],
    "reservation_groups": ["10_reservationGroups_getAll.csv"],
    "product_service_orders": ["12_productServiceOrders_getAll.csv"],
    "service_order_notes": ["12_serviceOrderNotes_getAll.csv"],
}

# Endpoints implemented in sync but no CSV sample in csv_samples — VARIANT + keys only
EXTRA_MINIMAL_LOGICAL: List[str] = [
    "company_contracts",
    "devices",
    "commands",
    "cashier_transactions",
    "outlet_items",
    "payments",
    "preauthorizations",
    "products",
    "rules",
    "resource_access_tokens",
    "message_threads",
    "messages",
    "routing_rules",
    "reservation_prices",
    "customers_search",
]

# rates/getPricing — explicit columns (Connector API response); not driven by csv_samples
RATES_GET_PRICING_SCHEMA: List[str] = [
    "currency",
    "dates_utc",
    "time_unit_starts_utc",
    "base_prices",
    "base_amount_prices",
    "category_prices",
    "category_adjustments",
    "age_category_adjustments",
    "relative_adjustment",
    "absolute_adjustment",
    "empty_unit_adjustment",
    "extra_unit_adjustment",
]


def emit_rates_get_pricing_pair() -> str:
    """DDL for rates/getPricing snapshot (matches Mews response root fields)."""
    cols = [
        '    "currency" VARCHAR(16),',
        '    "dates_utc" VARIANT,',
        '    "time_unit_starts_utc" VARIANT,',
        '    "base_prices" VARIANT,',
        '    "base_amount_prices" VARIANT,',
        '    "category_prices" VARIANT,',
        '    "category_adjustments" VARIANT,',
        '    "age_category_adjustments" VARIANT,',
        '    "relative_adjustment" FLOAT,',
        '    "absolute_adjustment" FLOAT,',
        '    "empty_unit_adjustment" FLOAT,',
        '    "extra_unit_adjustment" FLOAT,',
    ]
    daily = "pms_mews_rates_get_pricing_daily"
    diff = "pms_mews_rates_get_pricing_diff_daily"
    lines = [
        f"-- {daily} / {diff}",
        "-- rates/getPricing (Connector API): Currency, DatesUtc, TimeUnitStartsUtc, BasePrices,",
        "-- BaseAmountPrices, CategoryPrices, CategoryAdjustments, AgeCategoryAdjustments, adjustments",
        f"CREATE TABLE IF NOT EXISTS {daily} (",
        META_COLS_DDL,
        *cols,
        '    "record_json" VARIANT NOT NULL,',
        '    "created_at" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),',
        '    PRIMARY KEY ("snapshot_date", "enterprise_id", "entity_id")',
        ");",
        "",
        "-- Legacy table upgrade (run once if daily table existed with only record_json):",
        '-- ALTER TABLE pms_mews_rates_get_pricing_daily ADD COLUMN IF NOT EXISTS "currency" VARCHAR(16);',
        '-- ALTER TABLE pms_mews_rates_get_pricing_daily ADD COLUMN IF NOT EXISTS "dates_utc" VARIANT;',
        '-- ALTER TABLE pms_mews_rates_get_pricing_daily ADD COLUMN IF NOT EXISTS "time_unit_starts_utc" VARIANT;',
        '-- ALTER TABLE pms_mews_rates_get_pricing_daily ADD COLUMN IF NOT EXISTS "base_prices" VARIANT;',
        '-- ALTER TABLE pms_mews_rates_get_pricing_daily ADD COLUMN IF NOT EXISTS "base_amount_prices" VARIANT;',
        '-- ALTER TABLE pms_mews_rates_get_pricing_daily ADD COLUMN IF NOT EXISTS "category_prices" VARIANT;',
        '-- ALTER TABLE pms_mews_rates_get_pricing_daily ADD COLUMN IF NOT EXISTS "category_adjustments" VARIANT;',
        '-- ALTER TABLE pms_mews_rates_get_pricing_daily ADD COLUMN IF NOT EXISTS "age_category_adjustments" VARIANT;',
        '-- ALTER TABLE pms_mews_rates_get_pricing_daily ADD COLUMN IF NOT EXISTS "relative_adjustment" FLOAT;',
        '-- ALTER TABLE pms_mews_rates_get_pricing_daily ADD COLUMN IF NOT EXISTS "absolute_adjustment" FLOAT;',
        '-- ALTER TABLE pms_mews_rates_get_pricing_daily ADD COLUMN IF NOT EXISTS "empty_unit_adjustment" FLOAT;',
        '-- ALTER TABLE pms_mews_rates_get_pricing_daily ADD COLUMN IF NOT EXISTS "extra_unit_adjustment" FLOAT;',
        "",
        f"CREATE TABLE IF NOT EXISTS {diff} (",
        '    "snapshot_date" DATE NOT NULL,',
        '    "enterprise_id" VARCHAR(64) NOT NULL,',
        '    "entity_id" VARCHAR(256) NOT NULL,',
        '    "changed_metric_name" VARCHAR(256) NOT NULL,',
        '    "old_value" VARCHAR(65535),',
        '    "new_value" VARCHAR(65535),',
        '    "created_at" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),',
        '    PRIMARY KEY ("snapshot_date", "enterprise_id", "entity_id", "changed_metric_name")',
        ");",
        "",
    ]
    return "\n".join(lines)


def header_to_snake(h: str) -> str:
    h = (h or "").strip()
    if not h:
        return "col_empty"
    s = h.replace(".", "_").replace(" ", "_")
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    s = s.lower()
    s = re.sub(r"[^a-z0-9_]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    if not s:
        return "col_empty"
    if s[0].isdigit():
        s = "c_" + s
    return s


def read_csv_headers(path: Path) -> List[str]:
    with path.open("r", encoding="utf-8", newline="") as f:
        r = csv.reader(f)
        row = next(r, None)
        return list(row) if row else []


def merge_columns_for_table(files: List[str]) -> List[str]:
    seen: Set[str] = set()
    ordered: List[str] = []
    for fn in files:
        p = CSV_DIR / fn
        if not p.exists():
            continue
        for h in read_csv_headers(p):
            sn = header_to_snake(h)
            if sn in ("snapshot_date", "enterprise_id", "entity_id", "record_json", "created_at"):
                continue
            if sn not in seen:
                seen.add(sn)
                ordered.append(sn)
    return ordered


META_COLS_DDL = '''    "snapshot_date" DATE NOT NULL,
    "enterprise_id" VARCHAR(64) NOT NULL,
    "entity_id" VARCHAR(256) NOT NULL,
'''

def col_ddl(name: str) -> str:
    # Typed columns: use FLOAT for obvious numeric columns; else VARCHAR
    n = name.lower()
    if any(
        x in n
        for x in (
            "_utc",
            "latitude",
            "longitude",
            "amount",
            "count",
            "rate",
            "price",
            "micros",
            "precision",
            "offset",
            "number",
            "ordinal",
            "hour",
            "minute",
        )
    ):
        if "utc" in n or "time" in n:
            return f'    "{name}" VARCHAR(64)'
    if n.endswith("_micros") or n.endswith("_amount") or "latitude" in n or "longitude" in n:
        return f'    "{name}" FLOAT'
    if any(k in n for k in ("is_", "has_", "can_", "should_")) and n.split("_")[-1] not in ("utc", "code"):
        return f'    "{name}" BOOLEAN'
    return f'    "{name}" VARCHAR(65535)'


def emit_table_pair(logical: str, data_columns: List[str]) -> str:
    daily = f"pms_mews_{logical}_daily"
    diff = f"pms_mews_{logical}_diff_daily"
    lines = [
        f"-- {daily} / {diff}",
        f"CREATE TABLE IF NOT EXISTS {daily} (",
        META_COLS_DDL,
    ]
    for c in data_columns:
        lines.append(col_ddl(c) + ",")
    lines.extend(
        [
            '    "record_json" VARIANT NOT NULL,',
            '    "created_at" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),',
            f'    PRIMARY KEY ("snapshot_date", "enterprise_id", "entity_id")',
            ");",
            "",
            f"CREATE TABLE IF NOT EXISTS {diff} (",
            '    "snapshot_date" DATE NOT NULL,',
            '    "enterprise_id" VARCHAR(64) NOT NULL,',
            '    "entity_id" VARCHAR(256) NOT NULL,',
            '    "changed_metric_name" VARCHAR(256) NOT NULL,',
            '    "old_value" VARCHAR(65535),',
            '    "new_value" VARCHAR(65535),',
            '    "created_at" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),',
            '    PRIMARY KEY ("snapshot_date", "enterprise_id", "entity_id", "changed_metric_name")',
            ");",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    if not CSV_DIR.is_dir():
        raise SystemExit(f"Missing CSV directory: {CSV_DIR}")

    parts: List[str] = [
        "-- =============================================================================",
        "-- PMS Mews Flight Recorder — snapshot + diff tables (lowercase quoted identifiers)",
        "-- Generated by scripts/generate_pms_mews_sql.py — re-run after updating csv_samples",
        "-- =============================================================================",
        "",
    ]

    for logical, files in sorted(TABLE_GROUPS.items(), key=lambda x: x[0]):
        cols = merge_columns_for_table(files)
        parts.append(emit_table_pair(logical, cols))

    for logical in EXTRA_MINIMAL_LOGICAL:
        parts.append(
            emit_table_pair(
                logical,
                [],
            )
        )

    parts.append(emit_rates_get_pricing_pair())

    OUT_SQL.parent.mkdir(parents=True, exist_ok=True)
    OUT_SQL.write_text("\n".join(parts), encoding="utf-8")
    print(f"Wrote {OUT_SQL} ({len(parts)} chunks)")

    schema_map: Dict[str, List[str]] = {}
    for logical, files in TABLE_GROUPS.items():
        schema_map[logical] = merge_columns_for_table(files)
    for logical in EXTRA_MINIMAL_LOGICAL:
        schema_map.setdefault(logical, [])
    schema_map["rates_get_pricing"] = list(RATES_GET_PRICING_SCHEMA)
    OUT_SCHEMA_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_SCHEMA_JSON.write_text(json.dumps(schema_map, indent=2), encoding="utf-8")
    print(f"Wrote {OUT_SCHEMA_JSON}")


if __name__ == "__main__":
    main()
