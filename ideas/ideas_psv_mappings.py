"""Parse IDeaS PSV files and map headers to Snowflake column names."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Callable

PSV_FILENAME_RE = re.compile(
    r"^(?P<property_code>\d+)_(?P<category>.+)_(?P<file_prepare_date>\d{8})_"
    r"(?P<file_prepare_time>\d{4})_(?P<cutoff_date>\d{8})_(?P<feed_frequency>\w+)\.psv$",
    re.IGNORECASE,
)
ARCHIVE_FILENAME_RE = re.compile(
    r"^(?P<property_code>\d+)_(?P<file_prepare_date>\d{8})_(?P<file_prepare_time>\d{4})\.tar\.gz$",
    re.IGNORECASE,
)

DATE_FMT = "%d-%b-%Y"
TS_FMT = "%d-%b-%Y %H:%M"


@dataclass(frozen=True)
class PsvFileMeta:
    property_code: str
    category: str
    file_prepare_date: str
    file_prepare_time: str
    cutoff_date: str
    feed_frequency: str
    source_filename: str


@dataclass(frozen=True)
class ArchiveMeta:
    property_code: str
    file_prepare_date: str
    file_prepare_time: str
    source_filename: str


FieldSpec = tuple[str, str]  # (column_name, type)


def parse_psv_filename(name: str) -> PsvFileMeta | None:
    base = name.replace("\\", "/").rsplit("/", 1)[-1]
    match = PSV_FILENAME_RE.match(base)
    if not match:
        return None
    return PsvFileMeta(source_filename=base, **match.groupdict())


def parse_archive_filename(name: str) -> ArchiveMeta | None:
    base = name.replace("\\", "/").rsplit("/", 1)[-1]
    match = ARCHIVE_FILENAME_RE.match(base)
    if not match:
        return None
    return ArchiveMeta(source_filename=base, **match.groupdict())


def parse_psv_content(content: str) -> tuple[list[str], list[list[str]]]:
    lines = [line for line in content.splitlines() if line.strip()]
    if len(lines) < 2:
        return [], []
    headers = [h.strip() for h in lines[1].split("|")]
    rows: list[list[str]] = []
    for line in lines[2:]:
        values = line.split("|")
        if len(values) < len(headers):
            values.extend([""] * (len(headers) - len(values)))
        rows.append(values[: len(headers)])
    return headers, rows


def _blank(value: str | None) -> bool:
    return value is None or not str(value).strip()


def parse_date(value: str) -> date | None:
    if _blank(value):
        return None
    return datetime.strptime(value.strip(), DATE_FMT).date()


def parse_timestamp(value: str) -> datetime | None:
    if _blank(value):
        return None
    return datetime.strptime(value.strip(), TS_FMT)


def parse_int(value: str) -> int | None:
    if _blank(value):
        return None
    return int(float(value.strip()))


def parse_decimal(value: str) -> float | None:
    if _blank(value):
        return None
    return float(value.strip())


def parse_bool(value: str) -> bool | None:
    if _blank(value):
        return None
    normalized = value.strip().lower()
    if normalized in {"true", "yes", "1"}:
        return True
    if normalized in {"false", "no", "0"}:
        return False
    return None


def parse_str(value: str) -> str | None:
    if _blank(value):
        return None
    return value.strip()


def parse_month_year(value: str) -> date | None:
    """Parse IDeaS month labels like 'Apr 2026' to the first day of that month."""
    if _blank(value):
        return None
    return datetime.strptime(value.strip(), "%b %Y").date()


COERCE: dict[str, Callable[[str], Any]] = {
    "date": parse_date,
    "month_year": parse_month_year,
    "timestamp": parse_timestamp,
    "int": parse_int,
    "decimal": parse_decimal,
    "bool": parse_bool,
    "str": parse_str,
}


def ymd_to_date(value: str) -> date:
    return datetime.strptime(value, "%Y%m%d").date()


def meta_to_common(meta: PsvFileMeta) -> dict[str, Any]:
    return {
        "property_code": meta.property_code,
        "file_prepare_date": ymd_to_date(meta.file_prepare_date),
        "file_prepare_time": meta.file_prepare_time,
        "cutoff_date": ymd_to_date(meta.cutoff_date),
        "feed_frequency": meta.feed_frequency,
        "source_filename": meta.source_filename,
    }


def map_row(headers: list[str], values: list[str], field_map: dict[str, FieldSpec]) -> dict[str, Any]:
    row: dict[str, Any] = {}
    for header, raw in zip(headers, values):
        spec = field_map.get(header)
        if not spec:
            continue
        col, kind = spec
        row[col] = COERCE[kind](raw)
    return row


# ---------------------------------------------------------------------------
# Category -> table + header map
# ---------------------------------------------------------------------------

INFORMATIONAL_MAP: dict[str, FieldSpec] = {
    "Property Code": ("property_code", "str"),
    "Property Name": ("property_name", "str"),
    "System Date": ("system_date", "timestamp"),
    "System Mode": ("system_mode", "str"),
    "Build Type": ("build_type", "str"),
    "Transaction System Date": ("transaction_system_date", "timestamp"),
    "Transaction Data Population Date": ("transaction_data_population_date", "timestamp"),
    "Forecast Date": ("forecast_date", "timestamp"),
    "Control Date": ("control_date", "timestamp"),
    "Last RateShopping Extract Date": ("last_rate_shopping_extract_date", "timestamp"),
    "Unqualified Processed Date": ("unqualified_processed_date", "timestamp"),
    "Function Space System Date": ("function_space_system_date", "timestamp"),
    "Last Reputation Extract Date": ("last_reputation_extract_date", "timestamp"),
    "Decision Delivery Mode Date": ("decision_delivery_mode_date", "timestamp"),
    "Scheduled Decision Delivery Mode Date": ("scheduled_decision_delivery_mode_date", "timestamp"),
    "History Extract Date": ("history_extract_date", "timestamp"),
    "Unprocessed Extract Count": ("unprocessed_extract_count", "int"),
    "Processed Extract Count": ("processed_extract_count", "int"),
    "Missing Daily Extract Count": ("missing_daily_extract_count", "int"),
    "Unprocessed Rate Shopping Extract Count": ("unprocessed_rate_shopping_extract_count", "int"),
    "Configuration File Load Date": ("configuration_file_load_date", "timestamp"),
    "Out Of Order": ("out_of_order", "str"),
    "Rate Shopping Vendor": ("rate_shopping_vendor", "str"),
    "Demand360 Last Data Population Date": ("demand360_last_data_population_date", "timestamp"),
    "Booked": ("booked", "str"),
    "lastLDBUpdate": ("last_ldb_update", "str"),
    "BDE Forecast Window": ("bde_forecast_window", "int"),
    "BDE Decision Window": ("bde_decision_window", "int"),
    "IDP Forecast Window": ("idp_forecast_window", "int"),
    "IDP Decision Window": ("idp_decision_window", "int"),
    "GroupPricingExtendedWindow": ("group_pricing_extended_window", "int"),
    "Variable Decision Window": ("variable_decision_window", "int"),
}

ROOM_TYPE_MAP: dict[str, FieldSpec] = {
    "Occupancy Date": ("occupancy_date", "date"),
    "Comparison Date Last Year": ("comparison_date_last_year", "date"),
    "Room Class Code": ("room_class_code", "str"),
    "Room Type Code": ("room_type_code", "str"),
    "Room Type Description": ("room_type_description", "str"),
    "Inventory Data - Capacity This Year": ("capacity_this_year", "int"),
    "Inventory Data - Capacity Comparison Date Last Year Actual": ("capacity_comparison_date_last_year_actual", "int"),
    "Inventory Data - Occupancy On Books This Year": ("occupancy_on_books_this_year", "int"),
    "Inventory Data - Occupancy On Books Comparison Date Last Year": ("occupancy_on_books_comparison_date_last_year", "int"),
    "Inventory Data - Arrivals This Year": ("arrivals_this_year", "int"),
    "Inventory Data - Arrivals Comparison Date Last Year": ("arrivals_comparison_date_last_year", "int"),
    "Inventory Data - Departures This Year": ("departures_this_year", "int"),
    "Inventory Data - Departures Comparison Date Last Year": ("departures_comparison_date_last_year", "int"),
    "Inventory Data - Rooms N/A - OOO This Year": ("rooms_na_ooo_this_year", "int"),
    "Inventory Data - Rooms N/A - OOO Comparison Date Last Year": ("rooms_na_ooo_comparison_date_last_year", "int"),
    "Inventory Data - Rooms N/A - Other This Year": ("rooms_na_other_this_year", "int"),
    "Inventory Data - Rooms N/A - Other Comparison Date Last Year": ("rooms_na_other_comparison_date_last_year", "int"),
    "Inventory Data - Cancelled This Year": ("cancelled_this_year", "int"),
    "Inventory Data - Cancelled Comparison Date Last Year": ("cancelled_comparison_date_last_year", "int"),
    "Inventory Data - No Show This Year": ("no_show_this_year", "int"),
    "Inventory Data - No Show Comparison Date Last Year": ("no_show_comparison_date_last_year", "int"),
    "Inventory Data - Booked Room Revenue This Year": ("booked_room_revenue_this_year", "decimal"),
    "Inventory Data - Booked Room Revenue Comparison Date Last Year": ("booked_room_revenue_comparison_date_last_year", "decimal"),
    "Forecast Occupancy This Year": ("forecast_occupancy_this_year", "decimal"),
    "Forecast Occupancy Comparison Date Last Year": ("forecast_occupancy_comparison_date_last_year", "decimal"),
    "Forecast Room Revenue This Year": ("forecast_room_revenue_this_year", "decimal"),
    "Forecast Room Revenue Comparison Date Last Year": ("forecast_room_revenue_comparison_date_last_year", "decimal"),
    "Decisions - Overbooking This Year": ("decisions_overbooking_this_year", "int"),
    "Decisions - Overbooking Comparison Date Last Year": ("decisions_overbooking_comparison_date_last_year", "int"),
    "Decisions - LRV This Year": ("decisions_lrv_this_year", "decimal"),
    "Decisions  - LRV Comparison Date Last Year": ("decisions_lrv_comparison_date_last_year", "decimal"),
    "Decisions - BAR LOS1": ("decisions_bar_los1", "decimal"),
    "Decisions - BAR LOS2": ("decisions_bar_los2", "decimal"),
    "Decisions - BAR LOS3": ("decisions_bar_los3", "decimal"),
    "Decisions - BAR LOS4": ("decisions_bar_los4", "decimal"),
    "Decisions - BAR LOS5": ("decisions_bar_los5", "decimal"),
    "Decisions - BAR LOS6": ("decisions_bar_los6", "decimal"),
    "Decisions - BAR LOS7": ("decisions_bar_los7", "decimal"),
    "Optimal BAR LOS1": ("optimal_bar_los1", "decimal"),
    "Inventory Data - Occupancy On Books Same Time Last Year": ("occupancy_on_books_same_time_last_year", "int"),
    "Inventory Data - Booked Room Revenue Same Time Last Year": ("booked_room_revenue_same_time_last_year", "decimal"),
    "Comparison Date Two Year's Ago": ("comparison_date_two_years_ago", "date"),
    "Inventory Data - Occupancy On Books Same Time Two Years Ago": ("occupancy_on_books_same_time_two_years_ago", "int"),
    "Inventory Data - Booked Room Revenue Same Time Two Years Ago": ("booked_room_revenue_same_time_two_years_ago", "decimal"),
    "Inventory Data - Occupancy On Books Comparison Date Two Years Ago": ("occupancy_on_books_comparison_date_two_years_ago", "int"),
    "Inventory Data - Booked Room Revenue Comparison Date Two Years Ago": ("booked_room_revenue_comparison_date_two_years_ago", "decimal"),
}

ROOM_CLASS_MAP: dict[str, FieldSpec] = {
    "Occupancy Date": ("occupancy_date", "date"),
    "Comparison Date Last Year": ("comparison_date_last_year", "date"),
    "Room Class Code": ("room_class_code", "str"),
    "Remaining Demand - System Unconstrained Demand This year": ("remaining_demand_system_unconstrained_this_year", "decimal"),
    "Remaining Demand - System Unconstrained Demand Comparison Date Last Year": ("remaining_demand_system_unconstrained_comparison_ly", "decimal"),
    "Remaining Demand - User Demand This Year": ("remaining_demand_user_this_year", "decimal"),
    "Remaining Demand - User Demand Comparison Date Last Year": ("remaining_demand_user_comparison_ly", "decimal"),
    "Remaining Demand - System LOS 1": ("remaining_demand_system_los1", "decimal"),
    "Remaining Demand - System LOS 2": ("remaining_demand_system_los2", "decimal"),
    "Remaining Demand - System LOS 3": ("remaining_demand_system_los3", "decimal"),
    "Remaining Demand - System LOS 4": ("remaining_demand_system_los4", "decimal"),
    "Remaining Demand - System LOS 5": ("remaining_demand_system_los5", "decimal"),
    "Remaining Demand - System LOS 6": ("remaining_demand_system_los6", "decimal"),
    "Remaining Demand - System LOS 7": ("remaining_demand_system_los7", "decimal"),
    "Remaining Demand - User LOS 1": ("remaining_demand_user_los1", "decimal"),
    "Remaining Demand - User LOS 2": ("remaining_demand_user_los2", "decimal"),
    "Remaining Demand - User LOS 3": ("remaining_demand_user_los3", "decimal"),
    "Remaining Demand - User LOS 4": ("remaining_demand_user_los4", "decimal"),
    "Remaining Demand - User LOS 5": ("remaining_demand_user_los5", "decimal"),
    "Remaining Demand - User LOS 6": ("remaining_demand_user_los6", "decimal"),
    "Remaining Demand - User LOS 7": ("remaining_demand_user_los7", "decimal"),
}

ROOM_CLASS_CONFIG_MAP: dict[str, FieldSpec] = {
    "Room Class Code": ("room_class_code", "str"),
    "Room Class Name": ("room_class_name", "str"),
    "Room Class Description": ("room_class_description", "str"),
    "Master Class": ("master_class", "str"),
    "Room Type Code": ("room_type_code", "str"),
    "Room Type Name": ("room_type_name", "str"),
    "Room Type Description": ("room_type_description", "str"),
    "Room Type Capacity": ("room_type_capacity", "int"),
    "Discontinued Room Type": ("discontinued_room_type", "bool"),
}

MARKET_SEGMENT_MAP: dict[str, FieldSpec] = {
    "Occupancy Date": ("occupancy_date", "date"),
    "Comparison Date Last Year": ("comparison_date_last_year", "date"),
    "Forecast Group Code": ("forecast_group_code", "str"),
    "Business View Name": ("business_view_name", "str"),
    "Market Segment Code": ("market_segment_code", "str"),
    "Inventory Data - Occupancy On Books This year": ("occupancy_on_books_this_year", "int"),
    "Inventory Data - Occupancy On Books Comparison Date Last Year": ("occupancy_on_books_comparison_date_last_year", "int"),
    "Inventory Data - Arrivals This Year": ("arrivals_this_year", "int"),
    "Inventory Data - Arrivals Comparison Date Last Year": ("arrivals_comparison_date_last_year", "int"),
    "Inventory Data - Departures This Year": ("departures_this_year", "int"),
    "Inventory Data - Departures Comparison Date Last Year": ("departures_comparison_date_last_year", "int"),
    "Inventory Data - Cancelled This Year": ("cancelled_this_year", "int"),
    "Inventory Data - Cancelled Comparison Date Last Year": ("cancelled_comparison_date_last_year", "int"),
    "Inventory Data - No Show This Year": ("no_show_this_year", "int"),
    "Inventory Data - No Show Comparison Date Last Year": ("no_show_comparison_date_last_year", "int"),
    "Inventory Data - Booked Room Revenue This Year": ("booked_room_revenue_this_year", "decimal"),
    "Inventory Data - Booked Room Revenue Comparison Date Last Year": ("booked_room_revenue_comparison_date_last_year", "decimal"),
    "Forecast Occupancy This Year": ("forecast_occupancy_this_year", "decimal"),
    "Forecast Occupancy Comparison Date Last Year": ("forecast_occupancy_comparison_date_last_year", "decimal"),
    "Forecast Room Revenue  This Year": ("forecast_room_revenue_this_year", "decimal"),
    "Forecast Room Revenue  Comparison Date Last Year": ("forecast_room_revenue_comparison_date_last_year", "decimal"),
    "Inventory Data - Occupancy On Books Same Time Last Year": ("occupancy_on_books_same_time_last_year", "int"),
    "Inventory Data - Booked Room Revenue Same Time Last Year": ("booked_room_revenue_same_time_last_year", "decimal"),
    "Comparison Date Two Year's Ago": ("comparison_date_two_years_ago", "date"),
    "Inventory Data - Occupancy On Books Same Time Two Year's Ago": ("occupancy_on_books_same_time_two_years_ago", "int"),
    "Inventory Data - Booked Room Revenue Same Time Two Year's Ago": ("booked_room_revenue_same_time_two_years_ago", "decimal"),
    "Inventory Data - Booked Room Revenue Same Time Two Years Ago": ("booked_room_revenue_same_time_two_years_ago", "decimal"),
    "Inventory Data - Occupancy On Books Comparison Date Two Years Ago": ("occupancy_on_books_comparison_date_two_years_ago", "int"),
    "Inventory Data - Booked Room Revenue Comparison Date Two Years Ago": ("booked_room_revenue_comparison_date_two_years_ago", "decimal"),
}

MARKET_SEGMENT_CONFIG_MAP: dict[str, FieldSpec] = {
    "Business View Name": ("business_view_name", "str"),
    "Business View Description": ("business_view_description", "str"),
    "Forecast Group Code": ("forecast_group_code", "str"),
    "Forecast Group Name": ("forecast_group_name", "str"),
    "Forecast Group Description": ("forecast_group_description", "str"),
    "Market Segment Code": ("market_segment_code", "str"),
    "Market Segment Name": ("market_segment_name", "str"),
    "Market Segment Description": ("market_segment_description", "str"),
    "Business Type": ("business_type", "str"),
    "Contract": ("contract", "str"),
    "Booking": ("booking", "str"),
    "Selling": ("selling", "str"),
    "Forecast Type": ("forecast_type", "str"),
    "Control": ("control", "str"),
    "Linked": ("linked", "str"),
    "Priced By BAR": ("priced_by_bar", "str"),
    "Base Product": ("base_product", "str"),
}

FORECAST_GROUP_MAP: dict[str, FieldSpec] = {
    "Occupancy Date": ("occupancy_date", "date"),
    "Comparison Date Last Year": ("comparison_date_last_year", "date"),
    "Forecast Group Code": ("forecast_group_code", "str"),
    "Wash % - System This Year": ("wash_pct_system_this_year", "decimal"),
    "Wash % - System Comparison Date Last Year": ("wash_pct_system_comparison_ly", "decimal"),
    "Wash % - User Override This Year": ("wash_pct_user_override_this_year", "decimal"),
    "Wash % - User Override Comparison Date Last Year": ("wash_pct_user_override_comparison_ly", "decimal"),
    "Expiration Date": ("expiration_date", "date"),
    "Remaining Demand - System Unconstrained Total Demand This Year": ("remaining_demand_system_unconstrained_total_this_year", "decimal"),
    "Remaining Demand - System Unconstrained Total Demand Comparison Date Last Year": ("remaining_demand_system_unconstrained_total_comparison_ly", "decimal"),
    "Remaining Demand - User Total Demand": ("remaining_demand_user_total_this_year", "decimal"),
    "Remaining Demand - User Total Demand Comparison Date Last Year": ("remaining_demand_user_total_comparison_ly", "decimal"),
    "Remaining Demand - System LOS 1": ("remaining_demand_system_los1", "decimal"),
    "Remaining Demand - System LOS 2": ("remaining_demand_system_los2", "decimal"),
    "Remaining Demand - System LOS 3": ("remaining_demand_system_los3", "decimal"),
    "Remaining Demand - System LOS 4": ("remaining_demand_system_los4", "decimal"),
    "Remaining Demand - System LOS 5": ("remaining_demand_system_los5", "decimal"),
    "Remaining Demand - System LOS 6": ("remaining_demand_system_los6", "decimal"),
    "Remaining Demand - System LOS 7": ("remaining_demand_system_los7", "decimal"),
    "Remaining Demand - User LOS 1": ("remaining_demand_user_los1", "decimal"),
    "Remaining Demand - User LOS 2": ("remaining_demand_user_los2", "decimal"),
    "Remaining Demand - User LOS 3": ("remaining_demand_user_los3", "decimal"),
    "Remaining Demand - User LOS 4": ("remaining_demand_user_los4", "decimal"),
    "Remaining Demand - User LOS 5": ("remaining_demand_user_los5", "decimal"),
    "Remaining Demand - User LOS 6": ("remaining_demand_user_los6", "decimal"),
    "Remaining Demand - User LOS 7": ("remaining_demand_user_los7", "decimal"),
}

HOTEL_LEVEL_MAP: dict[str, FieldSpec] = {
    "Occupancy Date": ("occupancy_date", "date"),
    "Comparison Date Last Year": ("comparison_date_last_year", "date"),
    "Hotel Overbooking": ("hotel_overbooking", "int"),
    "Special Event Name This Year": ("special_event_name_this_year", "str"),
    "Special Event Name Last Year": ("special_event_name_last_year", "str"),
    "Budgeted Rooms Sold": ("budgeted_rooms_sold", "int"),
    "Budgeted Room Revenue": ("budgeted_room_revenue", "decimal"),
    "Property Wash %": ("property_wash_pct", "decimal"),
}

PRICING_MAP: dict[str, FieldSpec] = {
    "Occupancy Date": ("occupancy_date", "date"),
    "Rate Product Name": ("rate_product_name", "str"),
    "Room Class Code": ("room_class_code", "str"),
    "Room Type Code": ("room_type_code", "str"),
    "Price": ("price", "decimal"),
}

FORECAST_ARRIVALS_MAP: dict[str, FieldSpec] = {
    "Occupancy Date": ("occupancy_date", "date"),
    "On Books Arrivals": ("on_books_arrivals", "int"),
    "On Books Departures": ("on_books_departures", "int"),
    "On Books Stay Thrus": ("on_books_stay_thrus", "int"),
    "Forecast Arrivals": ("forecast_arrivals", "int"),
    "Forecast Departures": ("forecast_departures", "int"),
    "Forecast Stay Thrus": ("forecast_stay_thrus", "int"),
    "On Books Adults": ("on_books_adults", "int"),
    "On Books Children": ("on_books_children", "int"),
    "Forecast Adults": ("forecast_adults", "int"),
    "Forecast Children": ("forecast_children", "int"),
}

BENEFIT_MEASUREMENT_MAP: dict[str, FieldSpec] = {
    "Month": ("measurement_month", "month_year"),
    "Occupancy - Estimated Benefit": ("occupancy_estimated_benefit", "decimal"),
    "Revenue - Estimated Benefit": ("revenue_estimated_benefit", "decimal"),
    "ADR - Estimated Benefit": ("adr_estimated_benefit", "decimal"),
    "RevPAR - Estimated Benefit": ("revpar_estimated_benefit", "decimal"),
    "Occupancy - Simulated": ("occupancy_simulated", "decimal"),
    "Occupancy - Actual": ("occupancy_actual", "decimal"),
    "Revenue - Simulated": ("revenue_simulated", "decimal"),
    "Revenue - Actual": ("revenue_actual", "decimal"),
    "ADR - Simulated": ("adr_simulated", "decimal"),
    "ADR - Actual": ("adr_actual", "decimal"),
    "RevPAR - Simulated": ("revpar_simulated", "decimal"),
    "RevPAR - Actual": ("revpar_actual", "decimal"),
    "Occupancy - % Gain": ("occupancy_pct_gain", "decimal"),
    "Revenue - % Gain": ("revenue_pct_gain", "decimal"),
    "ADR - % Gain": ("adr_pct_gain", "decimal"),
    "RevPAR - % Gain": ("revpar_pct_gain", "decimal"),
    "Ancillary Revenue - Estimated Benefit": ("ancillary_revenue_estimated_benefit", "decimal"),
    "Ancillary Revenue - Simulated": ("ancillary_revenue_simulated", "decimal"),
    "Ancillary Revenue - Actual": ("ancillary_revenue_actual", "decimal"),
    "Ancillary Revenue - % Gain": ("ancillary_revenue_pct_gain", "decimal"),
    "Ancillary Profit - Estimated Benefit": ("ancillary_profit_estimated_benefit", "decimal"),
    "Ancillary Profit - Simulated": ("ancillary_profit_simulated", "decimal"),
    "Ancillary Profit - Actual": ("ancillary_profit_actual", "decimal"),
    "Ancillary Profit - % Gain": ("ancillary_profit_pct_gain", "decimal"),
    "Profit - Estimated - Benefit": ("profit_estimated_benefit", "decimal"),
    "Profit - Simulated": ("profit_simulated", "decimal"),
    "Profit - Actual": ("profit_actual", "decimal"),
    "Profit - % Gain": ("profit_pct_gain", "decimal"),
    "ProPOR - Estimated - Benefit": ("propor_estimated_benefit", "decimal"),
    "ProPOR - Simulated": ("propor_simulated", "decimal"),
    "ProPOR - Actual": ("propor_actual", "decimal"),
    "ProPOR - % Gain": ("propor_pct_gain", "decimal"),
    "ProPAR - Estimated - Benefit": ("propar_estimated_benefit", "decimal"),
    "ProPAR - Simulated": ("propar_simulated", "decimal"),
    "ProPAR - Actual": ("propar_actual", "decimal"),
    "ProPAR - % Gain": ("propar_pct_gain", "decimal"),
}

SAVED_GROUP_PRICING_EVALUATIONS_MAP: dict[str, FieldSpec] = {
    "Group Name": ("group_name", "str"),
    "Unique Group ID": ("unique_group_id", "str"),
    "Evaluated On": ("evaluated_on", "timestamp"),
    "Salesperson": ("salesperson", "str"),
    "Materialization": ("materialization", "str"),
    "Multiproperty Evaluation": ("multiproperty_evaluation", "str"),
    "Preferred Date": ("preferred_date", "date"),
    "Market Segment Code": ("market_segment_code", "str"),
    "Arrival Date": ("arrival_date", "date"),
    "Number of Nights": ("number_of_nights", "int"),
    "Evaluation Method": ("evaluation_method", "str"),
    "Room Type Code": ("room_type_code", "str"),
    "Number of Rooms": ("number_of_rooms", "int"),
    "Recommended Rate": ("recommended_rate", "decimal"),
    "Adjusted Rate": ("adjusted_rate", "decimal"),
    "Break Even Rate": ("break_even_rate", "decimal"),
    "Average MAR": ("average_mar", "decimal"),
    "Displaced Rooms": ("displaced_rooms", "decimal"),
    "Incremental Rooms": ("incremental_rooms", "decimal"),
    "Gross Revenue - Rooms": ("gross_revenue_rooms", "decimal"),
    "Cost - Rooms": ("cost_rooms", "decimal"),
    "Net Revenue - Rooms": ("net_revenue_rooms", "decimal"),
    "Gross Profit - Rooms": ("gross_profit_rooms", "decimal"),
    "Displaced Revenue - Rooms": ("displaced_revenue_rooms", "decimal"),
    "Displaced Profit - Rooms": ("displaced_profit_rooms", "decimal"),
    "Cost of Walk": ("cost_of_walk", "decimal"),
    "Net Profit - Rooms": ("net_profit_rooms", "decimal"),
    "Net Profit % - Rooms": ("net_profit_pct_rooms", "decimal"),
    "Gross Revenue - Ancillary": ("gross_revenue_ancillary", "decimal"),
    "Net Revenue - Ancillary": ("net_revenue_ancillary", "decimal"),
    "Gross Profit - Ancillary": ("gross_profit_ancillary", "decimal"),
    "Displaced Revenue - Ancillary": ("displaced_revenue_ancillary", "decimal"),
    "Displaced Profit - Ancillary": ("displaced_profit_ancillary", "decimal"),
    "Net Profit - Ancillary": ("net_profit_ancillary", "decimal"),
    "Net Profit % - Ancillary": ("net_profit_pct_ancillary", "decimal"),
    "Gross Revenue - C&B": ("gross_revenue_cnb", "decimal"),
    "Total Cost - C&B": ("total_cost_cnb", "decimal"),
    "Net Revenue - C&B": ("net_revenue_cnb", "decimal"),
    "Gross Profit - C&B": ("gross_profit_cnb", "decimal"),
    "Net Profit - C&B": ("net_profit_cnb", "decimal"),
    "Net Profit % - C&B": ("net_profit_pct_cnb", "decimal"),
    "Gross Revenue - Total": ("gross_revenue_total", "decimal"),
    "Cost - Total": ("cost_total", "decimal"),
    "Net Revenue - Total": ("net_revenue_total", "decimal"),
    "Gross Profit - Total": ("gross_profit_total", "decimal"),
    "Displaced Revenue - Total": ("displaced_revenue_total", "decimal"),
    "Displaced Profit - Total": ("displaced_profit_total", "decimal"),
    "Net Profit - Total": ("net_profit_total", "decimal"),
    "Net Profit %": ("net_profit_pct", "decimal"),
    "Adjust Gross Revenue - Total": ("adjust_gross_revenue_total", "decimal"),
    "Adjust Gross Profit - Total": ("adjust_gross_profit_total", "decimal"),
    "Adjust Net Profit - Total": ("adjust_net_profit_total", "decimal"),
    "Adjust Net Profit % - Total": ("adjust_net_profit_pct_total", "decimal"),
    "Rate Contracted": ("rate_contracted", "str"),
    "Notes": ("notes", "str"),
    "Booking Id": ("booking_id", "str"),
}

LDB_PROJECTIONS_MAP: dict[str, FieldSpec] = {
    "Occupancy Date": ("occupancy_date", "date"),
    "Market Segment Code": ("market_segment_code", "str"),
    "Projected Rooms": ("projected_rooms", "int"),
    "Projected Revenue": ("projected_revenue", "decimal"),
}

CHANNEL_FORECAST_MAP: dict[str, FieldSpec] = {
    "Occupancy Date": ("occupancy_date", "date"),
    "Channel": ("channel", "str"),
    "Source": ("source", "str"),
    "On Books": ("on_books", "int"),
    "On Books Revenue": ("on_books_revenue", "decimal"),
    "Net On Books Revenue": ("net_on_books_revenue", "decimal"),
    "Cost": ("cost", "decimal"),
    "Cost Forecast": ("cost_forecast", "decimal"),
    "Average Cost Forecast": ("average_cost_forecast", "decimal"),
    "Occupancy Forecast": ("occupancy_forecast", "decimal"),
    "Revenue Forecast": ("revenue_forecast", "decimal"),
    "Net Revenue Forecast": ("net_revenue_forecast", "decimal"),
    "ADR Forecast": ("adr_forecast", "decimal"),
    "Net ADR Forecast": ("net_adr_forecast", "decimal"),
    "RevPAR Forecast": ("revpar_forecast", "decimal"),
    "Net RevPAR Forecast": ("net_revpar_forecast", "decimal"),
}


@dataclass(frozen=True)
class CategoryConfig:
    table: str
    field_map: dict[str, FieldSpec]
    also_property: bool = False


# String PK columns: IDeaS may leave these blank (e.g. Unassigned room class with no room type).
TABLE_STRING_PK_COLUMNS: dict[str, tuple[str, ...]] = {
    "ideas_flight_recorder_room_type_daily": ("room_class_code", "room_type_code"),
    "ideas_flight_recorder_room_class_daily": ("room_class_code",),
    "ideas_flight_recorder_room_class_configuration": ("room_class_code", "room_type_code"),
    "ideas_flight_recorder_market_segment_daily": (
        "forecast_group_code",
        "business_view_name",
        "market_segment_code",
    ),
    "ideas_flight_recorder_market_segment_configuration": (
        "forecast_group_code",
        "business_view_name",
        "market_segment_code",
    ),
    "ideas_flight_recorder_forecast_group_wash_remaining_demand_daily": ("forecast_group_code",),
    "ideas_flight_recorder_pricing_daily": ("rate_product_name", "room_class_code", "room_type_code"),
    "ideas_flight_recorder_channel_forecast_daily": ("channel", "source"),
    "ideas_flight_recorder_saved_group_pricing_evaluations": (
        "unique_group_id",
        "room_type_code",
        "evaluation_method",
        "market_segment_code",
        "booking_id",
    ),
    "ideas_flight_recorder_ldb_projections_weekly": ("market_segment_code",),
}

TABLE_REQUIRED_COLUMNS: dict[str, tuple[str, ...]] = {
    "ideas_flight_recorder_room_type_daily": ("occupancy_date", "room_class_code"),
    "ideas_flight_recorder_room_class_daily": ("occupancy_date", "room_class_code"),
    "ideas_flight_recorder_room_class_configuration": ("room_class_code",),
    "ideas_flight_recorder_market_segment_daily": (
        "occupancy_date",
        "forecast_group_code",
        "business_view_name",
        "market_segment_code",
    ),
    "ideas_flight_recorder_market_segment_configuration": (
        "forecast_group_code",
        "business_view_name",
        "market_segment_code",
    ),
    "ideas_flight_recorder_forecast_group_wash_remaining_demand_daily": (
        "occupancy_date",
        "forecast_group_code",
    ),
    "ideas_flight_recorder_hotel_level_daily": ("occupancy_date",),
    "ideas_flight_recorder_pricing_daily": (
        "occupancy_date",
        "rate_product_name",
        "room_class_code",
        "room_type_code",
    ),
    "ideas_flight_recorder_forecast_arrivals_departures_daily": ("occupancy_date",),
    "ideas_flight_recorder_channel_forecast_daily": ("occupancy_date", "source"),
    "ideas_flight_recorder_saved_group_pricing_evaluations": (
        "unique_group_id",
        "evaluated_on",
        "arrival_date",
    ),
    "ideas_flight_recorder_benefit_measurement_monthly": ("measurement_month",),
    "ideas_flight_recorder_ldb_projections_weekly": ("occupancy_date", "market_segment_code"),
}


def sanitize_row(table: str, row: dict[str, Any]) -> dict[str, Any] | None:
    for col in TABLE_STRING_PK_COLUMNS.get(table, ()):
        if row.get(col) is None:
            row[col] = ""

    for col in TABLE_REQUIRED_COLUMNS.get(table, ()):
        if row.get(col) is None or (isinstance(row.get(col), str) and not str(row.get(col)).strip()):
            return None
    return row


CATEGORY_CONFIG: dict[str, CategoryConfig] = {
    "Informational": CategoryConfig("ideas_flight_recorder_informational_daily", INFORMATIONAL_MAP, also_property=True),
    "RoomType": CategoryConfig("ideas_flight_recorder_room_type_daily", ROOM_TYPE_MAP),
    "RoomClass": CategoryConfig("ideas_flight_recorder_room_class_daily", ROOM_CLASS_MAP),
    "RoomClassConfiguration": CategoryConfig("ideas_flight_recorder_room_class_configuration", ROOM_CLASS_CONFIG_MAP),
    "MarketSegment": CategoryConfig("ideas_flight_recorder_market_segment_daily", MARKET_SEGMENT_MAP),
    "MarketSegmentConfig": CategoryConfig("ideas_flight_recorder_market_segment_configuration", MARKET_SEGMENT_CONFIG_MAP),
    "ForecastGroup_Wash_RemainingDemand": CategoryConfig(
        "ideas_flight_recorder_forecast_group_wash_remaining_demand_daily", FORECAST_GROUP_MAP
    ),
    "HotelLevel": CategoryConfig("ideas_flight_recorder_hotel_level_daily", HOTEL_LEVEL_MAP),
    "Pricing": CategoryConfig("ideas_flight_recorder_pricing_daily", PRICING_MAP),
    "ForecastArrivalsDepartures": CategoryConfig(
        "ideas_flight_recorder_forecast_arrivals_departures_daily", FORECAST_ARRIVALS_MAP
    ),
    "ChannelForecast": CategoryConfig("ideas_flight_recorder_channel_forecast_daily", CHANNEL_FORECAST_MAP),
    "SavedGroupPricingEvaluations": CategoryConfig(
        "ideas_flight_recorder_saved_group_pricing_evaluations",
        SAVED_GROUP_PRICING_EVALUATIONS_MAP,
    ),
    "BenefitMeasurement": CategoryConfig(
        "ideas_flight_recorder_benefit_measurement_monthly",
        BENEFIT_MEASUREMENT_MAP,
    ),
    "LDBProjections": CategoryConfig(
        "ideas_flight_recorder_ldb_projections_weekly",
        LDB_PROJECTIONS_MAP,
    ),
}


def rows_for_psv(meta: PsvFileMeta, content: str) -> tuple[str, list[dict[str, Any]], bool]:
    config = CATEGORY_CONFIG.get(meta.category)
    if config is None:
        raise ValueError(f"Unsupported PSV category: {meta.category}")

    headers, raw_rows = parse_psv_content(content)
    common = meta_to_common(meta)
    rows: list[dict[str, Any]] = []

    skipped = 0
    for values in raw_rows:
        mapped = map_row(headers, values, config.field_map)
        mapped.update(common)
        if mapped.get("property_code") is None:
            mapped["property_code"] = meta.property_code
        mapped = sanitize_row(config.table, mapped)
        if mapped is None:
            skipped += 1
            continue
        rows.append(mapped)

    if skipped:
        import sys

        print(
            f"  WARN {meta.source_filename}: skipped {skipped} row(s) missing required PK fields",
            file=sys.stderr,
        )

    return config.table, rows, config.also_property
