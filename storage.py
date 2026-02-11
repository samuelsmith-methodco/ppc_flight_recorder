"""
PPC Flight Recorder – Storage (Snowflake). Standalone, no Leonardo dependency.
Uses batch MERGE (one statement per table) and optional connection reuse to avoid repeated slow connects.
"""

import logging
from datetime import date
from typing import Any, Dict, List, Optional

from config import SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA
from snowflake_connection import execute, execute_many, execute_query, get_connection

logger = logging.getLogger(__name__)


def _table(name: str) -> str:
    """Return fully qualified table name (database.schema.table). Uses unquoted identifiers so Snowflake resolves to uppercase (matches DDL)."""
    if SNOWFLAKE_DATABASE and SNOWFLAKE_SCHEMA:
        return f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{name}"
    return name


def _safe_str(v: Any, max_len: int = 65535) -> Optional[str]:
    if v is None:
        return None
    s = str(v)
    return s[:max_len] if len(s) > max_len else s


def _run_with_conn(conn: Optional[Any], use_connection):
    """If conn is provided, run use_connection(conn) and return its result. Else open get_connection() and run inside it."""
    if conn is not None:
        return use_connection(conn)
    with get_connection() as c:
        return use_connection(c)


def upsert_control_state_daily(
    snapshot_date: date,
    customer_id: str,
    rows: List[Dict[str, Any]],
    conn: Optional[Any] = None,
) -> int:
    if not rows:
        return 0
    snapshot_str = snapshot_date.isoformat()
    tbl = _table("ppc_campaign_control_state_daily")
    def do(conn):
        values_parts = []
        params = {}
        for i, r in enumerate(rows):
            prefix = f"r{i}_"
            params[prefix + "campaign_id"] = r.get("campaign_id")
            params[prefix + "snapshot_date"] = snapshot_str
            params[prefix + "customer_id"] = customer_id
            params[prefix + "campaign_name"] = _safe_str(r.get("campaign_name"), 512)
            params[prefix + "status"] = _safe_str(r.get("status"), 32)
            params[prefix + "advertising_channel_type"] = _safe_str(r.get("advertising_channel_type"), 64)
            params[prefix + "advertising_channel_sub_type"] = _safe_str(r.get("advertising_channel_sub_type"), 64)
            params[prefix + "daily_budget_micros"] = r.get("daily_budget_micros")
            params[prefix + "daily_budget_amount"] = r.get("daily_budget_amount")
            params[prefix + "budget_delivery_method"] = _safe_str(r.get("budget_delivery_method"), 64)
            params[prefix + "bidding_strategy_type"] = _safe_str(r.get("bidding_strategy_type"), 64)
            params[prefix + "target_cpa_micros"] = r.get("target_cpa_micros")
            params[prefix + "target_cpa_amount"] = r.get("target_cpa_amount")
            params[prefix + "target_roas"] = r.get("target_roas")
            params[prefix + "target_impression_share_location"] = _safe_str(r.get("target_impression_share_location"), 32)
            params[prefix + "target_impression_share_location_fraction_micros"] = r.get("target_impression_share_location_fraction_micros")
            params[prefix + "geo_target_ids"] = _safe_str(r.get("geo_target_ids"), 4096)
            params[prefix + "geo_negative_ids"] = _safe_str(r.get("geo_negative_ids"), 4096)
            params[prefix + "geo_target_names"] = _safe_str(r.get("geo_target_names"), 4096)
            params[prefix + "geo_negative_names"] = _safe_str(r.get("geo_negative_names"), 4096)
            params[prefix + "geo_radius_json"] = _safe_str(r.get("geo_radius_json"), 65535)
            params[prefix + "account_timezone"] = _safe_str(r.get("account_timezone"), 64)
            params[prefix + "network_settings_target_google_search"] = r.get("network_settings_target_google_search")
            params[prefix + "network_settings_target_search_network"] = r.get("network_settings_target_search_network")
            params[prefix + "network_settings_target_content_network"] = r.get("network_settings_target_content_network")
            params[prefix + "network_settings_target_partner_search_network"] = r.get("network_settings_target_partner_search_network")
            params[prefix + "ad_schedule_json"] = _safe_str(r.get("ad_schedule_json"), 65535)
            params[prefix + "audience_target_count"] = r.get("audience_target_count")
            params[prefix + "campaign_type"] = _safe_str(r.get("campaign_type"), 128)
            params[prefix + "networks"] = _safe_str(r.get("networks"), 256)
            params[prefix + "campaign_start_date"] = r.get("campaign_start_date")
            params[prefix + "campaign_end_date"] = r.get("campaign_end_date")
            params[prefix + "location"] = _safe_str(r.get("location"), 4096)
            params[prefix + "active_bid_adj"] = _safe_str(r.get("active_bid_adj"), 256)
            values_parts.append(
                f"(%(r{i}_campaign_id)s, %(r{i}_snapshot_date)s::DATE, %(r{i}_customer_id)s, %(r{i}_campaign_name)s, %(r{i}_status)s, "
                f"%(r{i}_advertising_channel_type)s, %(r{i}_advertising_channel_sub_type)s, %(r{i}_daily_budget_micros)s, %(r{i}_daily_budget_amount)s, "
                f"%(r{i}_budget_delivery_method)s, %(r{i}_bidding_strategy_type)s, %(r{i}_target_cpa_micros)s, %(r{i}_target_cpa_amount)s, %(r{i}_target_roas)s, "
                f"%(r{i}_target_impression_share_location)s, %(r{i}_target_impression_share_location_fraction_micros)s, "
                f"%(r{i}_geo_target_ids)s, %(r{i}_geo_negative_ids)s, %(r{i}_geo_target_names)s, %(r{i}_geo_negative_names)s, %(r{i}_geo_radius_json)s, %(r{i}_account_timezone)s, "
                f"%(r{i}_network_settings_target_google_search)s, %(r{i}_network_settings_target_search_network)s, "
                f"%(r{i}_network_settings_target_content_network)s, %(r{i}_network_settings_target_partner_search_network)s, "
                f"%(r{i}_ad_schedule_json)s, %(r{i}_audience_target_count)s, "
                f"%(r{i}_campaign_type)s, %(r{i}_networks)s, %(r{i}_campaign_start_date)s::DATE, %(r{i}_campaign_end_date)s::DATE, "
                f"%(r{i}_location)s, %(r{i}_active_bid_adj)s)"
            )
        values_sql = ",\n                ".join(values_parts)
        v_cols = "campaign_id, snapshot_date, customer_id, campaign_name, status, advertising_channel_type, advertising_channel_sub_type, daily_budget_micros, daily_budget_amount, budget_delivery_method, bidding_strategy_type, target_cpa_micros, target_cpa_amount, target_roas, target_impression_share_location, target_impression_share_location_fraction_micros, geo_target_ids, geo_negative_ids, geo_target_names, geo_negative_names, geo_radius_json, account_timezone, network_settings_target_google_search, network_settings_target_search_network, network_settings_target_content_network, network_settings_target_partner_search_network, ad_schedule_json, audience_target_count, campaign_type, networks, campaign_start_date, campaign_end_date, location, active_bid_adj"
        merge_sql = f"""
            MERGE INTO {tbl} AS target
            USING (
                SELECT * FROM (VALUES
                {values_sql}
                ) AS v({v_cols})
            ) AS source
            ON target.campaign_id = source.campaign_id AND target.snapshot_date = source.snapshot_date AND target.customer_id = source.customer_id
            WHEN MATCHED THEN UPDATE SET
                campaign_name = source.campaign_name, status = source.status,
                advertising_channel_type = source.advertising_channel_type,
                advertising_channel_sub_type = source.advertising_channel_sub_type,
                daily_budget_micros = source.daily_budget_micros, daily_budget_amount = source.daily_budget_amount,
                budget_delivery_method = source.budget_delivery_method, bidding_strategy_type = source.bidding_strategy_type,
                target_cpa_micros = source.target_cpa_micros, target_cpa_amount = source.target_cpa_amount, target_roas = source.target_roas,
                target_impression_share_location = source.target_impression_share_location, target_impression_share_location_fraction_micros = source.target_impression_share_location_fraction_micros,
                geo_target_ids = source.geo_target_ids, geo_negative_ids = source.geo_negative_ids, geo_target_names = source.geo_target_names, geo_negative_names = source.geo_negative_names, geo_radius_json = source.geo_radius_json, account_timezone = source.account_timezone, network_settings_target_google_search = source.network_settings_target_google_search,
                network_settings_target_search_network = source.network_settings_target_search_network,
                network_settings_target_content_network = source.network_settings_target_content_network,
                network_settings_target_partner_search_network = source.network_settings_target_partner_search_network,
                ad_schedule_json = source.ad_schedule_json, audience_target_count = source.audience_target_count,
                campaign_type = source.campaign_type, networks = source.networks,
                campaign_start_date = source.campaign_start_date, campaign_end_date = source.campaign_end_date,
                location = source.location, active_bid_adj = source.active_bid_adj
            WHEN NOT MATCHED THEN INSERT (campaign_id, snapshot_date, customer_id, campaign_name, status, advertising_channel_type, advertising_channel_sub_type, daily_budget_micros, daily_budget_amount, budget_delivery_method, bidding_strategy_type, target_cpa_micros, target_cpa_amount, target_roas, target_impression_share_location, target_impression_share_location_fraction_micros, geo_target_ids, geo_negative_ids, geo_target_names, geo_negative_names, geo_radius_json, account_timezone, network_settings_target_google_search, network_settings_target_search_network, network_settings_target_content_network, network_settings_target_partner_search_network, ad_schedule_json, audience_target_count, campaign_type, networks, campaign_start_date, campaign_end_date, location, active_bid_adj)
            VALUES (source.campaign_id, source.snapshot_date, source.customer_id, source.campaign_name, source.status, source.advertising_channel_type, source.advertising_channel_sub_type, source.daily_budget_micros, source.daily_budget_amount, source.budget_delivery_method, source.bidding_strategy_type, source.target_cpa_micros, source.target_cpa_amount, source.target_roas, source.target_impression_share_location, source.target_impression_share_location_fraction_micros, source.geo_target_ids, source.geo_negative_ids, source.geo_target_names, source.geo_negative_names, source.geo_radius_json, source.account_timezone, source.network_settings_target_google_search, source.network_settings_target_search_network, source.network_settings_target_content_network, source.network_settings_target_partner_search_network, source.ad_schedule_json, source.audience_target_count, source.campaign_type, source.networks, source.campaign_start_date, source.campaign_end_date, source.location, source.active_bid_adj)
            """
        execute(conn, merge_sql, params)
        conn.commit()
        logger.info("ppc_flight_recorder: upserted %s control_state rows for customer_id=%s @ %s", len(rows), customer_id, snapshot_str)

    _run_with_conn(conn, do)
    return len(rows)


def upsert_outcomes_daily(outcome_date: date, customer_id: str, rows: List[Dict[str, Any]], conn: Optional[Any] = None) -> int:
    if not rows:
        return 0
    date_str = outcome_date.isoformat()
    tbl = _table("ppc_campaign_outcomes_daily")

    def do(conn):
        values_parts = []
        params = {}
        for i, r in enumerate(rows):
            cost_micros = r.get("cost_micros")
            if cost_micros is None and r.get("cost") is not None:
                cost_micros = int(float(r["cost"]) * 1_000_000)
            cost_micros = cost_micros or 0
            prefix = f"r{i}_"
            params[prefix + "campaign_id"] = r.get("campaignId") or r.get("campaign_id")
            params[prefix + "outcome_date"] = date_str
            params[prefix + "customer_id"] = customer_id
            params[prefix + "campaign_name"] = _safe_str(r.get("campaignName") or r.get("campaign_name"), 512)
            params[prefix + "impressions"] = int(r.get("impressions") or 0)
            params[prefix + "clicks"] = int(r.get("clicks") or 0)
            params[prefix + "cost_micros"] = cost_micros
            params[prefix + "cost_amount"] = float(r.get("cost") or r.get("cost_amount") or 0)
            params[prefix + "conversions"] = float(r.get("conversions") or 0)
            params[prefix + "conversions_value"] = float(r.get("conversionValue") or r.get("conversions_value") or 0)
            params[prefix + "ctr"] = r.get("ctr")
            params[prefix + "cpc"] = r.get("cpc")
            params[prefix + "cpa"] = r.get("cpa")
            params[prefix + "roas"] = r.get("roas")
            params[prefix + "cvr"] = r.get("cvr")
            params[prefix + "search_impression_share_pct"] = r.get("impressionSharePct") or r.get("search_impression_share_pct")
            params[prefix + "search_rank_lost_impression_share_pct"] = r.get("search_rank_lost_impression_share_pct")
            values_parts.append(
                f"(%(r{i}_campaign_id)s, %(r{i}_outcome_date)s::DATE, %(r{i}_customer_id)s, %(r{i}_campaign_name)s, %(r{i}_impressions)s, %(r{i}_clicks)s, %(r{i}_cost_micros)s, %(r{i}_cost_amount)s, "
                f"%(r{i}_conversions)s, %(r{i}_conversions_value)s, %(r{i}_ctr)s, %(r{i}_cpc)s, %(r{i}_cpa)s, %(r{i}_roas)s, %(r{i}_cvr)s, "
                f"%(r{i}_search_impression_share_pct)s, %(r{i}_search_rank_lost_impression_share_pct)s)"
            )
        values_sql = ",\n                ".join(values_parts)
        merge_sql = f"""
            MERGE INTO {tbl} AS target
            USING (SELECT * FROM (VALUES {values_sql}) AS v(campaign_id, outcome_date, customer_id, campaign_name, impressions, clicks, cost_micros, cost_amount, conversions, conversions_value, ctr, cpc, cpa, roas, cvr, search_impression_share_pct, search_rank_lost_impression_share_pct)) AS source
            ON target.campaign_id = source.campaign_id AND target.outcome_date = source.outcome_date AND target.customer_id = source.customer_id
            WHEN MATCHED THEN UPDATE SET campaign_name = source.campaign_name, impressions = source.impressions, clicks = source.clicks, cost_micros = source.cost_micros, cost_amount = source.cost_amount, conversions = source.conversions, conversions_value = source.conversions_value, ctr = source.ctr, cpc = source.cpc, cpa = source.cpa, roas = source.roas, cvr = source.cvr, search_impression_share_pct = source.search_impression_share_pct, search_rank_lost_impression_share_pct = source.search_rank_lost_impression_share_pct
            WHEN NOT MATCHED THEN INSERT (campaign_id, outcome_date, customer_id, campaign_name, impressions, clicks, cost_micros, cost_amount, conversions, conversions_value, ctr, cpc, cpa, roas, cvr, search_impression_share_pct, search_rank_lost_impression_share_pct) VALUES (source.campaign_id, source.outcome_date, source.customer_id, source.campaign_name, source.impressions, source.clicks, source.cost_micros, source.cost_amount, source.conversions, source.conversions_value, source.ctr, source.cpc, source.cpa, source.roas, source.cvr, source.search_impression_share_pct, source.search_rank_lost_impression_share_pct)
            """
        execute(conn, merge_sql, params)
        conn.commit()
        logger.info("ppc_flight_recorder: upserted %s outcomes for customer_id=%s @ %s", len(rows), customer_id, date_str)

    _run_with_conn(conn, do)
    return len(rows)


def upsert_outcomes_batch(rows: List[Dict[str, Any]], conn: Optional[Any] = None) -> int:
    """Batch upsert outcomes for multiple dates. Rows must have 'outcome_date' (YYYY-MM-DD string) and 'customer_id'."""
    if not rows:
        return 0
    tbl = _table("ppc_campaign_outcomes_daily")

    def do(conn):
        values_parts = []
        params = {}
        for i, r in enumerate(rows):
            cost_micros = r.get("cost_micros")
            if cost_micros is None and r.get("cost") is not None:
                cost_micros = int(float(r["cost"]) * 1_000_000)
            cost_micros = cost_micros or 0
            prefix = f"r{i}_"
            outcome_date_str = r.get("outcome_date")
            if not outcome_date_str:
                continue
            params[prefix + "campaign_id"] = r.get("campaignId") or r.get("campaign_id")
            params[prefix + "outcome_date"] = outcome_date_str
            params[prefix + "customer_id"] = r.get("customer_id")
            params[prefix + "campaign_name"] = _safe_str(r.get("campaignName") or r.get("campaign_name"), 512)
            params[prefix + "impressions"] = int(r.get("impressions") or 0)
            params[prefix + "clicks"] = int(r.get("clicks") or 0)
            params[prefix + "cost_micros"] = cost_micros
            params[prefix + "cost_amount"] = float(r.get("cost") or r.get("cost_amount") or 0)
            params[prefix + "conversions"] = float(r.get("conversions") or 0)
            params[prefix + "conversions_value"] = float(r.get("conversionValue") or r.get("conversions_value") or 0)
            params[prefix + "ctr"] = r.get("ctr")
            params[prefix + "cpc"] = r.get("cpc")
            params[prefix + "cpa"] = r.get("cpa")
            params[prefix + "roas"] = r.get("roas")
            params[prefix + "cvr"] = r.get("cvr")
            params[prefix + "search_impression_share_pct"] = r.get("impressionSharePct") or r.get("search_impression_share_pct")
            params[prefix + "search_rank_lost_impression_share_pct"] = r.get("search_rank_lost_impression_share_pct")
            values_parts.append(
                f"(%(r{i}_campaign_id)s, %(r{i}_outcome_date)s::DATE, %(r{i}_customer_id)s, %(r{i}_campaign_name)s, %(r{i}_impressions)s, %(r{i}_clicks)s, %(r{i}_cost_micros)s, %(r{i}_cost_amount)s, "
                f"%(r{i}_conversions)s, %(r{i}_conversions_value)s, %(r{i}_ctr)s, %(r{i}_cpc)s, %(r{i}_cpa)s, %(r{i}_roas)s, %(r{i}_cvr)s, "
                f"%(r{i}_search_impression_share_pct)s, %(r{i}_search_rank_lost_impression_share_pct)s)"
            )
        if not values_parts:
            return 0
        values_sql = ",\n                ".join(values_parts)
        merge_sql = f"""
            MERGE INTO {tbl} AS target
            USING (SELECT * FROM (VALUES {values_sql}) AS v(campaign_id, outcome_date, customer_id, campaign_name, impressions, clicks, cost_micros, cost_amount, conversions, conversions_value, ctr, cpc, cpa, roas, cvr, search_impression_share_pct, search_rank_lost_impression_share_pct)) AS source
            ON target.campaign_id = source.campaign_id AND target.outcome_date = source.outcome_date AND target.customer_id = source.customer_id
            WHEN MATCHED THEN UPDATE SET campaign_name = source.campaign_name, impressions = source.impressions, clicks = source.clicks, cost_micros = source.cost_micros, cost_amount = source.cost_amount, conversions = source.conversions, conversions_value = source.conversions_value, ctr = source.ctr, cpc = source.cpc, cpa = source.cpa, roas = source.roas, cvr = source.cvr, search_impression_share_pct = source.search_impression_share_pct, search_rank_lost_impression_share_pct = source.search_rank_lost_impression_share_pct
            WHEN NOT MATCHED THEN INSERT (campaign_id, outcome_date, customer_id, campaign_name, impressions, clicks, cost_micros, cost_amount, conversions, conversions_value, ctr, cpc, cpa, roas, cvr, search_impression_share_pct, search_rank_lost_impression_share_pct) VALUES (source.campaign_id, source.outcome_date, source.customer_id, source.campaign_name, source.impressions, source.clicks, source.cost_micros, source.cost_amount, source.conversions, source.conversions_value, source.ctr, source.cpc, source.cpa, source.roas, source.cvr, source.search_impression_share_pct, source.search_rank_lost_impression_share_pct)
            """
        execute(conn, merge_sql, params)
        conn.commit()
        logger.info("ppc_flight_recorder: batch upserted %s outcomes", len(rows))

    _run_with_conn(conn, do)
    return len(rows)


def get_outcomes_for_date(customer_id: str, outcome_date: date, conn: Optional[Any] = None) -> List[Dict[str, Any]]:
    """Load outcomes for a given customer_id/date (for outcome diff computation)."""
    tbl = _table("ppc_campaign_outcomes_daily")

    def do(conn):
        q = f"SELECT campaign_id, outcome_date, customer_id, campaign_name, impressions, clicks, cost_micros, cost_amount, conversions, conversions_value, ctr, cpc, cpa, roas, cvr, search_impression_share_pct, search_rank_lost_impression_share_pct FROM {tbl} WHERE customer_id = %(customer_id)s AND outcome_date = %(outcome_date)s"
        return execute_query(conn, q, {"customer_id": customer_id, "outcome_date": outcome_date.isoformat()})

    df = _run_with_conn(conn, do)
    if df.empty:
        return []
    df.columns = [c.lower() for c in df.columns]
    return df.to_dict("records")


def insert_outcomes_diff_daily(outcome_date: date, customer_id: str, diff_rows: List[Dict[str, Any]], conn: Optional[Any] = None) -> int:
    if not diff_rows:
        return 0
    tbl = _table("ppc_campaign_outcomes_diff_daily")
    date_str = outcome_date.isoformat()

    def do(conn):
        execute(conn, f"DELETE FROM {tbl} WHERE outcome_date = %(outcome_date)s::DATE AND customer_id = %(customer_id)s", {"outcome_date": date_str, "customer_id": customer_id})
        insert_sql = f"INSERT INTO {tbl} (campaign_id, outcome_date, customer_id, changed_metric_name, old_value, new_value) VALUES (%(campaign_id)s, %(outcome_date)s::DATE, %(customer_id)s, %(changed_metric_name)s, %(old_value)s, %(new_value)s)"
        params_list = [{"campaign_id": r["campaign_id"], "outcome_date": date_str, "customer_id": customer_id, "changed_metric_name": _safe_str(r["changed_metric_name"], 128), "old_value": _safe_str(r.get("old_value"), 65535), "new_value": _safe_str(r.get("new_value"), 65535)} for r in diff_rows]
        execute_many(conn, insert_sql, params_list)
        conn.commit()
        logger.info("ppc_flight_recorder: inserted %s outcome diff rows for customer_id=%s @ %s", len(diff_rows), customer_id, date_str)

    _run_with_conn(conn, do)
    return len(diff_rows)


def get_control_state_for_date(customer_id: str, snapshot_date: date, conn: Optional[Any] = None) -> List[Dict[str, Any]]:
    tbl = _table("ppc_campaign_control_state_daily")
    params = {"customer_id": customer_id, "snapshot_date": snapshot_date.isoformat()}
    q = f"SELECT campaign_id, campaign_name, status, advertising_channel_type, advertising_channel_sub_type, daily_budget_micros, daily_budget_amount, budget_delivery_method, bidding_strategy_type, target_cpa_micros, target_cpa_amount, target_roas, target_impression_share_location, target_impression_share_location_fraction_micros, geo_target_ids, geo_negative_ids, geo_target_names, geo_negative_names, geo_radius_json, account_timezone, network_settings_target_google_search, network_settings_target_search_network, network_settings_target_content_network, network_settings_target_partner_search_network, ad_schedule_json, audience_target_count, campaign_type, networks, campaign_start_date, campaign_end_date, location, active_bid_adj FROM {tbl} WHERE customer_id = %(customer_id)s AND snapshot_date = %(snapshot_date)s"
    df = _run_with_conn(conn, lambda c: execute_query(c, q, params))
    if df.empty:
        return []
    df.columns = [c.lower() for c in df.columns]
    return df.to_dict("records")


def insert_control_diff_daily(snapshot_date: date, customer_id: str, diff_rows: List[Dict[str, Any]], conn: Optional[Any] = None) -> int:
    if not diff_rows:
        return 0
    tbl = _table("ppc_campaign_control_diff_daily")
    date_str = snapshot_date.isoformat()

    def do(conn):
        execute(conn, f"DELETE FROM {tbl} WHERE snapshot_date = %(snapshot_date)s::DATE AND customer_id = %(customer_id)s", {"snapshot_date": date_str, "customer_id": customer_id})
        insert_sql = f"INSERT INTO {tbl} (campaign_id, snapshot_date, customer_id, changed_metric_name, old_value, new_value) VALUES (%(campaign_id)s, %(snapshot_date)s::DATE, %(customer_id)s, %(changed_metric_name)s, %(old_value)s, %(new_value)s)"
        params_list = [{"campaign_id": r["campaign_id"], "snapshot_date": date_str, "customer_id": customer_id, "changed_metric_name": _safe_str(r["changed_metric_name"], 128), "old_value": _safe_str(r.get("old_value"), 65535), "new_value": _safe_str(r.get("new_value"), 65535)} for r in diff_rows]
        execute_many(conn, insert_sql, params_list)
        conn.commit()
        logger.info("ppc_flight_recorder: inserted %s control_state diff rows for customer_id=%s @ %s", len(diff_rows), customer_id, date_str)

    _run_with_conn(conn, do)
    return len(diff_rows)


def upsert_geo_targeting_daily(
    snapshot_date: date,
    customer_id: str,
    rows: List[Dict[str, Any]],
    conn: Optional[Any] = None,
) -> int:
    """Replace all geo targeting rows for (snapshot_date, customer_id) then insert the given rows. When rows is empty, only the DELETE runs (clears snapshot for that date)."""
    tbl = _table("ppc_campaign_geo_targeting_daily")
    date_str = snapshot_date.isoformat()

    def do(conn):
        execute(conn, f"DELETE FROM {tbl} WHERE snapshot_date = %(snapshot_date)s::DATE AND customer_id = %(customer_id)s", {"snapshot_date": date_str, "customer_id": customer_id})
        if not rows:
            conn.commit()
            return
        insert_sql = (
            f"INSERT INTO {tbl} (snapshot_date, customer_id, campaign_id, campaign_name, criterion_id, criterion_type, ordinal, "
            "geo_target_constant, geo_name, negative, positive_geo_target_type, negative_geo_target_type, "
            "proximity_street_address, proximity_city_name, radius, radius_units, latitude_micro, longitude_micro, estimated_reach) "
            "VALUES (%(snapshot_date)s::DATE, %(customer_id)s, %(campaign_id)s, %(campaign_name)s, %(criterion_id)s, %(criterion_type)s, %(ordinal)s, "
            "%(geo_target_constant)s, %(geo_name)s, %(negative)s, %(positive_geo_target_type)s, %(negative_geo_target_type)s, "
            "%(proximity_street_address)s, %(proximity_city_name)s, %(radius)s, %(radius_units)s, %(latitude_micro)s, %(longitude_micro)s, %(estimated_reach)s)"
        )
        params_list = [
            {
                "snapshot_date": date_str,
                "customer_id": customer_id,
                "campaign_id": r.get("campaign_id"),
                "campaign_name": _safe_str(r.get("campaign_name"), 512),
                "criterion_id": _safe_str(r.get("criterion_id"), 64),
                "criterion_type": _safe_str(r.get("criterion_type"), 32),
                "ordinal": r.get("ordinal"),
                "geo_target_constant": _safe_str(r.get("geo_target_constant"), 256),
                "geo_name": _safe_str(r.get("geo_name"), 512),
                "negative": r.get("negative"),
                "positive_geo_target_type": _safe_str(r.get("positive_geo_target_type"), 64),
                "negative_geo_target_type": _safe_str(r.get("negative_geo_target_type"), 64),
                "proximity_street_address": _safe_str(r.get("proximity_street_address"), 1024),
                "proximity_city_name": _safe_str(r.get("proximity_city_name"), 256),
                "radius": r.get("radius"),
                "radius_units": _safe_str(r.get("radius_units"), 16),
                "latitude_micro": r.get("latitude_micro"),
                "longitude_micro": r.get("longitude_micro"),
                "estimated_reach": r.get("estimated_reach"),
            }
            for r in rows
        ]
        execute_many(conn, insert_sql, params_list)
        conn.commit()
        logger.info("ppc_flight_recorder: upserted %s geo_targeting rows for customer_id=%s @ %s", len(rows), customer_id, date_str)

    _run_with_conn(conn, do)
    return len(rows)


def get_geo_targeting_for_date(customer_id: str, snapshot_date: date, conn: Optional[Any] = None) -> List[Dict[str, Any]]:
    """Return geo targeting rows for (customer_id, snapshot_date) from ppc_campaign_geo_targeting_daily."""
    tbl = _table("ppc_campaign_geo_targeting_daily")
    params = {"customer_id": customer_id, "snapshot_date": snapshot_date.isoformat()}
    q = (
        f"SELECT snapshot_date, customer_id, campaign_id, campaign_name, criterion_id, criterion_type, ordinal, "
        f"geo_target_constant, geo_name, negative, positive_geo_target_type, negative_geo_target_type, "
        f"proximity_street_address, proximity_city_name, radius, radius_units, latitude_micro, longitude_micro, estimated_reach "
        f"FROM {tbl} WHERE customer_id = %(customer_id)s AND snapshot_date = %(snapshot_date)s::DATE ORDER BY campaign_id, criterion_type, ordinal"
    )
    df = _run_with_conn(conn, lambda c: execute_query(c, q, params))
    if df.empty:
        return []
    df.columns = [c.lower() for c in df.columns]
    return df.to_dict("records")


def insert_geo_targeting_diff_daily(snapshot_date: date, customer_id: str, diff_rows: List[Dict[str, Any]], conn: Optional[Any] = None) -> int:
    """Insert day-over-day geo targeting changes into ppc_campaign_geo_targeting_diff_daily. Replaces all rows for (snapshot_date, customer_id)."""
    if not diff_rows:
        return 0
    tbl = _table("ppc_campaign_geo_targeting_diff_daily")
    date_str = snapshot_date.isoformat()

    def do(conn):
        execute(conn, f"DELETE FROM {tbl} WHERE snapshot_date = %(snapshot_date)s::DATE AND customer_id = %(customer_id)s", {"snapshot_date": date_str, "customer_id": customer_id})
        insert_sql = (
            f"INSERT INTO {tbl} (snapshot_date, customer_id, campaign_id, criterion_type, criterion_id, changed_metric_name, old_value, new_value) "
            "VALUES (%(snapshot_date)s::DATE, %(customer_id)s, %(campaign_id)s, %(criterion_type)s, %(criterion_id)s, %(changed_metric_name)s, %(old_value)s, %(new_value)s)"
        )
        params_list = [
            {
                "snapshot_date": date_str,
                "customer_id": customer_id,
                "campaign_id": r["campaign_id"],
                "criterion_type": _safe_str(r.get("criterion_type"), 32),
                "criterion_id": _safe_str(r.get("criterion_id"), 64),
                "changed_metric_name": _safe_str(r["changed_metric_name"], 128),
                "old_value": _safe_str(r.get("old_value"), 65535),
                "new_value": _safe_str(r.get("new_value"), 65535),
            }
            for r in diff_rows
        ]
        execute_many(conn, insert_sql, params_list)
        conn.commit()
        logger.info("ppc_flight_recorder: inserted %s geo_targeting diff rows for customer_id=%s @ %s", len(diff_rows), customer_id, date_str)

    _run_with_conn(conn, do)
    return len(diff_rows)


def upsert_ga4_traffic_acquisition_daily(rows: List[Dict[str, Any]], conn: Optional[Any] = None) -> int:
    if not rows:
        return 0
    tbl = _table("ppc_ga4_traffic_acquisition_daily")

    def do(conn):
        values_parts = []
        params = {}
        for i, r in enumerate(rows):
            prefix = f"r{i}_"
            params[prefix + "project"] = r.get("project")
            params[prefix + "acquisition_date"] = r.get("acquisition_date")
            params[prefix + "dimension_type"] = r.get("dimension_type")
            params[prefix + "dimension_value"] = _safe_str(r.get("dimension_value"), 512)
            params[prefix + "sessions"] = int(r.get("sessions") or 0)
            params[prefix + "engaged_sessions"] = int(r.get("engaged_sessions") or 0)
            params[prefix + "total_revenue"] = float(r.get("total_revenue") or 0)
            params[prefix + "event_count"] = int(r.get("event_count") or 0)
            params[prefix + "key_events"] = int(r.get("key_events") or 0)
            params[prefix + "active_users"] = int(r.get("active_users") or 0)
            params[prefix + "average_session_duration_sec"] = r.get("average_session_duration_sec")
            params[prefix + "engagement_rate"] = r.get("engagement_rate")
            params[prefix + "bounce_rate"] = r.get("bounce_rate")
            values_parts.append(
                f"(%(r{i}_project)s, %(r{i}_acquisition_date)s::DATE, %(r{i}_dimension_type)s, %(r{i}_dimension_value)s, "
                f"%(r{i}_sessions)s, %(r{i}_engaged_sessions)s, %(r{i}_total_revenue)s, %(r{i}_event_count)s, %(r{i}_key_events)s, %(r{i}_active_users)s, "
                f"%(r{i}_average_session_duration_sec)s, %(r{i}_engagement_rate)s, %(r{i}_bounce_rate)s)"
            )
        values_sql = ",\n                ".join(values_parts)
        merge_sql = f"""
            MERGE INTO {tbl} AS target
            USING (SELECT * FROM (VALUES {values_sql}) AS v(project, acquisition_date, dimension_type, dimension_value, sessions, engaged_sessions, total_revenue, event_count, key_events, active_users, average_session_duration_sec, engagement_rate, bounce_rate)) AS source
            ON target.project = source.project AND target.acquisition_date = source.acquisition_date AND target.dimension_type = source.dimension_type AND target.dimension_value = source.dimension_value
            WHEN MATCHED THEN UPDATE SET sessions = source.sessions, engaged_sessions = source.engaged_sessions, total_revenue = source.total_revenue, event_count = source.event_count, key_events = source.key_events, active_users = source.active_users, average_session_duration_sec = source.average_session_duration_sec, engagement_rate = source.engagement_rate, bounce_rate = source.bounce_rate
            WHEN NOT MATCHED THEN INSERT (project, acquisition_date, dimension_type, dimension_value, sessions, engaged_sessions, total_revenue, event_count, key_events, active_users, average_session_duration_sec, engagement_rate, bounce_rate) VALUES (source.project, source.acquisition_date, source.dimension_type, source.dimension_value, source.sessions, source.engaged_sessions, source.total_revenue, source.event_count, source.key_events, source.active_users, source.average_session_duration_sec, source.engagement_rate, source.bounce_rate)
            """
        execute(conn, merge_sql, params)
        conn.commit()
        logger.info("ppc_flight_recorder: upserted %s ga4_traffic_acquisition rows", len(rows))

    _run_with_conn(conn, do)
    return len(rows)


def get_ga4_acquisition_for_date(project: str, acquisition_date: date, conn: Optional[Any] = None) -> List[Dict[str, Any]]:
    """Load GA4 acquisition rows for a given project/date (for GA4 diff computation)."""
    tbl = _table("ppc_ga4_acquisition_daily")

    def do(conn):
        q = f"SELECT project, acquisition_date, report_type, dimension_type, dimension_value, sessions, engaged_sessions, total_revenue, event_count, key_events, active_users, average_session_duration_sec, engagement_rate, bounce_rate FROM {tbl} WHERE project = %(project)s AND acquisition_date = %(acquisition_date)s"
        return execute_query(conn, q, {"project": project, "acquisition_date": acquisition_date.isoformat()})

    df = _run_with_conn(conn, do)
    if df.empty:
        return []
    df.columns = [c.lower() for c in df.columns]
    return df.to_dict("records")


def upsert_ga4_acquisition_daily(rows: List[Dict[str, Any]], conn: Optional[Any] = None) -> int:
    """Upsert into ppc_ga4_acquisition_daily (all report types: traffic_acquisition, user_acquisition, acquisition_overview). Batch MERGE."""
    if not rows:
        return 0
    tbl = _table("ppc_ga4_acquisition_daily")

    def do(conn):
        values_parts = []
        params = {}
        for i, r in enumerate(rows):
            prefix = f"r{i}_"
            params[prefix + "project"] = r.get("project")
            params[prefix + "acquisition_date"] = r.get("acquisition_date")
            params[prefix + "report_type"] = r.get("report_type", "traffic_acquisition")
            params[prefix + "dimension_type"] = r.get("dimension_type")
            params[prefix + "dimension_value"] = _safe_str(r.get("dimension_value"), 512)
            params[prefix + "sessions"] = int(r.get("sessions") or 0)
            params[prefix + "engaged_sessions"] = int(r.get("engaged_sessions") or 0)
            params[prefix + "total_revenue"] = float(r.get("total_revenue") or 0)
            params[prefix + "event_count"] = int(r.get("event_count") or 0)
            params[prefix + "key_events"] = int(r.get("key_events") or 0)
            params[prefix + "active_users"] = int(r.get("active_users") or 0)
            params[prefix + "average_session_duration_sec"] = r.get("average_session_duration_sec")
            params[prefix + "engagement_rate"] = r.get("engagement_rate")
            params[prefix + "bounce_rate"] = r.get("bounce_rate")
            values_parts.append(
                f"(%(r{i}_project)s, %(r{i}_acquisition_date)s::DATE, %(r{i}_report_type)s, %(r{i}_dimension_type)s, %(r{i}_dimension_value)s, "
                f"%(r{i}_sessions)s, %(r{i}_engaged_sessions)s, %(r{i}_total_revenue)s, %(r{i}_event_count)s, %(r{i}_key_events)s, %(r{i}_active_users)s, "
                f"%(r{i}_average_session_duration_sec)s, %(r{i}_engagement_rate)s, %(r{i}_bounce_rate)s)"
            )
        values_sql = ",\n                ".join(values_parts)
        merge_sql = f"""
            MERGE INTO {tbl} AS target
            USING (SELECT * FROM (VALUES {values_sql}) AS v(project, acquisition_date, report_type, dimension_type, dimension_value, sessions, engaged_sessions, total_revenue, event_count, key_events, active_users, average_session_duration_sec, engagement_rate, bounce_rate)) AS source
            ON target.project = source.project AND target.acquisition_date = source.acquisition_date AND target.report_type = source.report_type AND target.dimension_type = source.dimension_type AND target.dimension_value = source.dimension_value
            WHEN MATCHED THEN UPDATE SET sessions = source.sessions, engaged_sessions = source.engaged_sessions, total_revenue = source.total_revenue, event_count = source.event_count, key_events = source.key_events, active_users = source.active_users, average_session_duration_sec = source.average_session_duration_sec, engagement_rate = source.engagement_rate, bounce_rate = source.bounce_rate
            WHEN NOT MATCHED THEN INSERT (project, acquisition_date, report_type, dimension_type, dimension_value, sessions, engaged_sessions, total_revenue, event_count, key_events, active_users, average_session_duration_sec, engagement_rate, bounce_rate) VALUES (source.project, source.acquisition_date, source.report_type, source.dimension_type, source.dimension_value, source.sessions, source.engaged_sessions, source.total_revenue, source.event_count, source.key_events, source.active_users, source.average_session_duration_sec, source.engagement_rate, source.bounce_rate)
            """
        execute(conn, merge_sql, params)
        conn.commit()
        logger.info("ppc_flight_recorder: upserted %s ga4_acquisition rows", len(rows))

    _run_with_conn(conn, do)
    return len(rows)


def insert_ga4_acquisition_diff_daily(acquisition_date: date, project: str, diff_rows: List[Dict[str, Any]], conn: Optional[Any] = None) -> int:
    if not diff_rows:
        return 0
    tbl = _table("ppc_ga4_acquisition_diff_daily")
    date_str = acquisition_date.isoformat()

    def do(conn):
        execute(conn, f"DELETE FROM {tbl} WHERE acquisition_date = %(acquisition_date)s::DATE AND project = %(project)s", {"acquisition_date": date_str, "project": project})
        insert_sql = f"INSERT INTO {tbl} (project, acquisition_date, report_type, dimension_type, dimension_value, changed_metric_name, old_value, new_value) VALUES (%(project)s, %(acquisition_date)s::DATE, %(report_type)s, %(dimension_type)s, %(dimension_value)s, %(changed_metric_name)s, %(old_value)s, %(new_value)s)"
        params_list = [{"project": project, "acquisition_date": date_str, "report_type": r["report_type"], "dimension_type": r["dimension_type"], "dimension_value": _safe_str(r.get("dimension_value"), 512), "changed_metric_name": _safe_str(r["changed_metric_name"], 128), "old_value": _safe_str(r.get("old_value"), 65535), "new_value": _safe_str(r.get("new_value"), 65535)} for r in diff_rows]
        execute_many(conn, insert_sql, params_list)
        conn.commit()
        logger.info("ppc_flight_recorder: inserted %s ga4_acquisition diff rows for %s @ %s", len(diff_rows), project, date_str)

    _run_with_conn(conn, do)
    return len(diff_rows)


def upsert_ad_group_outcomes_daily(outcome_date: date, customer_id: str, rows: List[Dict[str, Any]], conn: Optional[Any] = None) -> int:
    if not rows:
        return 0
    date_str = outcome_date.isoformat()
    tbl = _table("ppc_ad_group_outcomes_daily")

    def do(conn):
        values_parts = []
        params = {}
        for i, r in enumerate(rows):
            cost_micros = r.get("cost_micros")
            if cost_micros is None and r.get("cost") is not None:
                cost_micros = int(float(r["cost"]) * 1_000_000)
            cost_micros = cost_micros or 0
            prefix = f"r{i}_"
            params[prefix + "ad_group_id"] = r.get("ad_group_id") or r.get("adGroupId")
            params[prefix + "campaign_id"] = r.get("campaign_id") or r.get("campaignId")
            params[prefix + "outcome_date"] = date_str
            params[prefix + "customer_id"] = customer_id
            params[prefix + "ad_group_name"] = _safe_str(r.get("ad_group_name") or r.get("adGroupName"), 512)
            params[prefix + "impressions"] = int(r.get("impressions") or 0)
            params[prefix + "clicks"] = int(r.get("clicks") or 0)
            params[prefix + "cost_micros"] = cost_micros
            params[prefix + "cost_amount"] = float(r.get("cost") or r.get("cost_amount") or 0)
            params[prefix + "conversions"] = float(r.get("conversions") or 0)
            params[prefix + "conversions_value"] = float(r.get("conversionValue") or r.get("conversions_value") or 0)
            params[prefix + "ctr"] = r.get("ctr")
            params[prefix + "cpc"] = r.get("cpc")
            params[prefix + "cpa"] = r.get("cpa")
            params[prefix + "roas"] = r.get("roas")
            params[prefix + "cvr"] = r.get("cvr")
            params[prefix + "search_impression_share_pct"] = r.get("impressionSharePct") or r.get("search_impression_share_pct")
            params[prefix + "search_rank_lost_impression_share_pct"] = r.get("search_rank_lost_impression_share_pct")
            values_parts.append(
                f"(%(r{i}_ad_group_id)s, %(r{i}_campaign_id)s, %(r{i}_outcome_date)s::DATE, %(r{i}_customer_id)s, %(r{i}_ad_group_name)s, %(r{i}_impressions)s, %(r{i}_clicks)s, %(r{i}_cost_micros)s, %(r{i}_cost_amount)s, "
                f"%(r{i}_conversions)s, %(r{i}_conversions_value)s, %(r{i}_ctr)s, %(r{i}_cpc)s, %(r{i}_cpa)s, %(r{i}_roas)s, %(r{i}_cvr)s, "
                f"%(r{i}_search_impression_share_pct)s, %(r{i}_search_rank_lost_impression_share_pct)s)"
            )
        values_sql = ",\n                ".join(values_parts)
        merge_sql = f"""
            MERGE INTO {tbl} AS target
            USING (SELECT * FROM (VALUES {values_sql}) AS v(ad_group_id, campaign_id, outcome_date, customer_id, ad_group_name, impressions, clicks, cost_micros, cost_amount, conversions, conversions_value, ctr, cpc, cpa, roas, cvr, search_impression_share_pct, search_rank_lost_impression_share_pct)) AS source
            ON target.ad_group_id = source.ad_group_id AND target.outcome_date = source.outcome_date AND target.customer_id = source.customer_id
            WHEN MATCHED THEN UPDATE SET campaign_id = source.campaign_id, ad_group_name = source.ad_group_name, impressions = source.impressions, clicks = source.clicks, cost_micros = source.cost_micros, cost_amount = source.cost_amount, conversions = source.conversions, conversions_value = source.conversions_value, ctr = source.ctr, cpc = source.cpc, cpa = source.cpa, roas = source.roas, cvr = source.cvr, search_impression_share_pct = source.search_impression_share_pct, search_rank_lost_impression_share_pct = source.search_rank_lost_impression_share_pct
            WHEN NOT MATCHED THEN INSERT (ad_group_id, campaign_id, outcome_date, customer_id, ad_group_name, impressions, clicks, cost_micros, cost_amount, conversions, conversions_value, ctr, cpc, cpa, roas, cvr, search_impression_share_pct, search_rank_lost_impression_share_pct) VALUES (source.ad_group_id, source.campaign_id, source.outcome_date, source.customer_id, source.ad_group_name, source.impressions, source.clicks, source.cost_micros, source.cost_amount, source.conversions, source.conversions_value, source.ctr, source.cpc, source.cpa, source.roas, source.cvr, source.search_impression_share_pct, source.search_rank_lost_impression_share_pct)
        """
        execute(conn, merge_sql, params)
        conn.commit()
        logger.info("ppc_flight_recorder: upserted %s ad_group_outcomes for customer_id=%s @ %s", len(rows), customer_id, date_str)

    _run_with_conn(conn, do)
    return len(rows)


def upsert_ad_group_outcomes_batch(rows: List[Dict[str, Any]], conn: Optional[Any] = None) -> int:
    """Batch upsert ad group outcomes for multiple dates. Rows must have 'outcome_date' (YYYY-MM-DD string) and 'customer_id'."""
    if not rows:
        return 0
    tbl = _table("ppc_ad_group_outcomes_daily")

    def do(conn):
        values_parts = []
        params = {}
        for i, r in enumerate(rows):
            cost_micros = r.get("cost_micros")
            if cost_micros is None and r.get("cost") is not None:
                cost_micros = int(float(r["cost"]) * 1_000_000)
            cost_micros = cost_micros or 0
            prefix = f"r{i}_"
            outcome_date_str = r.get("outcome_date")
            if not outcome_date_str:
                continue
            params[prefix + "ad_group_id"] = r.get("ad_group_id") or r.get("adGroupId")
            params[prefix + "campaign_id"] = r.get("campaign_id") or r.get("campaignId")
            params[prefix + "outcome_date"] = outcome_date_str
            params[prefix + "customer_id"] = r.get("customer_id")
            params[prefix + "ad_group_name"] = _safe_str(r.get("ad_group_name") or r.get("adGroupName"), 512)
            params[prefix + "impressions"] = int(r.get("impressions") or 0)
            params[prefix + "clicks"] = int(r.get("clicks") or 0)
            params[prefix + "cost_micros"] = cost_micros
            params[prefix + "cost_amount"] = float(r.get("cost") or r.get("cost_amount") or 0)
            params[prefix + "conversions"] = float(r.get("conversions") or 0)
            params[prefix + "conversions_value"] = float(r.get("conversionValue") or r.get("conversions_value") or 0)
            params[prefix + "ctr"] = r.get("ctr")
            params[prefix + "cpc"] = r.get("cpc")
            params[prefix + "cpa"] = r.get("cpa")
            params[prefix + "roas"] = r.get("roas")
            params[prefix + "cvr"] = r.get("cvr")
            params[prefix + "search_impression_share_pct"] = r.get("impressionSharePct") or r.get("search_impression_share_pct")
            params[prefix + "search_rank_lost_impression_share_pct"] = r.get("search_rank_lost_impression_share_pct")
            values_parts.append(
                f"(%(r{i}_ad_group_id)s, %(r{i}_campaign_id)s, %(r{i}_outcome_date)s::DATE, %(r{i}_customer_id)s, %(r{i}_ad_group_name)s, %(r{i}_impressions)s, %(r{i}_clicks)s, %(r{i}_cost_micros)s, %(r{i}_cost_amount)s, "
                f"%(r{i}_conversions)s, %(r{i}_conversions_value)s, %(r{i}_ctr)s, %(r{i}_cpc)s, %(r{i}_cpa)s, %(r{i}_roas)s, %(r{i}_cvr)s, "
                f"%(r{i}_search_impression_share_pct)s, %(r{i}_search_rank_lost_impression_share_pct)s)"
            )
        if not values_parts:
            return 0
        values_sql = ",\n                ".join(values_parts)
        merge_sql = f"""
            MERGE INTO {tbl} AS target
            USING (SELECT * FROM (VALUES {values_sql}) AS v(ad_group_id, campaign_id, outcome_date, customer_id, ad_group_name, impressions, clicks, cost_micros, cost_amount, conversions, conversions_value, ctr, cpc, cpa, roas, cvr, search_impression_share_pct, search_rank_lost_impression_share_pct)) AS source
            ON target.ad_group_id = source.ad_group_id AND target.outcome_date = source.outcome_date AND target.customer_id = source.customer_id
            WHEN MATCHED THEN UPDATE SET campaign_id = source.campaign_id, ad_group_name = source.ad_group_name, impressions = source.impressions, clicks = source.clicks, cost_micros = source.cost_micros, cost_amount = source.cost_amount, conversions = source.conversions, conversions_value = source.conversions_value, ctr = source.ctr, cpc = source.cpc, cpa = source.cpa, roas = source.roas, cvr = source.cvr, search_impression_share_pct = source.search_impression_share_pct, search_rank_lost_impression_share_pct = source.search_rank_lost_impression_share_pct
            WHEN NOT MATCHED THEN INSERT (ad_group_id, campaign_id, outcome_date, customer_id, ad_group_name, impressions, clicks, cost_micros, cost_amount, conversions, conversions_value, ctr, cpc, cpa, roas, cvr, search_impression_share_pct, search_rank_lost_impression_share_pct) VALUES (source.ad_group_id, source.campaign_id, source.outcome_date, source.customer_id, source.ad_group_name, source.impressions, source.clicks, source.cost_micros, source.cost_amount, source.conversions, source.conversions_value, source.ctr, source.cpc, source.cpa, source.roas, source.cvr, source.search_impression_share_pct, source.search_rank_lost_impression_share_pct)
            """
        execute(conn, merge_sql, params)
        conn.commit()
        logger.info("ppc_flight_recorder: batch upserted %s ad_group_outcomes", len(rows))

    _run_with_conn(conn, do)
    return len(rows)


def get_ad_group_outcomes_for_date(customer_id: str, outcome_date: date, conn: Optional[Any] = None) -> List[Dict[str, Any]]:
    tbl = _table("ppc_ad_group_outcomes_daily")

    def do(conn):
        q = f"SELECT ad_group_id, campaign_id, outcome_date, customer_id, ad_group_name, impressions, clicks, cost_micros, cost_amount, conversions, conversions_value, ctr, cpc, cpa, roas, cvr, search_impression_share_pct, search_rank_lost_impression_share_pct FROM {tbl} WHERE customer_id = %(customer_id)s AND outcome_date = %(outcome_date)s"
        return execute_query(conn, q, {"customer_id": customer_id, "outcome_date": outcome_date.isoformat()})

    df = _run_with_conn(conn, do)
    if df.empty:
        return []
    df.columns = [c.lower() for c in df.columns]
    return df.to_dict("records")


def insert_ad_group_outcomes_diff_daily(outcome_date: date, customer_id: str, diff_rows: List[Dict[str, Any]], conn: Optional[Any] = None) -> int:
    if not diff_rows:
        return 0
    tbl = _table("ppc_ad_group_outcomes_diff_daily")
    date_str = outcome_date.isoformat()

    def do(conn):
        execute(conn, f"DELETE FROM {tbl} WHERE outcome_date = %(outcome_date)s::DATE AND customer_id = %(customer_id)s", {"outcome_date": date_str, "customer_id": customer_id})
        insert_sql = f"INSERT INTO {tbl} (ad_group_id, campaign_id, outcome_date, customer_id, changed_metric_name, old_value, new_value) VALUES (%(ad_group_id)s, %(campaign_id)s, %(outcome_date)s::DATE, %(customer_id)s, %(changed_metric_name)s, %(old_value)s, %(new_value)s)"
        params_list = [{"ad_group_id": r["ad_group_id"], "campaign_id": r["campaign_id"], "outcome_date": date_str, "customer_id": customer_id, "changed_metric_name": _safe_str(r["changed_metric_name"], 128), "old_value": _safe_str(r.get("old_value"), 65535), "new_value": _safe_str(r.get("new_value"), 65535)} for r in diff_rows]
        execute_many(conn, insert_sql, params_list)
        conn.commit()
        logger.info("ppc_flight_recorder: inserted %s ad_group outcome diff rows for customer_id=%s @ %s", len(diff_rows), customer_id, date_str)

    _run_with_conn(conn, do)
    return len(diff_rows)


# ---- TIER 2: Ad group structure snapshot / change ----

def upsert_ad_group_snapshot_daily(
    snapshot_date: date,
    customer_id: str,
    rows: List[Dict[str, Any]],
    conn: Optional[Any] = None,
) -> int:
    if not rows:
        return 0
    tbl = _table("ppc_ad_group_snapshot_daily")
    date_str = snapshot_date.isoformat()

    def do(conn):
        values_parts = []
        params = {}
        for i, r in enumerate(rows):
            prefix = f"r{i}_"
            params[prefix + "ad_group_id"] = r.get("ad_group_id")
            params[prefix + "campaign_id"] = r.get("campaign_id")
            params[prefix + "snapshot_date"] = date_str
            params[prefix + "customer_id"] = customer_id
            params[prefix + "ad_group_name"] = _safe_str(r.get("ad_group_name"), 512)
            params[prefix + "status"] = _safe_str(r.get("status"), 32)
            values_parts.append(
                f"(%(r{i}_ad_group_id)s, %(r{i}_campaign_id)s, %(r{i}_snapshot_date)s::DATE, %(r{i}_customer_id)s, %(r{i}_ad_group_name)s, %(r{i}_status)s)"
            )
        values_sql = ",\n                ".join(values_parts)
        merge_sql = f"""
            MERGE INTO {tbl} AS target
            USING (SELECT * FROM (VALUES {values_sql}) AS v(ad_group_id, campaign_id, snapshot_date, customer_id, ad_group_name, status)) AS source
            ON target.ad_group_id = source.ad_group_id AND target.snapshot_date = source.snapshot_date AND target.customer_id = source.customer_id
            WHEN MATCHED THEN UPDATE SET campaign_id = source.campaign_id, ad_group_name = source.ad_group_name, status = source.status
            WHEN NOT MATCHED THEN INSERT (ad_group_id, campaign_id, snapshot_date, customer_id, ad_group_name, status)
            VALUES (source.ad_group_id, source.campaign_id, source.snapshot_date, source.customer_id, source.ad_group_name, source.status)
        """
        execute(conn, merge_sql, params)
        conn.commit()
        logger.info("ppc_flight_recorder: upserted %s ad_group_snapshot rows for customer_id=%s @ %s", len(rows), customer_id, date_str)

    _run_with_conn(conn, do)
    return len(rows)


def get_ad_group_snapshot_for_date(customer_id: str, snapshot_date: date, conn: Optional[Any] = None) -> List[Dict[str, Any]]:
    tbl = _table("ppc_ad_group_snapshot_daily")

    def do(conn):
        q = f"SELECT ad_group_id, campaign_id, ad_group_name, status FROM {tbl} WHERE customer_id = %(customer_id)s AND snapshot_date = %(snapshot_date)s"
        return execute_query(conn, q, {"customer_id": customer_id, "snapshot_date": snapshot_date.isoformat()})

    df = _run_with_conn(conn, do)
    if df.empty:
        return []
    df.columns = [c.lower() for c in df.columns]
    return df.to_dict("records")


def insert_ad_group_change_daily(snapshot_date: date, customer_id: str, rows: List[Dict[str, Any]], conn: Optional[Any] = None) -> int:
    if not rows:
        return 0
    tbl = _table("ppc_ad_group_change_daily")
    date_str = snapshot_date.isoformat()

    def do(conn):
        execute(conn, f"DELETE FROM {tbl} WHERE snapshot_date = %(snapshot_date)s::DATE AND customer_id = %(customer_id)s", {"snapshot_date": date_str, "customer_id": customer_id})
        insert_sql = f"INSERT INTO {tbl} (snapshot_date, customer_id, campaign_id, ad_group_id, change_type, ad_group_name, status, old_value, new_value) VALUES (%(snapshot_date)s::DATE, %(customer_id)s, %(campaign_id)s, %(ad_group_id)s, %(change_type)s, %(ad_group_name)s, %(status)s, %(old_value)s, %(new_value)s)"
        params_list = [
            {
                "snapshot_date": date_str,
                "customer_id": customer_id,
                "campaign_id": r["campaign_id"],
                "ad_group_id": r["ad_group_id"],
                "change_type": _safe_str(r.get("change_type"), 32),
                "ad_group_name": _safe_str(r.get("ad_group_name"), 512),
                "status": _safe_str(r.get("status"), 32),
                "old_value": _safe_str(r.get("old_value"), 65535),
                "new_value": _safe_str(r.get("new_value"), 65535),
            }
            for r in rows
        ]
        execute_many(conn, insert_sql, params_list)
        conn.commit()
        logger.info("ppc_flight_recorder: inserted %s ad_group_change rows for customer_id=%s @ %s", len(rows), customer_id, date_str)

    _run_with_conn(conn, do)
    return len(rows)


# ---- Ad group device modifier (device targeting) snapshot / diff ----

def upsert_ad_group_device_modifier_daily(
    snapshot_date: date,
    customer_id: str,
    rows: List[Dict[str, Any]],
    conn: Optional[Any] = None,
) -> int:
    """Replace all device modifier rows for (snapshot_date, customer_id) then insert the given rows."""
    tbl = _table("ppc_ad_group_device_modifier_daily")
    date_str = snapshot_date.isoformat()

    def do(conn):
        execute(conn, f"DELETE FROM {tbl} WHERE snapshot_date = %(snapshot_date)s::DATE AND customer_id = %(customer_id)s", {"snapshot_date": date_str, "customer_id": customer_id})
        if not rows:
            conn.commit()
            return
        insert_sql = (
            f"INSERT INTO {tbl} (snapshot_date, customer_id, campaign_id, ad_group_id, device_type, bid_modifier) "
            "VALUES (%(snapshot_date)s::DATE, %(customer_id)s, %(campaign_id)s, %(ad_group_id)s, %(device_type)s, %(bid_modifier)s)"
        )
        params_list = [
            {
                "snapshot_date": date_str,
                "customer_id": customer_id,
                "campaign_id": r.get("campaign_id"),
                "ad_group_id": r.get("ad_group_id"),
                "device_type": _safe_str(r.get("device_type"), 32),
                "bid_modifier": r.get("bid_modifier"),
            }
            for r in rows
        ]
        execute_many(conn, insert_sql, params_list)
        conn.commit()
        logger.info("ppc_flight_recorder: upserted %s ad_group_device_modifier rows for customer_id=%s @ %s", len(rows), customer_id, date_str)

    _run_with_conn(conn, do)
    return len(rows)


def get_ad_group_device_modifier_for_date(customer_id: str, snapshot_date: date, conn: Optional[Any] = None) -> List[Dict[str, Any]]:
    """Return device modifier rows for (customer_id, snapshot_date) from ppc_ad_group_device_modifier_daily."""
    tbl = _table("ppc_ad_group_device_modifier_daily")

    def do(conn):
        q = f"SELECT snapshot_date, customer_id, campaign_id, ad_group_id, device_type, bid_modifier FROM {tbl} WHERE customer_id = %(customer_id)s AND snapshot_date = %(snapshot_date)s"
        return execute_query(conn, q, {"customer_id": customer_id, "snapshot_date": snapshot_date.isoformat()})

    df = _run_with_conn(conn, do)
    if df.empty:
        return []
    df.columns = [c.lower() for c in df.columns]
    return df.to_dict("records")


def insert_ad_group_device_modifier_diff_daily(snapshot_date: date, customer_id: str, diff_rows: List[Dict[str, Any]], conn: Optional[Any] = None) -> int:
    """Insert day-over-day device modifier changes. Replaces all rows for (snapshot_date, customer_id)."""
    if not diff_rows:
        return 0
    tbl = _table("ppc_ad_group_device_modifier_diff_daily")
    date_str = snapshot_date.isoformat()

    def do(conn):
        execute(conn, f"DELETE FROM {tbl} WHERE snapshot_date = %(snapshot_date)s::DATE AND customer_id = %(customer_id)s", {"snapshot_date": date_str, "customer_id": customer_id})
        insert_sql = (
            f"INSERT INTO {tbl} (snapshot_date, customer_id, campaign_id, ad_group_id, device_type, changed_metric_name, old_value, new_value) "
            "VALUES (%(snapshot_date)s::DATE, %(customer_id)s, %(campaign_id)s, %(ad_group_id)s, %(device_type)s, %(changed_metric_name)s, %(old_value)s, %(new_value)s)"
        )
        params_list = [
            {
                "snapshot_date": date_str,
                "customer_id": customer_id,
                "campaign_id": r["campaign_id"],
                "ad_group_id": r["ad_group_id"],
                "device_type": _safe_str(r.get("device_type"), 32),
                "changed_metric_name": _safe_str(r["changed_metric_name"], 128),
                "old_value": _safe_str(r.get("old_value"), 65535),
                "new_value": _safe_str(r.get("new_value"), 65535),
            }
            for r in diff_rows
        ]
        execute_many(conn, insert_sql, params_list)
        conn.commit()
        logger.info("ppc_flight_recorder: inserted %s ad_group_device_modifier diff rows for customer_id=%s @ %s", len(diff_rows), customer_id, date_str)

    _run_with_conn(conn, do)
    return len(diff_rows)


# ---- Change event (change history / actions taken) ----

def upsert_change_events_daily(
    change_date: date,
    customer_id: str,
    rows: List[Dict[str, Any]],
    conn: Optional[Any] = None,
) -> int:
    """Replace all change event rows for (change_date, customer_id) then insert the given rows."""
    tbl = _table("ppc_change_event_daily")
    date_str = change_date.isoformat()

    def do(conn):
        execute(conn, f"DELETE FROM {tbl} WHERE change_date = %(change_date)s::DATE AND customer_id = %(customer_id)s", {"change_date": date_str, "customer_id": customer_id})
        if not rows:
            conn.commit()
            return
        insert_sql = (
            f"INSERT INTO {tbl} (change_date, customer_id, change_event_resource_name, change_date_time, change_resource_type, change_resource_name, resource_change_operation, changed_fields, user_email, client_type, old_value, new_value) "
            "VALUES (%(change_date)s::DATE, %(customer_id)s, %(change_event_resource_name)s, %(change_date_time)s, %(change_resource_type)s, %(change_resource_name)s, %(resource_change_operation)s, %(changed_fields)s, %(user_email)s, %(client_type)s, %(old_value)s, %(new_value)s)"
        )
        params_list = [
            {
                "change_date": date_str,
                "customer_id": customer_id,
                "change_event_resource_name": _safe_str(r.get("change_event_resource_name"), 512),
                "change_date_time": _safe_str(r.get("change_date_time"), 48),
                "change_resource_type": _safe_str(r.get("change_resource_type"), 64),
                "change_resource_name": _safe_str(r.get("change_resource_name"), 512),
                "resource_change_operation": _safe_str(r.get("resource_change_operation"), 32),
                "changed_fields": _safe_str(r.get("changed_fields"), 4096),
                "user_email": _safe_str(r.get("user_email"), 256),
                "client_type": _safe_str(r.get("client_type"), 64),
                "old_value": _safe_str(r.get("old_value"), 65535),
                "new_value": _safe_str(r.get("new_value"), 65535),
            }
            for r in rows
        ]
        execute_many(conn, insert_sql, params_list)
        conn.commit()
        logger.info("ppc_flight_recorder: upserted %s change_event rows for customer_id=%s @ %s", len(rows), customer_id, date_str)

    _run_with_conn(conn, do)
    return len(rows)


def get_change_events_for_date(customer_id: str, change_date: date, conn: Optional[Any] = None) -> List[Dict[str, Any]]:
    """Return change event rows for (customer_id, change_date) from ppc_change_event_daily."""
    tbl = _table("ppc_change_event_daily")

    def do(conn):
        q = f"SELECT change_date, customer_id, change_event_resource_name, change_date_time, change_resource_type, change_resource_name, resource_change_operation, changed_fields, user_email, client_type, old_value, new_value FROM {tbl} WHERE customer_id = %(customer_id)s AND change_date = %(change_date)s"
        return execute_query(conn, q, {"customer_id": customer_id, "change_date": change_date.isoformat()})

    df = _run_with_conn(conn, do)
    if df.empty:
        return []
    df.columns = [c.lower() for c in df.columns]
    return df.to_dict("records")


def upsert_keyword_outcomes_daily(outcome_date: date, customer_id: str, rows: List[Dict[str, Any]], conn: Optional[Any] = None) -> int:
    if not rows:
        return 0
    date_str = outcome_date.isoformat()
    tbl = _table("ppc_keyword_outcomes_daily")

    def do(conn):
        values_parts = []
        params = {}
        for i, r in enumerate(rows):
            cost_micros = r.get("cost_micros")
            if cost_micros is None and r.get("cost") is not None:
                cost_micros = int(float(r["cost"]) * 1_000_000)
            cost_micros = cost_micros or 0
            prefix = f"r{i}_"
            params[prefix + "keyword_criterion_id"] = str(r.get("keyword_criterion_id") or r.get("keywordCriterionId") or r.get("criterion_id") or r.get("criterionId") or "")
            params[prefix + "ad_group_id"] = r.get("ad_group_id") or r.get("adGroupId")
            params[prefix + "campaign_id"] = r.get("campaign_id") or r.get("campaignId")
            params[prefix + "outcome_date"] = date_str
            params[prefix + "customer_id"] = customer_id
            params[prefix + "keyword_text"] = _safe_str(r.get("keyword_text") or r.get("keywordText"), 1024)
            params[prefix + "match_type"] = _safe_str(r.get("match_type") or r.get("matchType"), 32)
            params[prefix + "impressions"] = int(r.get("impressions") or 0)
            params[prefix + "clicks"] = int(r.get("clicks") or 0)
            params[prefix + "cost_micros"] = cost_micros
            params[prefix + "cost_amount"] = float(r.get("cost") or r.get("cost_amount") or 0)
            params[prefix + "conversions"] = float(r.get("conversions") or 0)
            params[prefix + "conversions_value"] = float(r.get("conversionValue") or r.get("conversions_value") or 0)
            params[prefix + "ctr"] = r.get("ctr")
            params[prefix + "cpc"] = r.get("cpc")
            params[prefix + "cpa"] = r.get("cpa")
            params[prefix + "roas"] = r.get("roas")
            params[prefix + "cvr"] = r.get("cvr")
            params[prefix + "search_impression_share_pct"] = r.get("impressionSharePct") or r.get("search_impression_share_pct")
            params[prefix + "search_rank_lost_impression_share_pct"] = r.get("search_rank_lost_impression_share_pct")
            values_parts.append(
                f"(%(r{i}_keyword_criterion_id)s, %(r{i}_ad_group_id)s, %(r{i}_campaign_id)s, %(r{i}_outcome_date)s::DATE, %(r{i}_customer_id)s, %(r{i}_keyword_text)s, %(r{i}_match_type)s, %(r{i}_impressions)s, %(r{i}_clicks)s, %(r{i}_cost_micros)s, %(r{i}_cost_amount)s, "
                f"%(r{i}_conversions)s, %(r{i}_conversions_value)s, %(r{i}_ctr)s, %(r{i}_cpc)s, %(r{i}_cpa)s, %(r{i}_roas)s, %(r{i}_cvr)s, "
                f"%(r{i}_search_impression_share_pct)s, %(r{i}_search_rank_lost_impression_share_pct)s)"
            )
        values_sql = ",\n                ".join(values_parts)
        merge_sql = f"""
            MERGE INTO {tbl} AS target
            USING (SELECT * FROM (VALUES {values_sql}) AS v(keyword_criterion_id, ad_group_id, campaign_id, outcome_date, customer_id, keyword_text, match_type, impressions, clicks, cost_micros, cost_amount, conversions, conversions_value, ctr, cpc, cpa, roas, cvr, search_impression_share_pct, search_rank_lost_impression_share_pct)) AS source
            ON target.keyword_criterion_id = source.keyword_criterion_id AND target.outcome_date = source.outcome_date AND target.customer_id = source.customer_id
            WHEN MATCHED THEN UPDATE SET ad_group_id = source.ad_group_id, campaign_id = source.campaign_id, keyword_text = source.keyword_text, match_type = source.match_type, impressions = source.impressions, clicks = source.clicks, cost_micros = source.cost_micros, cost_amount = source.cost_amount, conversions = source.conversions, conversions_value = source.conversions_value, ctr = source.ctr, cpc = source.cpc, cpa = source.cpa, roas = source.roas, cvr = source.cvr, search_impression_share_pct = source.search_impression_share_pct, search_rank_lost_impression_share_pct = source.search_rank_lost_impression_share_pct
            WHEN NOT MATCHED THEN INSERT (keyword_criterion_id, ad_group_id, campaign_id, outcome_date, customer_id, keyword_text, match_type, impressions, clicks, cost_micros, cost_amount, conversions, conversions_value, ctr, cpc, cpa, roas, cvr, search_impression_share_pct, search_rank_lost_impression_share_pct) VALUES (source.keyword_criterion_id, source.ad_group_id, source.campaign_id, source.outcome_date, source.customer_id, source.keyword_text, source.match_type, source.impressions, source.clicks, source.cost_micros, source.cost_amount, source.conversions, source.conversions_value, source.ctr, source.cpc, source.cpa, source.roas, source.cvr, source.search_impression_share_pct, source.search_rank_lost_impression_share_pct)
        """
        execute(conn, merge_sql, params)
        conn.commit()
        logger.info("ppc_flight_recorder: upserted %s keyword_outcomes for customer_id=%s @ %s", len(rows), customer_id, date_str)

    _run_with_conn(conn, do)
    return len(rows)


def upsert_keyword_outcomes_batch(rows: List[Dict[str, Any]], conn: Optional[Any] = None) -> int:
    """Batch upsert keyword outcomes for multiple dates. Rows must have 'outcome_date' (YYYY-MM-DD string) and 'customer_id'."""
    if not rows:
        return 0
    tbl = _table("ppc_keyword_outcomes_daily")

    def do(conn):
        values_parts = []
        params = {}
        for i, r in enumerate(rows):
            cost_micros = r.get("cost_micros")
            if cost_micros is None and r.get("cost") is not None:
                cost_micros = int(float(r["cost"]) * 1_000_000)
            cost_micros = cost_micros or 0
            prefix = f"r{i}_"
            outcome_date_str = r.get("outcome_date")
            if not outcome_date_str:
                continue
            params[prefix + "keyword_criterion_id"] = str(r.get("keyword_criterion_id") or r.get("keywordCriterionId") or r.get("criterion_id") or r.get("criterionId") or "")
            params[prefix + "ad_group_id"] = r.get("ad_group_id") or r.get("adGroupId")
            params[prefix + "campaign_id"] = r.get("campaign_id") or r.get("campaignId")
            params[prefix + "outcome_date"] = outcome_date_str
            params[prefix + "customer_id"] = r.get("customer_id")
            params[prefix + "keyword_text"] = _safe_str(r.get("keyword_text") or r.get("keywordText"), 1024)
            params[prefix + "match_type"] = _safe_str(r.get("match_type") or r.get("matchType"), 32)
            params[prefix + "impressions"] = int(r.get("impressions") or 0)
            params[prefix + "clicks"] = int(r.get("clicks") or 0)
            params[prefix + "cost_micros"] = cost_micros
            params[prefix + "cost_amount"] = float(r.get("cost") or r.get("cost_amount") or 0)
            params[prefix + "conversions"] = float(r.get("conversions") or 0)
            params[prefix + "conversions_value"] = float(r.get("conversionValue") or r.get("conversions_value") or 0)
            params[prefix + "ctr"] = r.get("ctr")
            params[prefix + "cpc"] = r.get("cpc")
            params[prefix + "cpa"] = r.get("cpa")
            params[prefix + "roas"] = r.get("roas")
            params[prefix + "cvr"] = r.get("cvr")
            params[prefix + "search_impression_share_pct"] = r.get("impressionSharePct") or r.get("search_impression_share_pct")
            params[prefix + "search_rank_lost_impression_share_pct"] = r.get("search_rank_lost_impression_share_pct")
            values_parts.append(
                f"(%(r{i}_keyword_criterion_id)s, %(r{i}_ad_group_id)s, %(r{i}_campaign_id)s, %(r{i}_outcome_date)s::DATE, %(r{i}_customer_id)s, %(r{i}_keyword_text)s, %(r{i}_match_type)s, %(r{i}_impressions)s, %(r{i}_clicks)s, %(r{i}_cost_micros)s, %(r{i}_cost_amount)s, "
                f"%(r{i}_conversions)s, %(r{i}_conversions_value)s, %(r{i}_ctr)s, %(r{i}_cpc)s, %(r{i}_cpa)s, %(r{i}_roas)s, %(r{i}_cvr)s, "
                f"%(r{i}_search_impression_share_pct)s, %(r{i}_search_rank_lost_impression_share_pct)s)"
            )
        if not values_parts:
            return 0
        values_sql = ",\n                ".join(values_parts)
        merge_sql = f"""
            MERGE INTO {tbl} AS target
            USING (SELECT * FROM (VALUES {values_sql}) AS v(keyword_criterion_id, ad_group_id, campaign_id, outcome_date, customer_id, keyword_text, match_type, impressions, clicks, cost_micros, cost_amount, conversions, conversions_value, ctr, cpc, cpa, roas, cvr, search_impression_share_pct, search_rank_lost_impression_share_pct)) AS source
            ON target.keyword_criterion_id = source.keyword_criterion_id AND target.outcome_date = source.outcome_date AND target.customer_id = source.customer_id
            WHEN MATCHED THEN UPDATE SET ad_group_id = source.ad_group_id, campaign_id = source.campaign_id, keyword_text = source.keyword_text, match_type = source.match_type, impressions = source.impressions, clicks = source.clicks, cost_micros = source.cost_micros, cost_amount = source.cost_amount, conversions = source.conversions, conversions_value = source.conversions_value, ctr = source.ctr, cpc = source.cpc, cpa = source.cpa, roas = source.roas, cvr = source.cvr, search_impression_share_pct = source.search_impression_share_pct, search_rank_lost_impression_share_pct = source.search_rank_lost_impression_share_pct
            WHEN NOT MATCHED THEN INSERT (keyword_criterion_id, ad_group_id, campaign_id, outcome_date, customer_id, keyword_text, match_type, impressions, clicks, cost_micros, cost_amount, conversions, conversions_value, ctr, cpc, cpa, roas, cvr, search_impression_share_pct, search_rank_lost_impression_share_pct) VALUES (source.keyword_criterion_id, source.ad_group_id, source.campaign_id, source.outcome_date, source.customer_id, source.keyword_text, source.match_type, source.impressions, source.clicks, source.cost_micros, source.cost_amount, source.conversions, source.conversions_value, source.ctr, source.cpc, source.cpa, source.roas, source.cvr, source.search_impression_share_pct, source.search_rank_lost_impression_share_pct)
            """
        execute(conn, merge_sql, params)
        conn.commit()
        logger.info("ppc_flight_recorder: batch upserted %s keyword_outcomes", len(rows))

    _run_with_conn(conn, do)
    return len(rows)


def get_keyword_outcomes_for_date(customer_id: str, outcome_date: date, conn: Optional[Any] = None) -> List[Dict[str, Any]]:
    tbl = _table("ppc_keyword_outcomes_daily")

    def do(conn):
        q = f"SELECT keyword_criterion_id, ad_group_id, campaign_id, outcome_date, customer_id, keyword_text, match_type, impressions, clicks, cost_micros, cost_amount, conversions, conversions_value, ctr, cpc, cpa, roas, cvr, search_impression_share_pct, search_rank_lost_impression_share_pct FROM {tbl} WHERE customer_id = %(customer_id)s AND outcome_date = %(outcome_date)s"
        return execute_query(conn, q, {"customer_id": customer_id, "outcome_date": outcome_date.isoformat()})

    df = _run_with_conn(conn, do)
    if df.empty:
        return []
    df.columns = [c.lower() for c in df.columns]
    return df.to_dict("records")


def insert_keyword_outcomes_diff_daily(outcome_date: date, customer_id: str, diff_rows: List[Dict[str, Any]], conn: Optional[Any] = None) -> int:
    if not diff_rows:
        return 0
    tbl = _table("ppc_keyword_outcomes_diff_daily")
    date_str = outcome_date.isoformat()

    def do(conn):
        execute(conn, f"DELETE FROM {tbl} WHERE outcome_date = %(outcome_date)s::DATE AND customer_id = %(customer_id)s", {"outcome_date": date_str, "customer_id": customer_id})
        insert_sql = f"INSERT INTO {tbl} (keyword_criterion_id, outcome_date, customer_id, changed_metric_name, old_value, new_value) VALUES (%(keyword_criterion_id)s, %(outcome_date)s::DATE, %(customer_id)s, %(changed_metric_name)s, %(old_value)s, %(new_value)s)"
        params_list = [{"keyword_criterion_id": r["keyword_criterion_id"], "outcome_date": date_str, "customer_id": customer_id, "changed_metric_name": _safe_str(r["changed_metric_name"], 128), "old_value": _safe_str(r.get("old_value"), 65535), "new_value": _safe_str(r.get("new_value"), 65535)} for r in diff_rows]
        execute_many(conn, insert_sql, params_list)
        conn.commit()
        logger.info("ppc_flight_recorder: inserted %s keyword outcome diff rows for customer_id=%s @ %s", len(diff_rows), customer_id, date_str)

    _run_with_conn(conn, do)
    return len(diff_rows)


def upsert_campaign_dims(reference_date: date, customer_id: str, rows: List[Dict[str, Any]], conn: Optional[Any] = None) -> int:
    """Upsert campaign dimension table (id -> name, status, channel type) for lookup when viewing outcomes."""
    if not rows:
        return 0
    tbl = _table("ppc_campaign_dims")
    ref_str = reference_date.isoformat()

    def do(conn):
        for r in rows:
            cid = r.get("campaign_id") or r.get("campaignId")
            if not cid:
                continue
            merge_sql = f"""
                MERGE INTO {tbl} AS target
                USING (SELECT %(campaign_id)s AS campaign_id, %(customer_id)s AS customer_id, %(campaign_name)s AS campaign_name, %(status)s AS status, %(advertising_channel_type)s AS advertising_channel_type, %(last_seen_date)s::DATE AS last_seen_date) AS source
                ON target.campaign_id = source.campaign_id AND target.customer_id = source.customer_id
                WHEN MATCHED THEN UPDATE SET campaign_name = COALESCE(source.campaign_name, target.campaign_name), status = COALESCE(source.status, target.status), advertising_channel_type = COALESCE(source.advertising_channel_type, target.advertising_channel_type), last_seen_date = source.last_seen_date, updated_at = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT (campaign_id, customer_id, campaign_name, status, advertising_channel_type, last_seen_date) VALUES (source.campaign_id, source.customer_id, source.campaign_name, source.status, source.advertising_channel_type, source.last_seen_date)
            """
            execute(conn, merge_sql, {
                "campaign_id": cid,
                "customer_id": customer_id,
                "campaign_name": _safe_str(r.get("campaign_name") or r.get("campaignName"), 512),
                "status": _safe_str(r.get("status"), 32),
                "advertising_channel_type": _safe_str(r.get("advertising_channel_type"), 64),
                "last_seen_date": ref_str,
            })
        conn.commit()
        logger.info("ppc_flight_recorder: upserted %s campaign_dims for customer_id=%s", len(rows), customer_id)

    _run_with_conn(conn, do)
    return len(rows)


def upsert_ad_group_dims(reference_date: date, customer_id: str, rows: List[Dict[str, Any]], conn: Optional[Any] = None) -> int:
    """Upsert ad group dimension table (id -> name, campaign_id) for lookup when viewing outcomes."""
    if not rows:
        return 0
    tbl = _table("ppc_ad_group_dims")
    ref_str = reference_date.isoformat()

    def do(conn):
        seen = set()
        for r in rows:
            agid = r.get("ad_group_id") or r.get("adGroupId")
            if not agid or (agid, customer_id) in seen:
                continue
            seen.add((agid, customer_id))
            merge_sql = f"""
                MERGE INTO {tbl} AS target
                USING (SELECT %(ad_group_id)s AS ad_group_id, %(campaign_id)s AS campaign_id, %(customer_id)s AS customer_id, %(ad_group_name)s AS ad_group_name, %(last_seen_date)s::DATE AS last_seen_date) AS source
                ON target.ad_group_id = source.ad_group_id AND target.customer_id = source.customer_id
                WHEN MATCHED THEN UPDATE SET campaign_id = source.campaign_id, ad_group_name = source.ad_group_name, last_seen_date = source.last_seen_date, updated_at = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT (ad_group_id, campaign_id, customer_id, ad_group_name, last_seen_date) VALUES (source.ad_group_id, source.campaign_id, source.customer_id, source.ad_group_name, source.last_seen_date)
            """
            execute(conn, merge_sql, {
                "ad_group_id": agid,
                "campaign_id": r.get("campaign_id") or r.get("campaignId"),
                "customer_id": customer_id,
                "ad_group_name": _safe_str(r.get("ad_group_name") or r.get("adGroupName"), 512),
                "last_seen_date": ref_str,
            })
        conn.commit()
        logger.info("ppc_flight_recorder: upserted %s ad_group_dims for customer_id=%s", len(seen), customer_id)

    _run_with_conn(conn, do)
    return len(rows)


# ---- TIER 2: Keyword snapshot / change ----

def upsert_keyword_snapshot_daily(
    snapshot_date: date,
    customer_id: str,
    rows: List[Dict[str, Any]],
    conn: Optional[Any] = None,
) -> int:
    if not rows:
        return 0
    # Deduplicate by table PK so MERGE source has at most one row per key (Snowflake errors on duplicate source rows).
    # PK is (keyword_criterion_id, snapshot_date, customer_id, ad_group_id).
    seen = set()
    deduped = []
    for r in rows:
        key = (str(r.get("keyword_criterion_id", "")), snapshot_date.isoformat(), customer_id, str(r.get("ad_group_id", "")))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)
    if len(deduped) < len(rows):
        logger.info("ppc_flight_recorder: deduplicated keyword_snapshot_daily %s -> %s rows by (keyword_criterion_id, snapshot_date, customer_id, ad_group_id)", len(rows), len(deduped))
    rows = deduped

    tbl = _table("ppc_keyword_snapshot_daily")
    date_str = snapshot_date.isoformat()

    def do(conn):
        values_parts = []
        params = {}
        for i, r in enumerate(rows):
            prefix = f"r{i}_"
            params[prefix + "keyword_criterion_id"] = str(r.get("keyword_criterion_id", ""))
            params[prefix + "ad_group_id"] = r.get("ad_group_id")
            params[prefix + "campaign_id"] = r.get("campaign_id")
            params[prefix + "snapshot_date"] = date_str
            params[prefix + "customer_id"] = customer_id
            params[prefix + "keyword_text"] = _safe_str(r.get("keyword_text"), 1024)
            params[prefix + "match_type"] = _safe_str(r.get("match_type"), 32)
            params[prefix + "keyword_level"] = _safe_str(r.get("keyword_level") or "AD_GROUP", 32)
            params[prefix + "campaign_name"] = _safe_str(r.get("campaign_name"), 512)
            params[prefix + "ad_group_name"] = _safe_str(r.get("ad_group_name"), 512)
            values_parts.append(
                f"(%(r{i}_keyword_criterion_id)s, %(r{i}_ad_group_id)s, %(r{i}_campaign_id)s, %(r{i}_snapshot_date)s::DATE, %(r{i}_customer_id)s, %(r{i}_keyword_text)s, %(r{i}_match_type)s, %(r{i}_keyword_level)s, %(r{i}_campaign_name)s, %(r{i}_ad_group_name)s)"
            )
        values_sql = ",\n                ".join(values_parts)
        merge_sql = f"""
            MERGE INTO {tbl} AS target
            USING (SELECT * FROM (VALUES {values_sql}) AS v(keyword_criterion_id, ad_group_id, campaign_id, snapshot_date, customer_id, keyword_text, match_type, keyword_level, campaign_name, ad_group_name)) AS source
            ON target.keyword_criterion_id = source.keyword_criterion_id AND target.snapshot_date = source.snapshot_date AND target.customer_id = source.customer_id AND target.ad_group_id = source.ad_group_id
            WHEN MATCHED THEN UPDATE SET campaign_id = source.campaign_id, keyword_text = source.keyword_text, match_type = source.match_type, keyword_level = source.keyword_level, campaign_name = source.campaign_name, ad_group_name = source.ad_group_name
            WHEN NOT MATCHED THEN INSERT (keyword_criterion_id, ad_group_id, campaign_id, snapshot_date, customer_id, keyword_text, match_type, keyword_level, campaign_name, ad_group_name)
            VALUES (source.keyword_criterion_id, source.ad_group_id, source.campaign_id, source.snapshot_date, source.customer_id, source.keyword_text, source.match_type, source.keyword_level, source.campaign_name, source.ad_group_name)
        """
        execute(conn, merge_sql, params)
        conn.commit()
        logger.info("ppc_flight_recorder: upserted %s keyword_snapshot rows for customer_id=%s @ %s", len(rows), customer_id, date_str)

    _run_with_conn(conn, do)
    return len(rows)


def get_keyword_snapshot_for_date(customer_id: str, snapshot_date: date, conn: Optional[Any] = None) -> List[Dict[str, Any]]:
    tbl = _table("ppc_keyword_snapshot_daily")

    def do(conn):
        q = f"SELECT keyword_criterion_id, ad_group_id, campaign_id, keyword_text, match_type, keyword_level, campaign_name, ad_group_name FROM {tbl} WHERE customer_id = %(customer_id)s AND snapshot_date = %(snapshot_date)s"
        return execute_query(conn, q, {"customer_id": customer_id, "snapshot_date": snapshot_date.isoformat()})

    df = _run_with_conn(conn, do)
    if df.empty:
        return []
    df.columns = [c.lower() for c in df.columns]
    return df.to_dict("records")


def insert_keyword_change_daily(snapshot_date: date, customer_id: str, rows: List[Dict[str, Any]], conn: Optional[Any] = None) -> int:
    if not rows:
        return 0
    tbl = _table("ppc_keyword_change_daily")
    date_str = snapshot_date.isoformat()

    def do(conn):
        execute(conn, f"DELETE FROM {tbl} WHERE snapshot_date = %(snapshot_date)s::DATE AND customer_id = %(customer_id)s", {"snapshot_date": date_str, "customer_id": customer_id})
        insert_sql = f"INSERT INTO {tbl} (snapshot_date, customer_id, campaign_id, ad_group_id, keyword_criterion_id, change_type, keyword_text, match_type, old_value, new_value) VALUES (%(snapshot_date)s::DATE, %(customer_id)s, %(campaign_id)s, %(ad_group_id)s, %(keyword_criterion_id)s, %(change_type)s, %(keyword_text)s, %(match_type)s, %(old_value)s, %(new_value)s)"
        params_list = [
            {
                "snapshot_date": date_str,
                "customer_id": customer_id,
                "campaign_id": r["campaign_id"],
                "ad_group_id": r["ad_group_id"],
                "keyword_criterion_id": r["keyword_criterion_id"],
                "change_type": _safe_str(r.get("change_type"), 32),
                "keyword_text": _safe_str(r.get("keyword_text"), 1024),
                "match_type": _safe_str(r.get("match_type"), 32),
                "old_value": _safe_str(r.get("old_value"), 65535),
                "new_value": _safe_str(r.get("new_value"), 65535),
            }
            for r in rows
        ]
        execute_many(conn, insert_sql, params_list)
        conn.commit()
        logger.info("ppc_flight_recorder: inserted %s keyword_change rows for customer_id=%s @ %s", len(rows), customer_id, date_str)

    _run_with_conn(conn, do)
    return len(rows)


# ---- TIER 2: Negative keyword snapshot / diff ----

def upsert_negative_keyword_snapshot_daily(
    snapshot_date: date,
    customer_id: str,
    rows: List[Dict[str, Any]],
    conn: Optional[Any] = None,
) -> int:
    if not rows:
        return 0
    # Deduplicate by table PK so MERGE source has at most one row per key (Snowflake errors on duplicate source rows).
    # PK is (snapshot_date, customer_id, campaign_id, ad_group_id, criterion_id).
    date_str = snapshot_date.isoformat()
    seen = set()
    deduped = []
    for r in rows:
        ad_group_id = r.get("ad_group_id") if r.get("ad_group_id") is not None else ""
        key = (date_str, customer_id, str(r.get("campaign_id", "")), ad_group_id, str(r.get("criterion_id", "")))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)
    if len(deduped) < len(rows):
        logger.info(
            "ppc_flight_recorder: deduplicated negative_keyword_snapshot_daily %s -> %s rows by (snapshot_date, customer_id, campaign_id, ad_group_id, criterion_id)",
            len(rows), len(deduped),
        )
    rows = deduped

    tbl = _table("ppc_negative_keyword_snapshot_daily")

    def do(conn):
        values_parts = []
        params = {}
        for i, r in enumerate(rows):
            prefix = f"r{i}_"
            ad_group_id = r.get("ad_group_id")
            if ad_group_id is None:
                ad_group_id = ""
            params[prefix + "snapshot_date"] = date_str
            params[prefix + "customer_id"] = customer_id
            params[prefix + "campaign_id"] = r.get("campaign_id")
            params[prefix + "ad_group_id"] = ad_group_id
            params[prefix + "criterion_id"] = str(r.get("criterion_id", ""))
            params[prefix + "keyword_text"] = _safe_str(r.get("keyword_text"), 1024)
            params[prefix + "match_type"] = _safe_str(r.get("match_type"), 32)
            params[prefix + "keyword_level"] = _safe_str(r.get("keyword_level") or ("AD_GROUP" if ad_group_id else "CAMPAIGN"), 32)
            params[prefix + "campaign_name"] = _safe_str(r.get("campaign_name"), 512)
            params[prefix + "ad_group_name"] = _safe_str(r.get("ad_group_name"), 512)
            values_parts.append(
                f"(%(r{i}_snapshot_date)s::DATE, %(r{i}_customer_id)s, %(r{i}_campaign_id)s, %(r{i}_ad_group_id)s, %(r{i}_criterion_id)s, %(r{i}_keyword_text)s, %(r{i}_match_type)s, %(r{i}_keyword_level)s, %(r{i}_campaign_name)s, %(r{i}_ad_group_name)s)"
            )
        values_sql = ",\n                ".join(values_parts)
        merge_sql = f"""
            MERGE INTO {tbl} AS target
            USING (SELECT * FROM (VALUES {values_sql}) AS v(snapshot_date, customer_id, campaign_id, ad_group_id, criterion_id, keyword_text, match_type, keyword_level, campaign_name, ad_group_name)) AS source
            ON target.snapshot_date = source.snapshot_date AND target.customer_id = source.customer_id AND target.campaign_id = source.campaign_id AND target.ad_group_id = source.ad_group_id AND target.criterion_id = source.criterion_id
            WHEN MATCHED THEN UPDATE SET keyword_text = source.keyword_text, match_type = source.match_type, keyword_level = source.keyword_level, campaign_name = source.campaign_name, ad_group_name = source.ad_group_name
            WHEN NOT MATCHED THEN INSERT (snapshot_date, customer_id, campaign_id, ad_group_id, criterion_id, keyword_text, match_type, keyword_level, campaign_name, ad_group_name)
            VALUES (source.snapshot_date, source.customer_id, source.campaign_id, source.ad_group_id, source.criterion_id, source.keyword_text, source.match_type, source.keyword_level, source.campaign_name, source.ad_group_name)
        """
        execute(conn, merge_sql, params)
        conn.commit()
        logger.info("ppc_flight_recorder: upserted %s negative_keyword_snapshot rows for customer_id=%s @ %s", len(rows), customer_id, date_str)

    _run_with_conn(conn, do)
    return len(rows)


def get_negative_keyword_snapshot_for_date(customer_id: str, snapshot_date: date, conn: Optional[Any] = None) -> List[Dict[str, Any]]:
    tbl = _table("ppc_negative_keyword_snapshot_daily")

    def do(conn):
        q = f"SELECT campaign_id, ad_group_id, criterion_id, keyword_text, match_type, keyword_level, campaign_name, ad_group_name FROM {tbl} WHERE customer_id = %(customer_id)s AND snapshot_date = %(snapshot_date)s"
        return execute_query(conn, q, {"customer_id": customer_id, "snapshot_date": snapshot_date.isoformat()})

    df = _run_with_conn(conn, do)
    if df.empty:
        return []
    df.columns = [c.lower() for c in df.columns]
    return df.to_dict("records")


def insert_negative_keyword_diff_daily(snapshot_date: date, customer_id: str, rows: List[Dict[str, Any]], conn: Optional[Any] = None) -> int:
    if not rows:
        return 0
    tbl = _table("ppc_negative_keyword_diff_daily")
    date_str = snapshot_date.isoformat()

    def do(conn):
        execute(conn, f"DELETE FROM {tbl} WHERE snapshot_date = %(snapshot_date)s::DATE AND customer_id = %(customer_id)s", {"snapshot_date": date_str, "customer_id": customer_id})
        insert_sql = f"INSERT INTO {tbl} (snapshot_date, customer_id, campaign_id, ad_group_id, criterion_id, change_type, keyword_text, match_type, old_value, new_value) VALUES (%(snapshot_date)s::DATE, %(customer_id)s, %(campaign_id)s, %(ad_group_id)s, %(criterion_id)s, %(change_type)s, %(keyword_text)s, %(match_type)s, %(old_value)s, %(new_value)s)"
        params_list = [
            {
                "snapshot_date": date_str,
                "customer_id": customer_id,
                "campaign_id": r["campaign_id"],
                "ad_group_id": r.get("ad_group_id") or "",
                "criterion_id": r["criterion_id"],
                "change_type": _safe_str(r.get("change_type"), 32),
                "keyword_text": _safe_str(r.get("keyword_text"), 1024),
                "match_type": _safe_str(r.get("match_type"), 32),
                "old_value": _safe_str(r.get("old_value"), 65535),
                "new_value": _safe_str(r.get("new_value"), 65535),
            }
            for r in rows
        ]
        execute_many(conn, insert_sql, params_list)
        conn.commit()
        logger.info("ppc_flight_recorder: inserted %s negative_keyword_diff rows for customer_id=%s @ %s", len(rows), customer_id, date_str)

    _run_with_conn(conn, do)
    return len(rows)


# ---- TIER 2: Ad creative (RSA) snapshot / diff ----

def upsert_ad_creative_snapshot_daily(
    snapshot_date: date,
    customer_id: str,
    rows: List[Dict[str, Any]],
    conn: Optional[Any] = None,
) -> int:
    if not rows:
        return 0
    tbl = _table("ppc_ad_creative_snapshot_daily")
    date_str = snapshot_date.isoformat()

    def do(conn):
        values_parts = []
        params = {}
        for i, r in enumerate(rows):
            prefix = f"r{i}_"
            params[prefix + "snapshot_date"] = date_str
            params[prefix + "customer_id"] = customer_id
            params[prefix + "ad_group_id"] = r.get("ad_group_id")
            params[prefix + "campaign_id"] = r.get("campaign_id")
            params[prefix + "ad_id"] = str(r.get("ad_id", ""))
            params[prefix + "ad_type"] = _safe_str(r.get("ad_type"), 64)
            params[prefix + "status"] = _safe_str(r.get("status"), 32)
            params[prefix + "headlines_json"] = _safe_str(r.get("headlines_json"), 65535)
            params[prefix + "descriptions_json"] = _safe_str(r.get("descriptions_json"), 65535)
            params[prefix + "final_urls"] = _safe_str(r.get("final_urls"), 65535)
            params[prefix + "path1"] = _safe_str(r.get("path1"), 512)
            params[prefix + "path2"] = _safe_str(r.get("path2"), 512)
            params[prefix + "policy_summary_json"] = _safe_str(r.get("policy_summary_json"), 65535)
            params[prefix + "asset_urls"] = _safe_str(r.get("asset_urls"), 65535)
            values_parts.append(
                f"(%(r{i}_snapshot_date)s::DATE, %(r{i}_customer_id)s, %(r{i}_ad_group_id)s, %(r{i}_campaign_id)s, %(r{i}_ad_id)s, %(r{i}_ad_type)s, %(r{i}_status)s, %(r{i}_headlines_json)s, %(r{i}_descriptions_json)s, %(r{i}_final_urls)s, %(r{i}_path1)s, %(r{i}_path2)s, %(r{i}_policy_summary_json)s, %(r{i}_asset_urls)s)"
            )
        values_sql = ",\n                ".join(values_parts)
        merge_sql = f"""
            MERGE INTO {tbl} AS target
            USING (SELECT * FROM (VALUES {values_sql}) AS v(snapshot_date, customer_id, ad_group_id, campaign_id, ad_id, ad_type, status, headlines_json, descriptions_json, final_urls, path1, path2, policy_summary_json, asset_urls)) AS source
            ON target.snapshot_date = source.snapshot_date AND target.customer_id = source.customer_id AND target.ad_group_id = source.ad_group_id AND target.ad_id = source.ad_id
            WHEN MATCHED THEN UPDATE SET campaign_id = source.campaign_id, ad_type = source.ad_type, status = source.status, headlines_json = source.headlines_json, descriptions_json = source.descriptions_json, final_urls = source.final_urls, path1 = source.path1, path2 = source.path2, policy_summary_json = source.policy_summary_json, asset_urls = source.asset_urls
            WHEN NOT MATCHED THEN INSERT (snapshot_date, customer_id, ad_group_id, campaign_id, ad_id, ad_type, status, headlines_json, descriptions_json, final_urls, path1, path2, policy_summary_json, asset_urls)
            VALUES (source.snapshot_date, source.customer_id, source.ad_group_id, source.campaign_id, source.ad_id, source.ad_type, source.status, source.headlines_json, source.descriptions_json, source.final_urls, source.path1, source.path2, source.policy_summary_json, source.asset_urls)
        """
        execute(conn, merge_sql, params)
        conn.commit()
        logger.info("ppc_flight_recorder: upserted %s ad_creative_snapshot rows for customer_id=%s @ %s", len(rows), customer_id, date_str)

    _run_with_conn(conn, do)
    return len(rows)


def get_ad_creative_snapshot_for_date(customer_id: str, snapshot_date: date, conn: Optional[Any] = None) -> List[Dict[str, Any]]:
    tbl = _table("ppc_ad_creative_snapshot_daily")

    def do(conn):
        q = f"SELECT ad_group_id, campaign_id, ad_id, ad_type, status, headlines_json, descriptions_json, final_urls, path1, path2, policy_summary_json, asset_urls FROM {tbl} WHERE customer_id = %(customer_id)s AND snapshot_date = %(snapshot_date)s"
        return execute_query(conn, q, {"customer_id": customer_id, "snapshot_date": snapshot_date.isoformat()})

    df = _run_with_conn(conn, do)
    if df.empty:
        return []
    df.columns = [c.lower() for c in df.columns]
    return df.to_dict("records")


def insert_ad_creative_diff_daily(snapshot_date: date, customer_id: str, rows: List[Dict[str, Any]], conn: Optional[Any] = None) -> int:
    if not rows:
        return 0
    tbl = _table("ppc_ad_creative_diff_daily")
    date_str = snapshot_date.isoformat()

    def do(conn):
        execute(conn, f"DELETE FROM {tbl} WHERE snapshot_date = %(snapshot_date)s::DATE AND customer_id = %(customer_id)s", {"snapshot_date": date_str, "customer_id": customer_id})
        insert_sql = f"INSERT INTO {tbl} (snapshot_date, customer_id, ad_group_id, ad_id, changed_metric_name, old_value, new_value) VALUES (%(snapshot_date)s::DATE, %(customer_id)s, %(ad_group_id)s, %(ad_id)s, %(changed_metric_name)s, %(old_value)s, %(new_value)s)"
        params_list = [
            {
                "snapshot_date": date_str,
                "customer_id": customer_id,
                "ad_group_id": r["ad_group_id"],
                "ad_id": r["ad_id"],
                "changed_metric_name": _safe_str(r["changed_metric_name"], 128),
                "old_value": _safe_str(r.get("old_value"), 65535),
                "new_value": _safe_str(r.get("new_value"), 65535),
            }
            for r in rows
        ]
        execute_many(conn, insert_sql, params_list)
        conn.commit()
        logger.info("ppc_flight_recorder: inserted %s ad_creative_diff rows for customer_id=%s @ %s", len(rows), customer_id, date_str)

    _run_with_conn(conn, do)
    return len(rows)


# ---- TIER 2: Audience targeting snapshot / diff ----

def upsert_audience_targeting_snapshot_daily(
    snapshot_date: date,
    customer_id: str,
    rows: List[Dict[str, Any]],
    conn: Optional[Any] = None,
) -> int:
    if not rows:
        return 0
    tbl = _table("ppc_audience_targeting_snapshot_daily")
    date_str = snapshot_date.isoformat()

    def do(conn):
        values_parts = []
        params = {}
        for i, r in enumerate(rows):
            prefix = f"r{i}_"
            ad_group_id = r.get("ad_group_id") or ""
            params[prefix + "snapshot_date"] = date_str
            params[prefix + "customer_id"] = customer_id
            params[prefix + "campaign_id"] = r.get("campaign_id")
            params[prefix + "ad_group_id"] = ad_group_id
            params[prefix + "criterion_id"] = str(r.get("criterion_id", ""))
            params[prefix + "audience_type"] = _safe_str(r.get("audience_type"), 64)
            params[prefix + "audience_id"] = _safe_str(r.get("audience_id"), 256)
            params[prefix + "audience_name"] = _safe_str(r.get("audience_name"), 512)
            params[prefix + "targeting_mode"] = _safe_str(r.get("targeting_mode"), 32)
            params[prefix + "audience_size"] = r.get("audience_size")
            params[prefix + "status"] = _safe_str(r.get("status"), 32)
            params[prefix + "bid_modifier"] = r.get("bid_modifier")
            params[prefix + "negative"] = r.get("negative")
            values_parts.append(
                f"(%(r{i}_snapshot_date)s::DATE, %(r{i}_customer_id)s, %(r{i}_campaign_id)s, %(r{i}_ad_group_id)s, %(r{i}_criterion_id)s, "
                f"%(r{i}_audience_type)s, %(r{i}_audience_id)s, %(r{i}_audience_name)s, %(r{i}_targeting_mode)s, %(r{i}_audience_size)s, %(r{i}_status)s, %(r{i}_bid_modifier)s, %(r{i}_negative)s)"
            )
        values_sql = ",\n                ".join(values_parts)
        merge_sql = f"""
            MERGE INTO {tbl} AS target
            USING (SELECT * FROM (VALUES {values_sql}) AS v(snapshot_date, customer_id, campaign_id, ad_group_id, criterion_id, audience_type, audience_id, audience_name, targeting_mode, audience_size, status, bid_modifier, negative)) AS source
            ON target.snapshot_date = source.snapshot_date AND target.customer_id = source.customer_id AND target.campaign_id = source.campaign_id AND target.ad_group_id = source.ad_group_id AND target.criterion_id = source.criterion_id
            WHEN MATCHED THEN UPDATE SET audience_type = source.audience_type, audience_id = source.audience_id, audience_name = source.audience_name, targeting_mode = source.targeting_mode, audience_size = source.audience_size, status = source.status, bid_modifier = source.bid_modifier, negative = source.negative
            WHEN NOT MATCHED THEN INSERT (snapshot_date, customer_id, campaign_id, ad_group_id, criterion_id, audience_type, audience_id, audience_name, targeting_mode, audience_size, status, bid_modifier, negative)
            VALUES (source.snapshot_date, source.customer_id, source.campaign_id, source.ad_group_id, source.criterion_id, source.audience_type, source.audience_id, source.audience_name, source.targeting_mode, source.audience_size, source.status, source.bid_modifier, source.negative)
        """
        execute(conn, merge_sql, params)
        conn.commit()
        logger.info("ppc_flight_recorder: upserted %s audience_targeting_snapshot rows for customer_id=%s @ %s", len(rows), customer_id, date_str)

    _run_with_conn(conn, do)
    return len(rows)


def get_audience_targeting_snapshot_for_date(customer_id: str, snapshot_date: date, conn: Optional[Any] = None) -> List[Dict[str, Any]]:
    tbl = _table("ppc_audience_targeting_snapshot_daily")
    params = {"customer_id": customer_id, "snapshot_date": snapshot_date.isoformat()}

    def do(conn):
        q = f"SELECT campaign_id, ad_group_id, criterion_id, audience_type, audience_id, audience_name, targeting_mode, audience_size, status, bid_modifier, negative FROM {tbl} WHERE customer_id = %(customer_id)s AND snapshot_date = %(snapshot_date)s"
        return execute_query(conn, q, params)

    try:
        df = _run_with_conn(conn, do)
    except Exception:
        q_fallback = f"SELECT campaign_id, ad_group_id, criterion_id, audience_type, audience_id, audience_name, targeting_mode, bid_modifier, negative FROM {tbl} WHERE customer_id = %(customer_id)s AND snapshot_date = %(snapshot_date)s"
        def do_fallback(conn):
            return execute_query(conn, q_fallback, params)
        df = _run_with_conn(conn, do_fallback)
        if not df.empty:
            if "audience_size" not in df.columns:
                df["audience_size"] = None  # type: ignore[assignment]
            if "status" not in df.columns:
                df["status"] = None  # type: ignore[assignment]
    if df.empty:
        return []
    df.columns = [c.lower() for c in df.columns]
    records = df.to_dict("records")
    for row in records:
        ag = row.get("ad_group_id")
        if ag is None or (isinstance(ag, float) and ag != ag) or str(ag).strip() == "":
            row["ad_group_id"] = ""
        else:
            row["ad_group_id"] = str(ag)
    return records


def insert_audience_targeting_diff_daily(snapshot_date: date, customer_id: str, rows: List[Dict[str, Any]], conn: Optional[Any] = None) -> int:
    if not rows:
        return 0
    tbl = _table("ppc_audience_targeting_diff_daily")
    date_str = snapshot_date.isoformat()

    def do(conn):
        execute(conn, f"DELETE FROM {tbl} WHERE snapshot_date = %(snapshot_date)s::DATE AND customer_id = %(customer_id)s", {"snapshot_date": date_str, "customer_id": customer_id})
        insert_sql = f"INSERT INTO {tbl} (snapshot_date, customer_id, campaign_id, ad_group_id, criterion_id, change_type, audience_type, audience_id, audience_name, targeting_mode, old_value, new_value, old_size, new_size) VALUES (%(snapshot_date)s::DATE, %(customer_id)s, %(campaign_id)s, %(ad_group_id)s, %(criterion_id)s, %(change_type)s, %(audience_type)s, %(audience_id)s, %(audience_name)s, %(targeting_mode)s, %(old_value)s, %(new_value)s, %(old_size)s, %(new_size)s)"
        params_list = [
            {
                "snapshot_date": date_str,
                "customer_id": customer_id,
                "campaign_id": r["campaign_id"],
                "ad_group_id": r.get("ad_group_id") or "",
                "criterion_id": r["criterion_id"],
                "change_type": _safe_str(r.get("change_type"), 32),
                "audience_type": _safe_str(r.get("audience_type"), 64),
                "audience_id": _safe_str(r.get("audience_id"), 256),
                "audience_name": _safe_str(r.get("audience_name"), 512),
                "targeting_mode": _safe_str(r.get("targeting_mode"), 32),
                "old_value": _safe_str(r.get("old_value"), 65535),
                "new_value": _safe_str(r.get("new_value"), 65535),
                "old_size": r.get("old_size"),
                "new_size": r.get("new_size"),
            }
            for r in rows
        ]
        execute_many(conn, insert_sql, params_list)
        conn.commit()
        logger.info("ppc_flight_recorder: inserted %s audience_targeting_diff rows for customer_id=%s @ %s", len(rows), customer_id, date_str)

    _run_with_conn(conn, do)
    return len(rows)


def upsert_keyword_dims(reference_date: date, customer_id: str, rows: List[Dict[str, Any]], conn: Optional[Any] = None) -> int:
    """Upsert keyword dimension table (id -> keyword_text, match_type, ad_group_id, campaign_id) for lookup when viewing outcomes."""
    if not rows:
        return 0
    tbl = _table("ppc_keyword_dims")
    ref_str = reference_date.isoformat()

    def do(conn):
        seen = set()
        for r in rows:
            kid = str(r.get("keyword_criterion_id") or r.get("keywordCriterionId") or "")
            if not kid or (kid, customer_id) in seen:
                continue
            seen.add((kid, customer_id))
            merge_sql = f"""
                MERGE INTO {tbl} AS target
                USING (SELECT %(keyword_criterion_id)s AS keyword_criterion_id, %(ad_group_id)s AS ad_group_id, %(campaign_id)s AS campaign_id, %(customer_id)s AS customer_id, %(keyword_text)s AS keyword_text, %(match_type)s AS match_type, %(last_seen_date)s::DATE AS last_seen_date) AS source
                ON target.keyword_criterion_id = source.keyword_criterion_id AND target.customer_id = source.customer_id
                WHEN MATCHED THEN UPDATE SET ad_group_id = source.ad_group_id, campaign_id = source.campaign_id, keyword_text = source.keyword_text, match_type = source.match_type, last_seen_date = source.last_seen_date, updated_at = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT (keyword_criterion_id, ad_group_id, campaign_id, customer_id, keyword_text, match_type, last_seen_date) VALUES (source.keyword_criterion_id, source.ad_group_id, source.campaign_id, source.customer_id, source.keyword_text, source.match_type, source.last_seen_date)
            """
            execute(conn, merge_sql, {
                "keyword_criterion_id": kid,
                "ad_group_id": r.get("ad_group_id") or r.get("adGroupId"),
                "campaign_id": r.get("campaign_id") or r.get("campaignId"),
                "customer_id": customer_id,
                "keyword_text": _safe_str(r.get("keyword_text") or r.get("keywordText"), 1024),
                "match_type": _safe_str(r.get("match_type") or r.get("matchType"), 32),
                "last_seen_date": ref_str,
            })
        conn.commit()
        logger.info("ppc_flight_recorder: upserted %s keyword_dims for customer_id=%s", len(seen), customer_id)

    _run_with_conn(conn, do)
    return len(rows)
