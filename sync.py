"""
PPC Flight Recorder – Daily sync. Run this project separately from Leonardo.

Design (state vs change vs outcome):
  - Google Ads: control_state_daily (state) -> control_diff_daily (diff); outcomes_daily (outcome) -> outcomes_diff_daily (diff).
  - GA4: ga4_acquisition_daily (state: traffic/user/overview) -> ga4_acquisition_diff_daily (diff).
So we have two diff tables for Google Ads (control + outcomes) and one for GA4.

  cd ppc_flight_recorder
  pip install -r requirements.txt
  copy env.example.txt to .env and set credentials
  python sync.py [--date YYYY-MM-DD] [--project the-pinch] [--ga4]
  Historical (1-year backfill):
  python sync.py --start-date 2024-02-06 --end-date 2025-02-06 [--batch-days 30] [--ga4] [--diffs]
"""

import argparse
import logging
import sys
import time
from collections import defaultdict
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from config import PPC_PROJECTS, get_google_ads_customer_id, normalize_customer_id
from ga4_client import fetch_ga4_acquisition_all_sync
from google_ads_client import (
    fetch_ad_groups_daily,
    fetch_campaign_control_state,
    fetch_campaigns,
    fetch_campaigns_daily,
    fetch_keywords_daily,
)
from snowflake_connection import get_connection
from storage import (
    get_ad_group_outcomes_for_date,
    get_control_state_for_date,
    get_ga4_acquisition_for_date,
    get_keyword_outcomes_for_date,
    get_outcomes_for_date,
    insert_ad_group_outcomes_diff_daily,
    insert_control_diff_daily,
    insert_ga4_acquisition_diff_daily,
    insert_keyword_outcomes_diff_daily,
    insert_outcomes_diff_daily,
    upsert_ad_group_dims,
    upsert_ad_group_outcomes_batch,
    upsert_ad_group_outcomes_daily,
    upsert_campaign_dims,
    upsert_control_state_daily,
    upsert_ga4_acquisition_daily,
    upsert_ga4_traffic_acquisition_daily,
    upsert_keyword_dims,
    upsert_keyword_outcomes_batch,
    upsert_keyword_outcomes_daily,
    upsert_outcomes_batch,
    upsert_outcomes_daily,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

OUTCOME_METRIC_FIELDS = [
    "impressions", "clicks", "cost_micros", "cost_amount", "conversions", "conversions_value",
    "ctr", "cpc", "cpa", "roas", "cvr", "search_impression_share_pct", "search_rank_lost_impression_share_pct",
]

GA4_ACQUISITION_METRIC_FIELDS = [
    "sessions", "engaged_sessions", "total_revenue", "event_count", "key_events", "active_users",
    "average_session_duration_sec", "engagement_rate", "bounce_rate",
]

CONTROL_STATE_METRIC_FIELDS = [
    "campaign_name", "status", "advertising_channel_type", "advertising_channel_sub_type",
    "daily_budget_micros", "daily_budget_amount", "budget_delivery_method", "bidding_strategy_type",
    "target_cpa_micros", "target_cpa_amount", "target_roas",
    "geo_target_ids", "network_settings_target_google_search", "network_settings_target_search_network",
    "network_settings_target_content_network", "network_settings_target_partner_search_network",
    "ad_schedule_json", "audience_target_count",
]

# Max rows per MERGE in historical backfill to avoid timeouts
GA4_UPSERT_BATCH_SIZE = 2000
OUTCOMES_UPSERT_BATCH_SIZE = 1000  # Batch size for campaign outcomes upserts
AD_GROUP_UPSERT_BATCH_SIZE = 1000  # Batch size for ad group outcomes upserts
KEYWORD_UPSERT_BATCH_SIZE = 1000   # Batch size for keyword outcomes upserts


def _control_state_row_for_storage(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "campaign_id": row.get("campaign_id"), "campaign_name": row.get("campaign_name"),
        "status": row.get("status"), "advertising_channel_type": row.get("advertising_channel_type"),
        "advertising_channel_sub_type": row.get("advertising_channel_sub_type"),
        "daily_budget_micros": row.get("daily_budget_micros"), "daily_budget_amount": row.get("daily_budget_amount"),
        "budget_delivery_method": row.get("budget_delivery_method"), "bidding_strategy_type": row.get("bidding_strategy_type"),
        "target_cpa_micros": row.get("target_cpa_micros"), "target_cpa_amount": row.get("target_cpa_amount"), "target_roas": row.get("target_roas"),
        "geo_target_ids": row.get("geo_target_ids"),
        "network_settings_target_google_search": row.get("network_settings_target_google_search"),
        "network_settings_target_search_network": row.get("network_settings_target_search_network"),
        "network_settings_target_content_network": row.get("network_settings_target_content_network"),
        "network_settings_target_partner_search_network": row.get("network_settings_target_partner_search_network"),
        "ad_schedule_json": row.get("ad_schedule_json"),
        "audience_target_count": row.get("audience_target_count"),
    }


def compute_control_state_diffs(current: List[Dict[str, Any]], prior: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Compare control state metrics day-over-day; return list for ppc_campaign_control_diff_daily.
    Inserts a row for every (campaign, metric) every day, even when unchanged (old_value = new_value).
    """
    prior_by_cid = {r["campaign_id"]: r for r in prior}
    diffs = []
    for cur in current:
        cid = cur.get("campaign_id")
        prev = prior_by_cid.get(cid)
        if not prev:
            continue
        for field in CONTROL_STATE_METRIC_FIELDS:
            ov, nv = prev.get(field), cur.get(field)
            diffs.append({
                "campaign_id": cid,
                "changed_metric_name": field,
                "old_value": str(ov) if ov is not None else None,
                "new_value": str(nv) if nv is not None else None,
            })
    return diffs


def _outcome_row_for_diff(row: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize outcome row (API or DB) to same keys for diff comparison."""
    cid = row.get("campaignId") or row.get("campaign_id")
    cost = row.get("cost")
    cost_amount = row.get("cost_amount")
    if cost_amount is None and cost is not None:
        cost_amount = float(cost)
    return {
        "campaign_id": cid,
        "impressions": int(row.get("impressions") or 0),
        "clicks": int(row.get("clicks") or 0),
        "cost_micros": row.get("cost_micros") or (int(float(cost or 0) * 1_000_000) if cost is not None else 0),
        "cost_amount": float(cost_amount or 0),
        "conversions": float(row.get("conversions") or 0),
        "conversions_value": float(row.get("conversionValue") or row.get("conversions_value") or 0),
        "ctr": row.get("ctr"), "cpc": row.get("cpc"), "cpa": row.get("cpa"), "roas": row.get("roas"), "cvr": row.get("cvr"),
        "search_impression_share_pct": row.get("impressionSharePct") or row.get("search_impression_share_pct"),
        "search_rank_lost_impression_share_pct": row.get("search_rank_lost_impression_share_pct"),
    }


def compute_outcome_diffs(current: List[Dict[str, Any]], prior: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Compare outcome metrics day-over-day; return list for ppc_campaign_outcomes_diff_daily."""
    cur_norm = [_outcome_row_for_diff(r) for r in current]
    prior_norm = [_outcome_row_for_diff(r) for r in prior]
    prior_by_cid = {r["campaign_id"]: r for r in prior_norm}
    diffs = []
    for cur in cur_norm:
        cid = cur.get("campaign_id")
        prev = prior_by_cid.get(cid)
        if not prev:
            continue
        for field in OUTCOME_METRIC_FIELDS:
            ov, nv = prev.get(field), cur.get(field)
            if ov is None and nv is None:
                continue
            if ov != nv:
                diffs.append({"campaign_id": cid, "changed_metric_name": field, "old_value": str(ov) if ov is not None else None, "new_value": str(nv) if nv is not None else None})
    return diffs


def _ad_group_outcome_row_for_diff(row: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize ad group outcome row for diff comparison."""
    agid = row.get("ad_group_id") or row.get("adGroupId")
    cid = row.get("campaign_id") or row.get("campaignId")
    cost = row.get("cost")
    cost_amount = row.get("cost_amount")
    if cost_amount is None and cost is not None:
        cost_amount = float(cost)
    return {
        "ad_group_id": agid,
        "campaign_id": cid,
        "impressions": int(row.get("impressions") or 0),
        "clicks": int(row.get("clicks") or 0),
        "cost_micros": row.get("cost_micros") or (int(float(cost or 0) * 1_000_000) if cost is not None else 0),
        "cost_amount": float(cost_amount or 0),
        "conversions": float(row.get("conversions") or 0),
        "conversions_value": float(row.get("conversionValue") or row.get("conversions_value") or 0),
        "ctr": row.get("ctr"), "cpc": row.get("cpc"), "cpa": row.get("cpa"), "roas": row.get("roas"), "cvr": row.get("cvr"),
        "search_impression_share_pct": row.get("impressionSharePct") or row.get("search_impression_share_pct"),
        "search_rank_lost_impression_share_pct": row.get("search_rank_lost_impression_share_pct"),
    }


def compute_ad_group_outcome_diffs(current: List[Dict[str, Any]], prior: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    prior_by_agid = {r["ad_group_id"]: r for r in prior}
    diffs = []
    for cur in current:
        agid = cur.get("ad_group_id")
        prev = prior_by_agid.get(agid)
        if not prev:
            continue
        for field in OUTCOME_METRIC_FIELDS:
            ov, nv = prev.get(field), cur.get(field)
            if ov is None and nv is None:
                continue
            if ov != nv:
                diffs.append({
                    "ad_group_id": agid,
                    "campaign_id": cur.get("campaign_id"),
                    "changed_metric_name": field,
                    "old_value": str(ov) if ov is not None else None,
                    "new_value": str(nv) if nv is not None else None,
                })
    return diffs


def _keyword_outcome_row_for_diff(row: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize keyword outcome row for diff comparison."""
    kid = str(row.get("keyword_criterion_id") or row.get("keywordCriterionId") or "")
    cost = row.get("cost")
    cost_amount = row.get("cost_amount")
    if cost_amount is None and cost is not None:
        cost_amount = float(cost)
    return {
        "keyword_criterion_id": kid,
        "impressions": int(row.get("impressions") or 0),
        "clicks": int(row.get("clicks") or 0),
        "cost_micros": row.get("cost_micros") or (int(float(cost or 0) * 1_000_000) if cost is not None else 0),
        "cost_amount": float(cost_amount or 0),
        "conversions": float(row.get("conversions") or 0),
        "conversions_value": float(row.get("conversionValue") or row.get("conversions_value") or 0),
        "ctr": row.get("ctr"), "cpc": row.get("cpc"), "cpa": row.get("cpa"), "roas": row.get("roas"), "cvr": row.get("cvr"),
        "search_impression_share_pct": row.get("impressionSharePct") or row.get("search_impression_share_pct"),
        "search_rank_lost_impression_share_pct": row.get("search_rank_lost_impression_share_pct"),
    }


def compute_keyword_outcome_diffs(current: List[Dict[str, Any]], prior: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    prior_by_kid = {r["keyword_criterion_id"]: r for r in prior}
    diffs = []
    for cur in current:
        kid = cur.get("keyword_criterion_id")
        prev = prior_by_kid.get(kid)
        if not prev:
            continue
        for field in OUTCOME_METRIC_FIELDS:
            ov, nv = prev.get(field), cur.get(field)
            if ov is None and nv is None:
                continue
            if ov != nv:
                diffs.append({
                    "keyword_criterion_id": kid,
                    "changed_metric_name": field,
                    "old_value": str(ov) if ov is not None else None,
                    "new_value": str(nv) if nv is not None else None,
                })
    return diffs


def compute_ga4_acquisition_diffs(current: List[Dict[str, Any]], prior: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Compare GA4 acquisition metrics day-over-day; return list for ppc_ga4_acquisition_diff_daily."""
    key = lambda r: (r.get("report_type"), r.get("dimension_type"), r.get("dimension_value"))
    prior_by_key = {key(r): r for r in prior}
    diffs = []
    for cur in current:
        k = key(cur)
        prev = prior_by_key.get(k)
        if not prev:
            continue
        for field in GA4_ACQUISITION_METRIC_FIELDS:
            ov, nv = prev.get(field), cur.get(field)
            if ov is None and nv is None:
                continue
            if ov != nv:
                diffs.append({
                    "report_type": cur.get("report_type"), "dimension_type": cur.get("dimension_type"), "dimension_value": cur.get("dimension_value"),
                    "changed_metric_name": field, "old_value": str(ov) if ov is not None else None, "new_value": str(nv) if nv is not None else None,
                })
    return diffs


def run_sync(
    snapshot_date: date,
    projects: List[str],
    run_ga4: bool = False,
    google_ads_filters: Optional[Dict[str, Any]] = None,
    control_state_only: bool = False,
) -> None:
    snapshot_str = snapshot_date.isoformat()
    prior_date = snapshot_date - timedelta(days=1)

    # One Snowflake connection for the whole run to avoid repeated slow connects
    with get_connection() as conn:
        for project in projects:
            customer_id = normalize_customer_id(get_google_ads_customer_id(project))
            logger.info("Syncing PPC Flight Recorder for project=%s (customer_id=%s) date=%s%s", project, customer_id, snapshot_str, " (control state only)" if control_state_only else "")
            try:
                control_rows = fetch_campaign_control_state(project=project, google_ads_filters=google_ads_filters)
                if not control_rows:
                    logger.warning("No control state rows for project=%s", project)
                else:
                    state_for_storage = [_control_state_row_for_storage(r) for r in control_rows]
                    upsert_control_state_daily(snapshot_date, customer_id, state_for_storage, conn=conn)
                    upsert_campaign_dims(snapshot_date, customer_id, state_for_storage, conn=conn)
                    prior_control = get_control_state_for_date(customer_id, prior_date, conn=conn)
                    if prior_control:
                        control_diff_list = compute_control_state_diffs(state_for_storage, prior_control)
                        if control_diff_list:
                            insert_control_diff_daily(snapshot_date, customer_id, control_diff_list, conn=conn)

                if control_state_only:
                    continue

                campaigns_one_day = fetch_campaigns(start_date=snapshot_str, end_date=snapshot_str, project=project, google_ads_filters=google_ads_filters)
                if campaigns_one_day:
                    upsert_outcomes_daily(snapshot_date, customer_id, campaigns_one_day, conn=conn)
                    prior_outcomes = get_outcomes_for_date(customer_id, prior_date, conn=conn)
                    if prior_outcomes:
                        outcome_diff_list = compute_outcome_diffs(campaigns_one_day, prior_outcomes)
                        if outcome_diff_list:
                            insert_outcomes_diff_daily(snapshot_date, customer_id, outcome_diff_list, conn=conn)

                ad_groups_one_day = fetch_ad_groups_daily(start_date=snapshot_str, end_date=snapshot_str, project=project, google_ads_filters=google_ads_filters)
                if ad_groups_one_day:
                    upsert_ad_group_outcomes_daily(snapshot_date, customer_id, ad_groups_one_day, conn=conn)
                    upsert_ad_group_dims(snapshot_date, customer_id, ad_groups_one_day, conn=conn)
                    prior_ag = get_ad_group_outcomes_for_date(customer_id, prior_date, conn=conn)
                    if prior_ag:
                        ag_norm_cur = [_ad_group_outcome_row_for_diff(r) for r in ad_groups_one_day]
                        ag_norm_prior = [_ad_group_outcome_row_for_diff(r) for r in prior_ag]
                        ag_diff_list = compute_ad_group_outcome_diffs(ag_norm_cur, ag_norm_prior)
                        if ag_diff_list:
                            insert_ad_group_outcomes_diff_daily(snapshot_date, customer_id, ag_diff_list, conn=conn)

                keywords_one_day = fetch_keywords_daily(start_date=snapshot_str, end_date=snapshot_str, project=project, google_ads_filters=google_ads_filters)
                if keywords_one_day:
                    upsert_keyword_outcomes_daily(snapshot_date, customer_id, keywords_one_day, conn=conn)
                    upsert_keyword_dims(snapshot_date, customer_id, keywords_one_day, conn=conn)
                    prior_kw = get_keyword_outcomes_for_date(customer_id, prior_date, conn=conn)
                    if prior_kw:
                        kw_norm_cur = [_keyword_outcome_row_for_diff(r) for r in keywords_one_day]
                        kw_norm_prior = [_keyword_outcome_row_for_diff(r) for r in prior_kw]
                        kw_diff_list = compute_keyword_outcome_diffs(kw_norm_cur, kw_norm_prior)
                        if kw_diff_list:
                            insert_keyword_outcomes_diff_daily(snapshot_date, customer_id, kw_diff_list, conn=conn)
            except Exception as e:
                logger.exception("PPC Flight Recorder sync failed for project=%s: %s", project, e)
                raise

        if control_state_only:
            return

        if run_ga4:
            logger.info("Fetching GA4 acquisition (traffic, user, overview) for date=%s", snapshot_str)
            all_ga4_rows: List[Dict[str, Any]] = []
            for proj in projects:
                try:
                    ga4_rows = fetch_ga4_acquisition_all_sync(start_date=snapshot_str, end_date=snapshot_str, project=proj)
                    if ga4_rows:
                        all_ga4_rows.extend(ga4_rows)
                except Exception as e:
                    logger.warning("GA4 acquisition failed for project=%s: %s", proj, e)
            if all_ga4_rows:
                upsert_ga4_acquisition_daily(all_ga4_rows, conn=conn)
                for proj in projects:
                    proj_rows = [r for r in all_ga4_rows if r.get("project") == proj]
                    if not proj_rows:
                        continue
                    prior_ga4 = get_ga4_acquisition_for_date(proj, prior_date, conn=conn)
                    if prior_ga4:
                        ga4_diff_list = compute_ga4_acquisition_diffs(proj_rows, prior_ga4)
                        if ga4_diff_list:
                            insert_ga4_acquisition_diff_daily(snapshot_date, proj, ga4_diff_list, conn=conn)
                traffic_only = [r for r in all_ga4_rows if r.get("report_type") == "traffic_acquisition"]
                if traffic_only:
                    upsert_ga4_traffic_acquisition_daily(traffic_only, conn=conn)


def run_historical_sync(
    start_date: date,
    end_date: date,
    projects: List[str],
    batch_days: int = 30,
    run_ga4: bool = True,
    no_diffs: bool = True,
    google_ads_filters: Optional[Dict[str, Any]] = None,
    delay_seconds: float = 1.0,
) -> None:
    """Backfill historical data by date range. Fetches in batches (by parameter), upserts in batches."""
    if start_date > end_date:
        raise ValueError("start_date must be <= end_date")
    with get_connection() as conn:
        current = start_date
        while current <= end_date:
            chunk_end = min(current + timedelta(days=batch_days - 1), end_date)
            chunk_start_str = current.isoformat()
            chunk_end_str = chunk_end.isoformat()
            logger.info("Historical batch: %s .. %s", chunk_start_str, chunk_end_str)

            # Google Ads outcomes: fetch per project for chunk range, then batch upsert all at once
            for project in projects:
                customer_id = normalize_customer_id(get_google_ads_customer_id(project))
                try:
                    time.sleep(delay_seconds)
                    daily_rows = fetch_campaigns_daily(
                        start_date=chunk_start_str,
                        end_date=chunk_end_str,
                        project=project,
                        google_ads_filters=google_ads_filters,
                    )
                    # Add customer_id to all rows for batch upsert
                    if daily_rows:
                        for r in daily_rows:
                            r["customer_id"] = customer_id
                        # Batch upsert all rows in chunks
                        for i in range(0, len(daily_rows), OUTCOMES_UPSERT_BATCH_SIZE):
                            chunk_rows = daily_rows[i : i + OUTCOMES_UPSERT_BATCH_SIZE]
                            upsert_outcomes_batch(chunk_rows, conn=conn)
                        # Upsert campaign dims (grouped by date for dims)
                        by_date: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
                        for r in daily_rows:
                            d = r.get("outcome_date")
                            if d:
                                by_date[d].append(r)
                        for outcome_date_str, rows in sorted(by_date.items()):
                            outcome_d = date.fromisoformat(outcome_date_str)
                            upsert_campaign_dims(outcome_d, customer_id, rows, conn=conn)
                        # Compute diffs day-by-day (needs prior day data)
                        if not no_diffs:
                            for outcome_date_str, rows in sorted(by_date.items()):
                                outcome_d = date.fromisoformat(outcome_date_str)
                                prior_d = outcome_d - timedelta(days=1)
                                prior_outcomes = get_outcomes_for_date(customer_id, prior_d, conn=conn)
                                if prior_outcomes:
                                    outcome_diff_list = compute_outcome_diffs(rows, prior_outcomes)
                                    if outcome_diff_list:
                                        insert_outcomes_diff_daily(outcome_d, customer_id, outcome_diff_list, conn=conn)

                    time.sleep(delay_seconds)
                    ad_group_daily = fetch_ad_groups_daily(
                        start_date=chunk_start_str, end_date=chunk_end_str, project=project, google_ads_filters=google_ads_filters
                    )
                    if ad_group_daily:
                        # Add customer_id to all rows for batch upsert
                        for r in ad_group_daily:
                            r["customer_id"] = customer_id
                        # Batch upsert all rows in chunks
                        for i in range(0, len(ad_group_daily), AD_GROUP_UPSERT_BATCH_SIZE):
                            chunk_rows = ad_group_daily[i : i + AD_GROUP_UPSERT_BATCH_SIZE]
                            upsert_ad_group_outcomes_batch(chunk_rows, conn=conn)
                        # Upsert ad group dims (grouped by date for dims)
                        by_date_ag: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
                        for r in ad_group_daily:
                            d = r.get("outcome_date")
                            if d:
                                by_date_ag[d].append(r)
                        for outcome_date_str, rows in sorted(by_date_ag.items()):
                            outcome_d = date.fromisoformat(outcome_date_str)
                            upsert_ad_group_dims(outcome_d, customer_id, rows, conn=conn)
                        # Compute diffs day-by-day (needs prior day data)
                        if not no_diffs:
                            for outcome_date_str, rows in sorted(by_date_ag.items()):
                                outcome_d = date.fromisoformat(outcome_date_str)
                                prior_ag = get_ad_group_outcomes_for_date(customer_id, outcome_d - timedelta(days=1), conn=conn)
                                if prior_ag:
                                    ag_norm_cur = [_ad_group_outcome_row_for_diff(r) for r in rows]
                                    ag_norm_prior = [_ad_group_outcome_row_for_diff(r) for r in prior_ag]
                                    ag_diff_list = compute_ad_group_outcome_diffs(ag_norm_cur, ag_norm_prior)
                                    if ag_diff_list:
                                        insert_ad_group_outcomes_diff_daily(outcome_d, customer_id, ag_diff_list, conn=conn)

                    time.sleep(delay_seconds)
                    keyword_daily = fetch_keywords_daily(
                        start_date=chunk_start_str, end_date=chunk_end_str, project=project, google_ads_filters=google_ads_filters
                    )
                    if keyword_daily:
                        # Add customer_id to all rows for batch upsert
                        for r in keyword_daily:
                            r["customer_id"] = customer_id
                        # Batch upsert all rows in chunks
                        for i in range(0, len(keyword_daily), KEYWORD_UPSERT_BATCH_SIZE):
                            chunk_rows = keyword_daily[i : i + KEYWORD_UPSERT_BATCH_SIZE]
                            upsert_keyword_outcomes_batch(chunk_rows, conn=conn)
                        # Upsert keyword dims (grouped by date for dims)
                        by_date_kw: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
                        for r in keyword_daily:
                            d = r.get("outcome_date")
                            if d:
                                by_date_kw[d].append(r)
                        for outcome_date_str, rows in sorted(by_date_kw.items()):
                            outcome_d = date.fromisoformat(outcome_date_str)
                            upsert_keyword_dims(outcome_d, customer_id, rows, conn=conn)
                        # Compute diffs day-by-day (needs prior day data)
                        if not no_diffs:
                            for outcome_date_str, rows in sorted(by_date_kw.items()):
                                outcome_d = date.fromisoformat(outcome_date_str)
                                prior_kw = get_keyword_outcomes_for_date(customer_id, outcome_d - timedelta(days=1), conn=conn)
                                if prior_kw:
                                    kw_norm_cur = [_keyword_outcome_row_for_diff(r) for r in rows]
                                    kw_norm_prior = [_keyword_outcome_row_for_diff(r) for r in prior_kw]
                                    kw_diff_list = compute_keyword_outcome_diffs(kw_norm_cur, kw_norm_prior)
                                    if kw_diff_list:
                                        insert_keyword_outcomes_diff_daily(outcome_d, customer_id, kw_diff_list, conn=conn)
                except Exception as e:
                    logger.exception(
                        "Historical Google Ads failed for project=%s batch %s..%s: %s",
                        project, chunk_start_str, chunk_end_str, e,
                    )
                    raise

            # GA4: fetch for chunk range, upsert in row batches
            if run_ga4:
                all_ga4_rows: List[Dict[str, Any]] = []
                for proj in projects:
                    try:
                        time.sleep(delay_seconds)
                        ga4_rows = fetch_ga4_acquisition_all_sync(
                            start_date=chunk_start_str, end_date=chunk_end_str, project=proj
                        )
                        if ga4_rows:
                            all_ga4_rows.extend(ga4_rows)
                    except Exception as e:
                        logger.warning("GA4 acquisition failed for project=%s: %s", proj, e)
                if all_ga4_rows:
                    for i in range(0, len(all_ga4_rows), GA4_UPSERT_BATCH_SIZE):
                        chunk_rows = all_ga4_rows[i : i + GA4_UPSERT_BATCH_SIZE]
                        upsert_ga4_acquisition_daily(chunk_rows, conn=conn)
                    traffic_only = [r for r in all_ga4_rows if r.get("report_type") == "traffic_acquisition"]
                    for i in range(0, len(traffic_only), GA4_UPSERT_BATCH_SIZE):
                        upsert_ga4_traffic_acquisition_daily(traffic_only[i : i + GA4_UPSERT_BATCH_SIZE], conn=conn)
                    if not no_diffs:
                        for proj in projects:
                            proj_rows = [r for r in all_ga4_rows if r.get("project") == proj]
                            if not proj_rows:
                                continue
                            by_acq_date: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
                            for r in proj_rows:
                                by_acq_date[r.get("acquisition_date", "")].append(r)
                            for acq_date_str, rows in sorted(by_acq_date.items()):
                                if not acq_date_str:
                                    continue
                                acq_d = date.fromisoformat(acq_date_str)
                                prior_ga4 = get_ga4_acquisition_for_date(proj, acq_d - timedelta(days=1), conn=conn)
                                if prior_ga4:
                                    ga4_diff_list = compute_ga4_acquisition_diffs(rows, prior_ga4)
                                    if ga4_diff_list:
                                        insert_ga4_acquisition_diff_daily(acq_d, proj, ga4_diff_list, conn=conn)

            current = chunk_end + timedelta(days=1)

        # Control state: one snapshot for end_date (current state)
        for project in projects:
            customer_id = normalize_customer_id(get_google_ads_customer_id(project))
            try:
                time.sleep(delay_seconds)
                control_rows = fetch_campaign_control_state(project=project, google_ads_filters=google_ads_filters)
                if control_rows:
                    state_for_storage = [_control_state_row_for_storage(r) for r in control_rows]
                    upsert_control_state_daily(end_date, customer_id, state_for_storage, conn=conn)
            except Exception as e:
                logger.warning("Control state failed for project=%s: %s", project, e)

    logger.info("Historical sync completed for %s .. %s", start_date.isoformat(), end_date.isoformat())


def main() -> None:
    parser = argparse.ArgumentParser(description="PPC Flight Recorder daily sync (standalone)")
    parser.add_argument("--date", type=str, default=None, help="Snapshot date YYYY-MM-DD (default: yesterday)")
    parser.add_argument("--project", type=str, default=None, help="Single project (default: all from PPC_PROJECTS)")
    parser.add_argument("--ga4", action="store_true", help="Also fetch and store GA4 traffic acquisition")
    parser.add_argument("--start-date", type=str, default=None, help="Historical backfill start YYYY-MM-DD (use with --end-date)")
    parser.add_argument("--end-date", type=str, default=None, help="Historical backfill end YYYY-MM-DD")
    parser.add_argument("--batch-days", type=int, default=30, help="Chunk size in days for historical fetch (default: 30)")
    parser.add_argument("--no-diffs", action="store_true", default=True, help="Skip diff computation in historical (default: True)")
    parser.add_argument("--diffs", action="store_false", dest="no_diffs", help="Compute outcome/GA4 diffs during historical backfill")
    parser.add_argument("--control-state-only", action="store_true", help="Update only ppc_campaign_control_state_daily and ppc_campaign_control_diff_daily (no outcomes, no GA4)")
    args = parser.parse_args()

    projects = [args.project] if args.project else [p.strip() for p in PPC_PROJECTS.split(",") if p.strip()] or ["the-pinch"]

    if args.start_date and args.end_date:
        try:
            start_date = date.fromisoformat(args.start_date)
            end_date = date.fromisoformat(args.end_date)
        except ValueError as e:
            logger.error("Invalid --start-date or --end-date: %s", e)
            sys.exit(1)
        run_historical_sync(
            start_date=start_date,
            end_date=end_date,
            projects=projects,
            batch_days=args.batch_days,
            run_ga4=args.ga4,
            no_diffs=args.no_diffs,
        )
        return

    if args.date:
        try:
            snapshot_date = date.fromisoformat(args.date)
        except ValueError:
            logger.error("Invalid --date: %s", args.date)
            sys.exit(1)
    else:
        snapshot_date = date.today() - timedelta(days=1)

    run_sync(
        snapshot_date=snapshot_date,
        projects=projects,
        run_ga4=args.ga4,
        control_state_only=args.control_state_only,
    )
    logger.info("PPC Flight Recorder sync completed for %s", snapshot_date.isoformat())


if __name__ == "__main__":
    main()
