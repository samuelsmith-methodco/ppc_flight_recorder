"""
PPC Flight Recorder – Google Ads API client (standalone).
"""

import logging
from typing import Any, Dict, List, Optional

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

from config import (
    GOOGLE_ADS_CLIENT_ID,
    GOOGLE_ADS_CLIENT_SECRET,
    GOOGLE_ADS_CUSTOMER_ID,
    GOOGLE_ADS_DEVELOPER_TOKEN,
    GOOGLE_ADS_LOGIN_CUSTOMER_ID,
    GOOGLE_ADS_REFRESH_TOKEN,
    get_google_ads_customer_id,
)

logger = logging.getLogger(__name__)

_client: Optional[GoogleAdsClient] = None


def get_client() -> GoogleAdsClient:
    global _client
    if _client is None:
        if not all([GOOGLE_ADS_DEVELOPER_TOKEN, GOOGLE_ADS_CLIENT_ID, GOOGLE_ADS_CLIENT_SECRET, GOOGLE_ADS_REFRESH_TOKEN]):
            raise RuntimeError("Google Ads credentials not set in .env (DEVELOPER_TOKEN, CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN)")
        _client = GoogleAdsClient.load_from_dict({
            "developer_token": GOOGLE_ADS_DEVELOPER_TOKEN,
            "client_id": GOOGLE_ADS_CLIENT_ID,
            "client_secret": GOOGLE_ADS_CLIENT_SECRET,
            "refresh_token": GOOGLE_ADS_REFRESH_TOKEN,
            "login_customer_id": GOOGLE_ADS_LOGIN_CUSTOMER_ID or "",
            "use_proto_plus": True,
        })
    return _client


def _customer_id_clean(project: str) -> str:
    cid = get_google_ads_customer_id(project)
    if not cid:
        raise ValueError(f"Google Ads customer ID not configured for project: {project}")
    return cid.replace("-", "")


def fetch_campaign_control_state(
    project: str,
    google_ads_filters: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Fetch current campaign control state (settings only). One row per campaign."""
    client = get_client()
    customer_id_clean = _customer_id_clean(project)
    ga_service = client.get_service("GoogleAdsService")
    query = """
        SELECT campaign.id, campaign.name, campaign.status, campaign.advertising_channel_type, campaign.advertising_channel_sub_type,
               campaign_budget.amount_micros, campaign_budget.delivery_method
        FROM campaign
        WHERE campaign.status != 'REMOVED'
    """
    rows_out = []
    try:
        stream = ga_service.search_stream(customer_id=customer_id_clean, query=query)
        for batch in stream:
            for row in batch.results:
                camp = row.campaign
                budget = getattr(row, "campaign_budget", None)
                amount_micros = getattr(budget, "amount_micros", None) if budget else None
                delivery_method = getattr(budget, "delivery_method", None) if budget else None
                if delivery_method and hasattr(delivery_method, "name"):
                    delivery_method = delivery_method.name
                campaign_id = str(camp.id)
                campaign_name = camp.name if camp.name else "Unnamed Campaign"
                status = camp.status.name if hasattr(camp.status, "name") else str(camp.status)
                channel_type = camp.advertising_channel_type.name if hasattr(camp.advertising_channel_type, "name") else str(camp.advertising_channel_type)
                sub_type = camp.advertising_channel_sub_type.name if hasattr(camp.advertising_channel_sub_type, "name") else str(camp.advertising_channel_sub_type)
                if google_ads_filters and google_ads_filters.get("campaignNamePatterns"):
                    patterns = google_ads_filters.get("campaignNamePatterns", [])
                    if patterns and not any(p.lower() in campaign_name.lower() for p in patterns):
                        continue
                daily_budget_micros = int(amount_micros) if amount_micros is not None else None
                daily_budget_amount = (daily_budget_micros / 1_000_000.0) if daily_budget_micros else None
                rows_out.append({
                    "campaign_id": campaign_id, "campaign_name": campaign_name, "status": status,
                    "advertising_channel_type": channel_type, "advertising_channel_sub_type": sub_type,
                    "daily_budget_micros": daily_budget_micros, "daily_budget_amount": daily_budget_amount,
                    "budget_delivery_method": delivery_method,
                    "bidding_strategy_type": None, "target_cpa_micros": None, "target_cpa_amount": None, "target_roas": None,
                })
        logger.info("fetch_campaign_control_state: %s campaigns for project %s", len(rows_out), project)
    except GoogleAdsException as ex:
        logger.error("Google Ads API error: %s", ex)
        return []
    return rows_out


def fetch_campaigns(
    start_date: str,
    end_date: str,
    project: str,
    google_ads_filters: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Fetch campaign performance (one row per campaign, aggregated over date range)."""
    client = get_client()
    customer_id_clean = _customer_id_clean(project)
    ga_service = client.get_service("GoogleAdsService")
    query = f"""
        SELECT campaign.id, campaign.name, campaign.status, campaign.advertising_channel_type, campaign.advertising_channel_sub_type,
               segments.date, metrics.impressions, metrics.clicks, metrics.cost_micros, metrics.conversions,
               metrics.conversions_value, metrics.all_conversions_value, metrics.average_cpc, metrics.ctr,
               metrics.search_impression_share, metrics.search_rank_lost_impression_share
        FROM campaign
        WHERE campaign.status != 'REMOVED' AND segments.date BETWEEN '{start_date}' AND '{end_date}'
    """
    campaign_map = {}
    try:
        stream = ga_service.search_stream(customer_id=customer_id_clean, query=query)
        for batch in stream:
            for row in batch.results:
                camp = row.campaign
                metrics = row.metrics
                campaign_id = str(camp.id)
                campaign_name = camp.name if camp.name else "Unnamed Campaign"
                if google_ads_filters and google_ads_filters.get("campaignNamePatterns"):
                    patterns = google_ads_filters.get("campaignNamePatterns", [])
                    if patterns and not any(p.lower() in campaign_name.lower() for p in patterns):
                        continue
                if campaign_id not in campaign_map:
                    campaign_map[campaign_id] = {
                        "campaignId": campaign_id, "campaignName": campaign_name,
                        "impressions": 0, "clicks": 0, "cost": 0.0, "conversions": 0.0, "conversionValue": 0.0,
                        "ctr": 0.0, "cpc": 0.0, "roas": 0.0, "cpa": 0.0, "cvr": 0.0, "impressionSharePct": 0.0,
                        "_impression_share_weighted_sum": 0.0,
                        "_rank_lost_impression_share_weighted_sum": 0.0,
                    }
                d = campaign_map[campaign_id]
                imp = int(getattr(metrics, "impressions", 0))
                clk = int(getattr(metrics, "clicks", 0))
                cost_micros = int(getattr(metrics, "cost_micros", 0))
                conv = float(getattr(metrics, "conversions", 0))
                cv = float(getattr(metrics, "conversions_value", 0) or getattr(metrics, "all_conversions_value", 0))
                d["impressions"] += imp
                d["clicks"] += clk
                d["cost"] += cost_micros / 1_000_000.0
                d["conversions"] += conv
                d["conversionValue"] += cv
                is_share = getattr(metrics, "search_impression_share", None)
                rank_lost = getattr(metrics, "search_rank_lost_impression_share", None)
                if is_share is not None and imp > 0:
                    d["_impression_share_weighted_sum"] += float(is_share) * 100 * imp
                if rank_lost is not None and imp > 0:
                    d["_rank_lost_impression_share_weighted_sum"] += float(rank_lost) * 100 * imp
        campaigns = []
        for d in campaign_map.values():
            d["impressions"] = int(d["impressions"])
            d["clicks"] = int(d["clicks"])
            if d["impressions"] > 0:
                d["ctr"] = round((d["clicks"] / d["impressions"]) * 100, 2)
            if d["clicks"] > 0:
                d["cpc"] = round(d["cost"] / d["clicks"], 2)
            if d["cost"] > 0 and d["conversions"] > 0 and d["conversionValue"] > 0:
                d["roas"] = round(d["conversionValue"] / d["cost"], 4)
            if d["conversions"] > 0:
                d["cpa"] = round(d["cost"] / d["conversions"], 2)
                d["cvr"] = round((d["conversions"] / d["clicks"]) * 100, 2) if d["clicks"] > 0 else 0.0
            if d["impressions"] > 0 and d.get("_impression_share_weighted_sum", 0) > 0:
                d["impressionSharePct"] = round(d["_impression_share_weighted_sum"] / d["impressions"], 2)
            if d["impressions"] > 0 and d.get("_rank_lost_impression_share_weighted_sum", 0) > 0:
                d["search_rank_lost_impression_share_pct"] = round(d["_rank_lost_impression_share_weighted_sum"] / d["impressions"], 2)
            else:
                d["search_rank_lost_impression_share_pct"] = None
            d["search_impression_share_pct"] = d.get("impressionSharePct")
            d.pop("_impression_share_weighted_sum", None)
            d.pop("_rank_lost_impression_share_weighted_sum", None)
            campaigns.append(d)
        logger.info("fetch_campaigns: %s campaigns for %s..%s project %s", len(campaigns), start_date, end_date, project)
        return campaigns
    except GoogleAdsException as ex:
        logger.error("Google Ads API error: %s", ex)
        return []


def fetch_campaigns_daily(
    start_date: str,
    end_date: str,
    project: str,
    google_ads_filters: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Fetch campaign performance with one row per campaign per day (for historical backfill).
    Returns list of dicts each with outcome_date (YYYY-MM-DD), campaignId, campaignName, and metrics.
    """
    client = get_client()
    customer_id_clean = _customer_id_clean(project)
    ga_service = client.get_service("GoogleAdsService")
    query = f"""
        SELECT campaign.id, campaign.name, campaign.status, campaign.advertising_channel_type, campaign.advertising_channel_sub_type,
               segments.date, metrics.impressions, metrics.clicks, metrics.cost_micros, metrics.conversions,
               metrics.conversions_value, metrics.all_conversions_value, metrics.average_cpc, metrics.ctr,
               metrics.search_impression_share, metrics.search_rank_lost_impression_share
        FROM campaign
        WHERE campaign.status != 'REMOVED' AND segments.date BETWEEN '{start_date}' AND '{end_date}'
    """
    rows_out: List[Dict[str, Any]] = []
    try:
        stream = ga_service.search_stream(customer_id=customer_id_clean, query=query)
        for batch in stream:
            for row in batch.results:
                camp = row.campaign
                metrics = row.metrics
                segment_date = getattr(row.segments, "date", None) if hasattr(row, "segments") else None
                if not segment_date:
                    continue
                outcome_date = str(segment_date).replace("-", "")  # YYYYMMDD
                if len(outcome_date) == 8:
                    outcome_date = f"{outcome_date[:4]}-{outcome_date[4:6]}-{outcome_date[6:8]}"
                campaign_id = str(camp.id)
                campaign_name = camp.name if camp.name else "Unnamed Campaign"
                if google_ads_filters and google_ads_filters.get("campaignNamePatterns"):
                    patterns = google_ads_filters.get("campaignNamePatterns", [])
                    if patterns and not any(p.lower() in campaign_name.lower() for p in patterns):
                        continue
                imp = int(getattr(metrics, "impressions", 0))
                clk = int(getattr(metrics, "clicks", 0))
                cost_micros = int(getattr(metrics, "cost_micros", 0))
                cost = cost_micros / 1_000_000.0
                conv = float(getattr(metrics, "conversions", 0))
                cv = float(getattr(metrics, "conversions_value", 0) or getattr(metrics, "all_conversions_value", 0))
                ctr = round((clk / imp) * 100, 2) if imp > 0 else 0.0
                cpc = round(cost / clk, 2) if clk > 0 else 0.0
                roas = round(cv / cost, 4) if cost > 0 and cv > 0 else 0.0
                cpa = round(cost / conv, 2) if conv > 0 else 0.0
                cvr = round((conv / clk) * 100, 2) if clk > 0 else 0.0
                is_share = getattr(metrics, "search_impression_share", None)
                rank_lost = getattr(metrics, "search_rank_lost_impression_share", None)
                impression_share_pct = round(float(is_share) * 100, 2) if is_share is not None else None
                search_rank_lost_pct = round(float(rank_lost) * 100, 2) if rank_lost is not None else None
                rows_out.append({
                    "outcome_date": outcome_date,
                    "campaignId": campaign_id,
                    "campaignName": campaign_name,
                    "impressions": imp,
                    "clicks": clk,
                    "cost": cost,
                    "conversions": conv,
                    "conversionValue": cv,
                    "ctr": ctr,
                    "cpc": cpc,
                    "roas": roas,
                    "cpa": cpa,
                    "cvr": cvr,
                    "impressionSharePct": impression_share_pct,
                    "search_impression_share_pct": impression_share_pct,
                    "search_rank_lost_impression_share_pct": search_rank_lost_pct,
                })
        logger.info(
            "fetch_campaigns_daily: %s rows for %s..%s project %s",
            len(rows_out), start_date, end_date, project,
        )
    except GoogleAdsException as ex:
        logger.error("Google Ads API error: %s", ex)
    return rows_out


def fetch_ad_groups_daily(
    start_date: str,
    end_date: str,
    project: str,
    google_ads_filters: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Fetch ad group performance with one row per ad group per day. Same metrics as campaign level."""
    client = get_client()
    customer_id_clean = _customer_id_clean(project)
    ga_service = client.get_service("GoogleAdsService")
    query = f"""
        SELECT ad_group.id, ad_group.name, campaign.id, campaign.name,
               segments.date,
               metrics.impressions, metrics.clicks, metrics.cost_micros,
               metrics.conversions, metrics.conversions_value, metrics.all_conversions_value,
               metrics.average_cpc, metrics.ctr, metrics.search_impression_share, metrics.search_rank_lost_impression_share
        FROM ad_group
        WHERE campaign.status != 'REMOVED' AND ad_group.status != 'REMOVED'
          AND segments.date BETWEEN '{start_date}' AND '{end_date}'
    """
    rows_out: List[Dict[str, Any]] = []
    try:
        stream = ga_service.search_stream(customer_id=customer_id_clean, query=query)
        for batch in stream:
            for row in batch.results:
                ad_group = row.ad_group
                campaign = row.campaign
                metrics = row.metrics
                segment_date = getattr(row.segments, "date", None) if hasattr(row, "segments") else None
                if not segment_date:
                    continue
                outcome_date = str(segment_date).replace("-", "")
                if len(outcome_date) == 8:
                    outcome_date = f"{outcome_date[:4]}-{outcome_date[4:6]}-{outcome_date[6:8]}"
                campaign_id = str(campaign.id)
                campaign_name = campaign.name if campaign.name else ""
                if google_ads_filters and google_ads_filters.get("campaignNamePatterns"):
                    patterns = google_ads_filters.get("campaignNamePatterns", [])
                    if patterns and not any(p.lower() in (campaign_name or "").lower() for p in patterns):
                        continue
                ad_group_id = str(ad_group.id)
                ad_group_name = ad_group.name if ad_group.name else "Unnamed Ad Group"
                imp = int(getattr(metrics, "impressions", 0))
                clk = int(getattr(metrics, "clicks", 0))
                cost_micros = int(getattr(metrics, "cost_micros", 0))
                cost = cost_micros / 1_000_000.0
                conv = float(getattr(metrics, "conversions", 0))
                cv = float(getattr(metrics, "conversions_value", 0) or getattr(metrics, "all_conversions_value", 0))
                ctr = round((clk / imp) * 100, 2) if imp > 0 else 0.0
                cpc = round(cost / clk, 2) if clk > 0 else 0.0
                roas = round(cv / cost, 4) if cost > 0 and cv > 0 else 0.0
                cpa = round(cost / conv, 2) if conv > 0 else 0.0
                cvr = round((conv / clk) * 100, 2) if clk > 0 else 0.0
                is_share = getattr(metrics, "search_impression_share", None)
                rank_lost = getattr(metrics, "search_rank_lost_impression_share", None)
                impression_share_pct = round(float(is_share) * 100, 2) if is_share is not None else None
                search_rank_lost_pct = round(float(rank_lost) * 100, 2) if rank_lost is not None else None
                rows_out.append({
                    "outcome_date": outcome_date,
                    "ad_group_id": ad_group_id,
                    "ad_group_name": ad_group_name,
                    "campaign_id": campaign_id,
                    "campaign_name": campaign_name,
                    "impressions": imp,
                    "clicks": clk,
                    "cost": cost,
                    "conversions": conv,
                    "conversionValue": cv,
                    "ctr": ctr,
                    "cpc": cpc,
                    "roas": roas,
                    "cpa": cpa,
                    "cvr": cvr,
                    "impressionSharePct": impression_share_pct,
                    "search_impression_share_pct": impression_share_pct,
                    "search_rank_lost_impression_share_pct": search_rank_lost_pct,
                })
        logger.info(
            "fetch_ad_groups_daily: %s rows for %s..%s project %s",
            len(rows_out), start_date, end_date, project,
        )
    except GoogleAdsException as ex:
        logger.error("Google Ads API error: %s", ex)
    return rows_out


def fetch_keywords_daily(
    start_date: str,
    end_date: str,
    project: str,
    google_ads_filters: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Fetch keyword performance (keyword_view) with one row per keyword per day. Same metrics as campaign level."""
    client = get_client()
    customer_id_clean = _customer_id_clean(project)
    ga_service = client.get_service("GoogleAdsService")
    query = f"""
        SELECT ad_group_criterion.criterion_id, ad_group_criterion.keyword.text, ad_group_criterion.keyword.match_type,
               ad_group.id, ad_group.name, campaign.id, campaign.name,
               segments.date,
               metrics.impressions, metrics.clicks, metrics.cost_micros,
               metrics.conversions, metrics.conversions_value, metrics.all_conversions_value,
               metrics.average_cpc, metrics.ctr, metrics.search_impression_share, metrics.search_rank_lost_impression_share
        FROM keyword_view
        WHERE campaign.status != 'REMOVED' AND ad_group.status != 'REMOVED'
          AND segments.date BETWEEN '{start_date}' AND '{end_date}'
    """
    rows_out: List[Dict[str, Any]] = []
    try:
        stream = ga_service.search_stream(customer_id=customer_id_clean, query=query)
        for batch in stream:
            for row in batch.results:
                criterion = row.ad_group_criterion
                ad_group = row.ad_group
                campaign = row.campaign
                metrics = row.metrics
                segment_date = getattr(row.segments, "date", None) if hasattr(row, "segments") else None
                if not segment_date:
                    continue
                outcome_date = str(segment_date).replace("-", "")
                if len(outcome_date) == 8:
                    outcome_date = f"{outcome_date[:4]}-{outcome_date[4:6]}-{outcome_date[6:8]}"
                campaign_id = str(campaign.id)
                campaign_name = campaign.name if campaign.name else ""
                if google_ads_filters and google_ads_filters.get("campaignNamePatterns"):
                    patterns = google_ads_filters.get("campaignNamePatterns", [])
                    if patterns and not any(p.lower() in (campaign_name or "").lower() for p in patterns):
                        continue
                keyword_criterion_id = str(criterion.criterion_id)
                keyword = getattr(criterion, "keyword", None)
                keyword_text = keyword.text if keyword and keyword.text else ""
                match_type = keyword.match_type.name if keyword and hasattr(keyword.match_type, "name") else (str(keyword.match_type) if keyword else "")
                ad_group_id = str(ad_group.id)
                imp = int(getattr(metrics, "impressions", 0))
                clk = int(getattr(metrics, "clicks", 0))
                cost_micros = int(getattr(metrics, "cost_micros", 0))
                cost = cost_micros / 1_000_000.0
                conv = float(getattr(metrics, "conversions", 0))
                cv = float(getattr(metrics, "conversions_value", 0) or getattr(metrics, "all_conversions_value", 0))
                ctr = round((clk / imp) * 100, 2) if imp > 0 else 0.0
                cpc = round(cost / clk, 2) if clk > 0 else 0.0
                roas = round(cv / cost, 4) if cost > 0 and cv > 0 else 0.0
                cpa = round(cost / conv, 2) if conv > 0 else 0.0
                cvr = round((conv / clk) * 100, 2) if clk > 0 else 0.0
                is_share = getattr(metrics, "search_impression_share", None)
                rank_lost = getattr(metrics, "search_rank_lost_impression_share", None)
                impression_share_pct = round(float(is_share) * 100, 2) if is_share is not None else None
                search_rank_lost_pct = round(float(rank_lost) * 100, 2) if rank_lost is not None else None
                rows_out.append({
                    "outcome_date": outcome_date,
                    "keyword_criterion_id": keyword_criterion_id,
                    "keyword_text": keyword_text,
                    "match_type": match_type,
                    "ad_group_id": ad_group_id,
                    "campaign_id": campaign_id,
                    "impressions": imp,
                    "clicks": clk,
                    "cost": cost,
                    "conversions": conv,
                    "conversionValue": cv,
                    "ctr": ctr,
                    "cpc": cpc,
                    "roas": roas,
                    "cpa": cpa,
                    "cvr": cvr,
                    "impressionSharePct": impression_share_pct,
                    "search_impression_share_pct": impression_share_pct,
                    "search_rank_lost_impression_share_pct": search_rank_lost_pct,
                })
        logger.info(
            "fetch_keywords_daily: %s rows for %s..%s project %s",
            len(rows_out), start_date, end_date, project,
        )
    except GoogleAdsException as ex:
        logger.error("Google Ads API error: %s", ex)
    return rows_out
