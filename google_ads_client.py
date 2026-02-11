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


# AdvertisingChannelSubType: UNSPECIFIED/UNKNOWN mean "no sub type" (e.g. standard Search) -> store None
_CHANNEL_SUB_TYPE_EMPTY = frozenset({"UNSPECIFIED", "UNKNOWN", "0", "CHANNEL_SUB_TYPE_UNSPECIFIED", "ADVERTISING_CHANNEL_SUB_TYPE_UNSPECIFIED"})

# Map Google Ads BiddingStrategyType enum to display label (e.g. "Maximize conversion value")
_BIDDING_STRATEGY_TYPE_LABELS = {
    "MAXIMIZE_CONVERSION_VALUE": "Maximize conversion value",
    "MAXIMIZE_CONVERSIONS": "Maximize conversions",
    "TARGET_CPA": "Target CPA",
    "TARGET_ROAS": "Target ROAS",
    "TARGET_IMPRESSION_SHARE": "Target impression share",
    "TARGET_SPEND": "Target spend",
    "MANUAL_CPC": "Manual CPC",
    "MANUAL_CPM": "Manual CPM",
    "MANUAL_CPV": "Manual CPV",
    "ENHANCED_CPC": "Enhanced CPC",
    "COMMISSION": "Commission",
    "UNSPECIFIED": None,
    "UNKNOWN": None,
    "INVALID": None,
}

# Audience criterion type -> display label for active_bid_adj (e.g. "User interest And List")
_AUDIENCE_TYPE_LABELS: Dict[str, str] = {
    "USER_INTEREST": "User interest",
    "USER_LIST": "List",
    "CUSTOM_AFFINITY": "Custom affinity",
    "CUSTOM_INTENT": "Custom intent",
    "COMBINED_AUDIENCE": "Combined audience",
    "CUSTOM_AUDIENCE": "Custom audience",
}
_AUDIENCE_TYPE_ORDER = ("USER_INTEREST", "USER_LIST", "CUSTOM_AFFINITY", "CUSTOM_INTENT", "COMBINED_AUDIENCE", "CUSTOM_AUDIENCE")


def _channel_sub_type_display(enum_val: Any) -> Optional[str]:
    """Return display string for advertising_channel_sub_type, or None when UNSPECIFIED/UNKNOWN (no sub type)."""
    if enum_val is None:
        return None
    name = getattr(enum_val, "name", None) or str(enum_val)
    if name in _CHANNEL_SUB_TYPE_EMPTY:
        return None
    return name.replace("_", " ").title()


def _numeric_value(val: Any, as_float: bool = False) -> Optional[Any]:
    """Extract numeric value from API response (may be proto message with .value or raw number)."""
    if val is None:
        return None
    if isinstance(val, (int, float)) and not isinstance(val, bool):
        return float(val) if as_float else int(val)
    if hasattr(val, "value"):
        return _numeric_value(getattr(val, "value"), as_float)
    if hasattr(val, "target_roas"):
        return _numeric_value(getattr(val, "target_roas"), True)
    if hasattr(val, "target_cpa_micros"):
        return _numeric_value(getattr(val, "target_cpa_micros"), False)
    try:
        return float(val) if as_float else int(val)
    except (TypeError, ValueError):
        return None


def _bidding_strategy_display_name(
    campaign_bidding_strategy_type: Any,
    campaign_bidding_strategy_resource: Any,
    strategy_names_by_resource: Optional[Dict[str, str]] = None,
) -> Optional[str]:
    """Resolve display string for bidding strategy (e.g. 'Maximize conversion value')."""
    strategy_names_by_resource = strategy_names_by_resource or {}
    type_name = getattr(campaign_bidding_strategy_type, "name", None) if campaign_bidding_strategy_type is not None else None
    type_name = type_name or str(campaign_bidding_strategy_type) if campaign_bidding_strategy_type is not None else None
    if type_name and type_name not in ("UNSPECIFIED", "UNKNOWN", "0", "BIDDING_STRATEGY_TYPE_UNSPECIFIED"):
        label = _BIDDING_STRATEGY_TYPE_LABELS.get(type_name)
        if label:
            return label
        return type_name.replace("_", " ").title()
    resource = getattr(campaign_bidding_strategy_resource, "resource_name", None) if campaign_bidding_strategy_resource else None
    resource = resource or str(campaign_bidding_strategy_resource) if campaign_bidding_strategy_resource else None
    if resource and resource in strategy_names_by_resource:
        return strategy_names_by_resource[resource] or None
    return None


def fetch_campaign_control_state(
    project: str,
    google_ads_filters: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Fetch current campaign control state (settings only). One row per campaign."""
    import json as _json

    client = get_client()
    customer_id_clean = _customer_id_clean(project)
    ga_service = client.get_service("GoogleAdsService")
    query_with_targets = """
        SELECT campaign.id, campaign.name, campaign.status, campaign.advertising_channel_type, campaign.advertising_channel_sub_type,
               campaign.bidding_strategy_type, campaign.bidding_strategy,
               campaign.start_date, campaign.end_date,
               campaign.maximize_conversions.target_cpa_micros,
               campaign.target_cpa.target_cpa_micros,
               campaign.maximize_conversion_value.target_roas,
               campaign.target_roas.target_roas,
               campaign.target_impression_share.location,
               campaign.target_impression_share.location_fraction_micros,
               campaign.network_settings.target_google_search,
               campaign.network_settings.target_search_network,
               campaign.network_settings.target_content_network,
               campaign.network_settings.target_partner_search_network,
               campaign_budget.amount_micros, campaign_budget.delivery_method
        FROM campaign
        WHERE campaign.status != 'REMOVED'
    """
    query_base = """
        SELECT campaign.id, campaign.name, campaign.status, campaign.advertising_channel_type, campaign.advertising_channel_sub_type,
               campaign.bidding_strategy_type, campaign.bidding_strategy,
               campaign.start_date, campaign.end_date,
               campaign.network_settings.target_google_search,
               campaign.network_settings.target_search_network,
               campaign.network_settings.target_content_network,
               campaign.network_settings.target_partner_search_network,
               campaign_budget.amount_micros, campaign_budget.delivery_method
        FROM campaign
        WHERE campaign.status != 'REMOVED'
    """
    rows_out = []
    strategy_resource_names: List[str] = []
    query = query_base
    try:
        stream = ga_service.search_stream(customer_id=customer_id_clean, query=query_with_targets)
        for batch in stream:
            for row in batch.results:
                camp = row.campaign
                res = getattr(camp, "bidding_strategy", None)
                resource = getattr(res, "resource_name", None) if res and hasattr(res, "resource_name") else (res if isinstance(res, str) else None)
                if resource:
                    strategy_resource_names.append(str(resource))
        query = query_with_targets
    except GoogleAdsException as e:
        if "UNRECOGNIZED_FIELD" in str(e) or "Unrecognized field" in str(e):
            logger.debug("Campaign-level target CPA/ROAS fields not supported, using base query")
            query = query_base
            stream = ga_service.search_stream(customer_id=customer_id_clean, query=query_base)
            for batch in stream:
                for row in batch.results:
                    camp = row.campaign
                    res = getattr(camp, "bidding_strategy", None)
                    resource = getattr(res, "resource_name", None) if res and hasattr(res, "resource_name") else (res if isinstance(res, str) else None)
                    if resource:
                        strategy_resource_names.append(str(resource))
        else:
            raise
    try:
        stream = ga_service.search_stream(customer_id=customer_id_clean, query=query)
        strategy_names_by_resource: Dict[str, str] = {}
        strategy_target_cpa_by_resource: Dict[str, Optional[int]] = {}
        strategy_target_roas_by_resource: Dict[str, Optional[float]] = {}
        strategy_impression_share_location_by_resource: Dict[str, Optional[str]] = {}
        strategy_impression_share_fraction_micros_by_resource: Dict[str, Optional[int]] = {}
        if strategy_resource_names:
            unique = list(set(strategy_resource_names))
            in_list = ",".join("'" + str(r).replace("'", "''") + "'" for r in unique)
            strat_query_full = f"""
                SELECT bidding_strategy.resource_name, bidding_strategy.name, bidding_strategy.type,
                       bidding_strategy.maximize_conversions.target_cpa_micros,
                       bidding_strategy.target_cpa.target_cpa_micros,
                       bidding_strategy.maximize_conversion_value.target_roas,
                       bidding_strategy.target_roas.target_roas,
                       bidding_strategy.target_impression_share.location,
                       bidding_strategy.target_impression_share.location_fraction_micros
                FROM bidding_strategy
                WHERE bidding_strategy.resource_name IN ({in_list})
            """
            strat_query_minimal = f"""
                SELECT bidding_strategy.resource_name, bidding_strategy.name, bidding_strategy.type
                FROM bidding_strategy
                WHERE bidding_strategy.resource_name IN ({in_list})
            """
            try:
                for strat_batch in ga_service.search_stream(customer_id=customer_id_clean, query=strat_query_full):
                    for srow in strat_batch.results:
                        bs = srow.bidding_strategy
                        rn = getattr(bs, "resource_name", None)
                        name = getattr(bs, "name", None) or ""
                        stype = getattr(bs, "type", None)
                        if rn:
                            rn_str = str(rn)
                            strategy_names_by_resource[rn_str] = (name.strip() or None) or (
                                getattr(stype, "name", None) and _BIDDING_STRATEGY_TYPE_LABELS.get(stype.name)
                            )
                            tcpa = None
                            mc = getattr(bs, "maximize_conversions", None)
                            if mc is not None:
                                tcpa = _numeric_value(getattr(mc, "target_cpa_micros", None), as_float=False)
                            if tcpa is None:
                                tcp = getattr(bs, "target_cpa", None)
                                if tcp is not None:
                                    tcpa = _numeric_value(getattr(tcp, "target_cpa_micros", None), as_float=False)
                            strategy_target_cpa_by_resource[rn_str] = tcpa
                            troas = None
                            mcv = getattr(bs, "maximize_conversion_value", None)
                            if mcv is not None:
                                troas = _numeric_value(getattr(mcv, "target_roas", None), as_float=True)
                            if troas is None:
                                tr = getattr(bs, "target_roas", None)
                                if tr is not None:
                                    troas = _numeric_value(getattr(tr, "target_roas", None), as_float=True)
                            strategy_target_roas_by_resource[rn_str] = troas
                            tis = getattr(bs, "target_impression_share", None)
                            if tis is not None:
                                loc = getattr(tis, "location", None)
                                strategy_impression_share_location_by_resource[rn_str] = (
                                    getattr(loc, "name", None) if loc and hasattr(loc, "name") else (str(loc) if loc else None)
                                )
                                strategy_impression_share_fraction_micros_by_resource[rn_str] = _numeric_value(
                                    getattr(tis, "location_fraction_micros", None), as_float=False
                                )
                            else:
                                strategy_impression_share_location_by_resource[rn_str] = None
                                strategy_impression_share_fraction_micros_by_resource[rn_str] = None
            except GoogleAdsException as e:
                if "UNRECOGNIZED_FIELD" in str(e) or "Unrecognized field" in str(e):
                    logger.debug("Bidding strategy target CPA/ROAS/impression share fields not supported, retrying minimal strategy query")
                    try:
                        for strat_batch in ga_service.search_stream(customer_id=customer_id_clean, query=strat_query_minimal):
                            for srow in strat_batch.results:
                                bs = srow.bidding_strategy
                                rn = getattr(bs, "resource_name", None)
                                name = getattr(bs, "name", None) or ""
                                stype = getattr(bs, "type", None)
                                if rn:
                                    rn_str = str(rn)
                                    strategy_names_by_resource[rn_str] = (name.strip() or None) or (
                                        getattr(stype, "name", None) and _BIDDING_STRATEGY_TYPE_LABELS.get(stype.name)
                                    )
                    except GoogleAdsException as e2:
                        logger.warning("Control state: bidding strategy query failed: %s", e2)
                        strategy_names_by_resource = {}
                        strategy_target_cpa_by_resource = {}
                        strategy_target_roas_by_resource = {}
                        strategy_impression_share_location_by_resource = {}
                        strategy_impression_share_fraction_micros_by_resource = {}
                else:
                    logger.warning("Control state: bidding strategy (target CPA/ROAS) query failed: %s", e)
                    strategy_names_by_resource = {}
                    strategy_target_cpa_by_resource = {}
                    strategy_target_roas_by_resource = {}
                    strategy_impression_share_location_by_resource = {}
                    strategy_impression_share_fraction_micros_by_resource = {}

        geo_by_campaign: Dict[str, List[str]] = {}
        geo_negative_by_campaign: Dict[str, List[str]] = {}
        geo_radius_by_campaign: Dict[str, List[Dict[str, Any]]] = {}
        location_presence_interest_by_campaign: Dict[str, List[Dict[str, Any]]] = {}
        ad_schedule_by_campaign: Dict[str, List[Dict[str, Any]]] = {}
        audience_count_by_campaign: Dict[str, int] = {}
        device_modifiers_by_campaign: Dict[str, Dict[str, float]] = {}
        account_timezone: Optional[str] = None
        try:
            tz_stream = ga_service.search_stream(
                customer_id=customer_id_clean,
                query="SELECT customer.time_zone FROM customer LIMIT 1",
            )
            for tb in tz_stream:
                for trow in tb.results:
                    account_timezone = getattr(trow.customer, "time_zone", None) or ""
                    break
                break
        except GoogleAdsException:
            pass
        try:
            geo_stream = ga_service.search_stream(
                customer_id=customer_id_clean,
                query="SELECT campaign.id, campaign_criterion.criterion_id, campaign_criterion.negative, "
                "campaign_criterion.location.geo_target_constant "
                "FROM campaign_criterion WHERE campaign_criterion.type = 'LOCATION' AND campaign.status != 'REMOVED'",
            )
            for gbatch in geo_stream:
                for grow in gbatch.results:
                    cid = str(grow.campaign.id)
                    neg = getattr(grow.campaign_criterion, "negative", None)
                    loc = getattr(grow.campaign_criterion, "location", None)
                    gt = getattr(loc, "geo_target_constant", None) if loc else None
                    val = str(gt) if gt else (str(getattr(grow.campaign_criterion, "criterion_id", "")) if getattr(grow.campaign_criterion, "criterion_id", None) is not None else None)
                    if val:
                        if neg:
                            geo_negative_by_campaign.setdefault(cid, []).append(val)
                        else:
                            geo_by_campaign.setdefault(cid, []).append(val)
        except GoogleAdsException as e:
            logger.warning("Control state: location (geo) query failed: %s", e)
        try:
            loc_poi_stream = ga_service.search_stream(
                customer_id=customer_id_clean,
                query="SELECT campaign.id, campaign_criterion.location.geo_target_constant, "
                "campaign_criterion.location.presence_or_interest "
                "FROM campaign_criterion WHERE campaign_criterion.type = 'LOCATION' AND campaign_criterion.negative = FALSE AND campaign.status != 'REMOVED'",
            )
            for lbatch in loc_poi_stream:
                for lrow in lbatch.results:
                    cid = str(lrow.campaign.id)
                    loc = getattr(lrow.campaign_criterion, "location", None)
                    if not loc:
                        continue
                    gt = getattr(loc, "geo_target_constant", None)
                    poi = getattr(loc, "presence_or_interest", None)
                    poi_name = poi.name if poi and hasattr(poi, "name") else (str(poi) if poi else None)
                    if gt or poi_name:
                        location_presence_interest_by_campaign.setdefault(cid, []).append({
                            "geo_target_constant": str(gt) if gt else None,
                            "presence_or_interest": poi_name,
                        })
        except GoogleAdsException as e:
            logger.debug("Control state: location presence_or_interest query not available or failed: %s", e)
        try:
            prox_stream = ga_service.search_stream(
                customer_id=customer_id_clean,
                query="SELECT campaign.id, campaign_criterion.proximity.radius, campaign_criterion.proximity.radius_units "
                "FROM campaign_criterion WHERE campaign_criterion.type = 'PROXIMITY' AND campaign.status != 'REMOVED'",
            )
            for pbatch in prox_stream:
                for prow in pbatch.results:
                    cid = str(prow.campaign.id)
                    prox = getattr(prow.campaign_criterion, "proximity", None)
                    if prox:
                        geocode = getattr(prox, "geocode", None) or getattr(prox, "geocode_center", None)
                        center = getattr(geocode, "center", None) if geocode else None
                        entry = {
                            "radius": getattr(prox, "radius", None),
                            "radius_units": getattr(prox, "radius_units", None) and getattr(prox.radius_units, "name", None),
                            "lat_micro": getattr(center, "latitude_micro_degrees", None) or getattr(center, "latitude_in_micro_degrees", None) if center else None,
                            "long_micro": getattr(center, "longitude_micro_degrees", None) or getattr(center, "longitude_in_micro_degrees", None) if center else None,
                        }
                        geo_radius_by_campaign.setdefault(cid, []).append(entry)
        except GoogleAdsException as e:
            logger.warning("Control state: proximity (geo radius) query failed: %s", e)
        try:
            dev_stream = ga_service.search_stream(
                customer_id=customer_id_clean,
                query="SELECT campaign.id, ad_group_criterion.criterion_id, ad_group_criterion.bid_modifier "
                "FROM ad_group_criterion "
                "WHERE ad_group_criterion.type = 'DEVICE' AND campaign.status != 'REMOVED' AND ad_group.status != 'REMOVED'",
            )
            for dbatch in dev_stream:
                for drow in dbatch.results:
                    cid = str(drow.campaign.id)
                    dev_type = getattr(getattr(drow.ad_group_criterion, "device", None), "type", None)
                    dev_name = dev_type.name if dev_type and hasattr(dev_type, "name") else str(dev_type) if dev_type else None
                    if dev_name is None:
                        dev_name = "device_" + str(getattr(drow.ad_group_criterion, "criterion_id", ""))
                    mod = getattr(drow.ad_group_criterion, "bid_modifier", None)
                    mod_val = float(mod) if mod is not None else None
                    if cid not in device_modifiers_by_campaign:
                        device_modifiers_by_campaign[cid] = {}
                    if mod_val is not None and dev_name:
                        device_modifiers_by_campaign[cid][dev_name.lower()] = mod_val
        except GoogleAdsException as e:
            logger.warning("Control state: device modifiers query failed: %s", e)
        try:
            sched_stream = ga_service.search_stream(
                customer_id=customer_id_clean,
                query="SELECT campaign.id, campaign_criterion.ad_schedule.day_of_week, campaign_criterion.ad_schedule.start_hour, "
                "campaign_criterion.ad_schedule.start_minute, campaign_criterion.ad_schedule.end_hour, "
                "campaign_criterion.ad_schedule.end_minute, campaign_criterion.bid_modifier "
                "FROM campaign_criterion WHERE campaign_criterion.type = 'AD_SCHEDULE' AND campaign.status != 'REMOVED'",
            )
            for sbatch in sched_stream:
                for srow in sbatch.results:
                    cid = str(srow.campaign.id)
                    ad = getattr(srow.campaign_criterion, "ad_schedule", None)
                    if ad:
                        entry = {
                            "day_of_week": getattr(ad, "day_of_week", None) and getattr(ad.day_of_week, "name", None),
                            "start_hour": getattr(ad, "start_hour", None),
                            "start_minute": getattr(ad, "start_minute", None),
                            "end_hour": getattr(ad, "end_hour", None),
                            "end_minute": getattr(ad, "end_minute", None),
                            "bid_modifier": getattr(srow.campaign_criterion, "bid_modifier", None),
                        }
                        ad_schedule_by_campaign.setdefault(cid, []).append(entry)
        except GoogleAdsException as e:
            logger.warning("Control state: ad schedule query failed: %s", e)
        audience_types_by_campaign: Dict[str, List[str]] = {}
        try:
            aud_stream = ga_service.search_stream(
                customer_id=customer_id_clean,
                query="SELECT campaign.id, campaign_criterion.type FROM campaign_criterion "
                "WHERE campaign_criterion.type IN ('USER_LIST','USER_INTEREST','CUSTOM_AFFINITY','CUSTOM_INTENT','COMBINED_AUDIENCE','CUSTOM_AUDIENCE') "
                "AND campaign.status != 'REMOVED'",
            )
            for abatch in aud_stream:
                for arow in abatch.results:
                    cid = str(arow.campaign.id)
                    audience_count_by_campaign[cid] = audience_count_by_campaign.get(cid, 0) + 1
                    ctype = getattr(arow.campaign_criterion, "type", None)
                    type_name = getattr(ctype, "name", None) if ctype and hasattr(ctype, "name") else (str(ctype) if ctype else None)
                    if type_name:
                        types_list = audience_types_by_campaign.setdefault(cid, [])
                        if type_name not in types_list:
                            types_list.append(type_name)
        except GoogleAdsException as e:
            logger.warning("Control state: audience count query failed: %s", e)
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
                sub_type = _channel_sub_type_display(getattr(camp, "advertising_channel_sub_type", None))
                if google_ads_filters and google_ads_filters.get("campaignNamePatterns"):
                    patterns = google_ads_filters.get("campaignNamePatterns", [])
                    if patterns and not any(p.lower() in campaign_name.lower() for p in patterns):
                        continue
                daily_budget_micros = int(amount_micros) if amount_micros is not None else None
                daily_budget_amount = (daily_budget_micros / 1_000_000.0) if daily_budget_micros else None
                bidding_type = getattr(camp, "bidding_strategy_type", None)
                bidding_strategy_resource = getattr(camp, "bidding_strategy", None)
                bidding_strategy_type = _bidding_strategy_display_name(
                    bidding_type, bidding_strategy_resource, strategy_names_by_resource
                )
                target_cpa_micros = None
                target_roas = None
                mc = getattr(camp, "maximize_conversions", None)
                if mc is not None:
                    target_cpa_micros = _numeric_value(getattr(mc, "target_cpa_micros", None), as_float=False)
                if target_cpa_micros is None:
                    tcp = getattr(camp, "target_cpa", None)
                    if tcp is not None:
                        target_cpa_micros = _numeric_value(getattr(tcp, "target_cpa_micros", None), as_float=False)
                mcv = getattr(camp, "maximize_conversion_value", None)
                if mcv is not None:
                    target_roas = _numeric_value(getattr(mcv, "target_roas", None), as_float=True)
                if target_roas is None:
                    tr = getattr(camp, "target_roas", None)
                    if tr is not None:
                        target_roas = _numeric_value(getattr(tr, "target_roas", None), as_float=True)
                if target_cpa_micros is None or target_roas is None:
                    if bidding_strategy_resource:
                        strat_rn = getattr(bidding_strategy_resource, "resource_name", None) or str(bidding_strategy_resource)
                        if strat_rn:
                            if target_cpa_micros is None:
                                target_cpa_micros = strategy_target_cpa_by_resource.get(strat_rn)
                            if target_roas is None:
                                target_roas = strategy_target_roas_by_resource.get(strat_rn)
                if bidding_strategy_type == "Maximize conversions" and target_cpa_micros is not None:
                    bidding_strategy_type = "Maximize conversions (Target CPA)"
                elif bidding_strategy_type == "Maximize conversion value" and target_roas is not None:
                    bidding_strategy_type = "Maximize conversion value (Target ROAS)"
                target_cpa_amount = (target_cpa_micros / 1_000_000.0) if target_cpa_micros is not None else None
                target_impression_share_location: Optional[str] = None
                target_impression_share_location_fraction_micros: Optional[int] = None
                tis_camp = getattr(camp, "target_impression_share", None)
                if tis_camp is not None:
                    loc = getattr(tis_camp, "location", None)
                    target_impression_share_location = getattr(loc, "name", None) if loc and hasattr(loc, "name") else (str(loc) if loc else None)
                    target_impression_share_location_fraction_micros = _numeric_value(
                        getattr(tis_camp, "location_fraction_micros", None), as_float=False
                    )
                if target_impression_share_location is None or target_impression_share_location_fraction_micros is None:
                    if bidding_strategy_resource:
                        strat_rn = getattr(bidding_strategy_resource, "resource_name", None) or str(bidding_strategy_resource)
                        if strat_rn:
                            if target_impression_share_location is None:
                                target_impression_share_location = strategy_impression_share_location_by_resource.get(strat_rn)
                            if target_impression_share_location_fraction_micros is None:
                                target_impression_share_location_fraction_micros = strategy_impression_share_fraction_micros_by_resource.get(strat_rn)
                def _bool_val(x: Any) -> Optional[bool]:
                    if x is None:
                        return None
                    if isinstance(x, bool):
                        return x
                    if hasattr(x, "value"):
                        return bool(getattr(x, "value", False))
                    return bool(x)

                ns = getattr(camp, "network_settings", None)
                target_google_search = _bool_val(getattr(ns, "target_google_search", None)) if ns else None
                target_search_network = _bool_val(getattr(ns, "target_search_network", None)) if ns else None
                target_content_network = _bool_val(getattr(ns, "target_content_network", None)) if ns else None
                target_partner_search_network = _bool_val(getattr(ns, "target_partner_search_network", None)) if ns else None
                geo_ids = geo_by_campaign.get(campaign_id)
                geo_target_ids = ",".join(geo_ids) if geo_ids else None
                geo_neg_ids = geo_negative_by_campaign.get(campaign_id)
                geo_negative_ids = ",".join(geo_neg_ids) if geo_neg_ids else None
                geo_radius_list = geo_radius_by_campaign.get(campaign_id)
                geo_radius_json = _json.dumps(geo_radius_list) if geo_radius_list else None
                sched_list = ad_schedule_by_campaign.get(campaign_id)
                ad_schedule_json = _json.dumps(sched_list) if sched_list else None
                audience_target_count = audience_count_by_campaign.get(campaign_id)
                dev_mods = device_modifiers_by_campaign.get(campaign_id)
                device_modifiers_json = _json.dumps(dev_mods) if dev_mods else None
                loc_poi_list = location_presence_interest_by_campaign.get(campaign_id)
                location_presence_interest_json = (_json.dumps(loc_poi_list)[:4096] if loc_poi_list else None)
                def _date_str(ymd: Any) -> Optional[str]:
                    if ymd is None:
                        return None
                    s = str(ymd).strip()
                    if len(s) >= 8 and s.isdigit():
                        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
                    return s if s else None
                campaign_start_date = _date_str(getattr(camp, "start_date", None))
                campaign_end_date = _date_str(getattr(camp, "end_date", None))
                campaign_type_parts = [channel_type or "", sub_type or ""]
                campaign_type = " ".join(p for p in campaign_type_parts if p).strip() or (channel_type or None)
                network_parts = []
                if target_google_search:
                    network_parts.append("Search")
                if target_search_network:
                    network_parts.append("Search Partners")
                if target_content_network:
                    network_parts.append("Display")
                if target_partner_search_network:
                    network_parts.append("Partner Search")
                networks = ", ".join(network_parts) if network_parts else None
                location_summary = geo_target_ids[:4096] if geo_target_ids else None
                aud_types = audience_types_by_campaign.get(campaign_id) or []
                active_bid_adj_parts = []
                for t in _AUDIENCE_TYPE_ORDER:
                    if t in aud_types:
                        active_bid_adj_parts.append(_AUDIENCE_TYPE_LABELS.get(t, t.replace("_", " ").title()))
                active_bid_adj = " And ".join(active_bid_adj_parts) if active_bid_adj_parts else None
                devices_str = None
                if dev_mods:
                    devices_str = ", ".join(k for k in (dev_mods or {}).keys())[:512] if dev_mods else None
                rows_out.append({
                    "campaign_id": campaign_id, "campaign_name": campaign_name, "status": status,
                    "advertising_channel_type": channel_type, "advertising_channel_sub_type": sub_type,
                    "daily_budget_micros": daily_budget_micros, "daily_budget_amount": daily_budget_amount,
                    "budget_delivery_method": delivery_method,
                    "bidding_strategy_type": bidding_strategy_type,
                    "target_cpa_micros": int(target_cpa_micros) if target_cpa_micros is not None else None,
                    "target_cpa_amount": target_cpa_amount,
                    "target_roas": float(target_roas) if target_roas is not None else None,
                    "target_impression_share_location": (target_impression_share_location[:32] if target_impression_share_location and len(target_impression_share_location) > 32 else target_impression_share_location) or None,
                    "target_impression_share_location_fraction_micros": int(target_impression_share_location_fraction_micros) if target_impression_share_location_fraction_micros is not None else None,
                    "geo_target_ids": geo_target_ids[:4096] if geo_target_ids and len(geo_target_ids) > 4096 else geo_target_ids,
                    "geo_negative_ids": geo_negative_ids[:4096] if geo_negative_ids and len(geo_negative_ids) > 4096 else geo_negative_ids,
                    "geo_radius_json": geo_radius_json[:65535] if geo_radius_json and len(geo_radius_json) > 65535 else geo_radius_json,
                    "location_presence_interest_json": location_presence_interest_json,
                    "account_timezone": account_timezone,
                    "device_modifiers_json": device_modifiers_json[:4096] if device_modifiers_json and len(device_modifiers_json) > 4096 else device_modifiers_json,
                    "network_settings_target_google_search": target_google_search,
                    "network_settings_target_search_network": target_search_network,
                    "network_settings_target_content_network": target_content_network,
                    "network_settings_target_partner_search_network": target_partner_search_network,
                    "ad_schedule_json": ad_schedule_json[:65535] if ad_schedule_json and len(ad_schedule_json) > 65535 else ad_schedule_json,
                    "audience_target_count": audience_target_count,
                    "campaign_type": campaign_type[:128] if campaign_type and len(campaign_type) > 128 else campaign_type,
                    "networks": networks[:256] if networks and len(networks) > 256 else networks,
                    "campaign_start_date": campaign_start_date,
                    "campaign_end_date": campaign_end_date,
                    "location": location_summary,
                    "active_bid_adj": (active_bid_adj[:256] if active_bid_adj and len(active_bid_adj) > 256 else active_bid_adj) or None,
                    "devices": devices_str,
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
        WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
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


def fetch_ad_group_structure_snapshot(
    project: str,
    google_ads_filters: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Fetch current ad group structure (id, name, status) for add/remove/rename/status change detection. TIER 2.
    Includes all ad groups (including REMOVED) so count matches UI. Optional campaignNamePatterns in google_ads_filters restricts by campaign name.
    """
    client = get_client()
    customer_id_clean = _customer_id_clean(project)
    ga_service = client.get_service("GoogleAdsService")
    query = """
        SELECT ad_group.id, ad_group.name, ad_group.status, campaign.id, campaign.name
        FROM ad_group
    """
    rows_out: List[Dict[str, Any]] = []
    try:
        stream = ga_service.search_stream(customer_id=customer_id_clean, query=query)
        for batch in stream:
            for row in batch.results:
                ad_grp = row.ad_group
                camp = row.campaign
                campaign_id = str(camp.id)
                if google_ads_filters and google_ads_filters.get("campaignNamePatterns"):
                    camp_name = getattr(camp, "name", None) or ""
                    patterns = google_ads_filters.get("campaignNamePatterns", [])
                    if patterns and not any(p.lower() in (camp_name or "").lower() for p in patterns):
                        continue
                status = ad_grp.status.name if hasattr(ad_grp.status, "name") else str(ad_grp.status)
                rows_out.append({
                    "ad_group_id": str(ad_grp.id),
                    "campaign_id": campaign_id,
                    "ad_group_name": ad_grp.name if ad_grp.name else "",
                    "status": status,
                })
        logger.info("fetch_ad_group_structure_snapshot: %s ad groups for project %s", len(rows_out), project)
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
        WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
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


def fetch_keyword_criteria_snapshot(
    project: str,
    google_ads_filters: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Fetch current keyword criteria (structure only) for add/remove/match-type change detection. TIER 2."""
    client = get_client()
    customer_id_clean = _customer_id_clean(project)
    ga_service = client.get_service("GoogleAdsService")
    query = """
        SELECT ad_group_criterion.criterion_id, ad_group_criterion.keyword.text, ad_group_criterion.keyword.match_type,
               ad_group.id, ad_group.name, campaign.id, campaign.name
        FROM ad_group_criterion
        WHERE ad_group_criterion.type = 'KEYWORD'
    """
    rows_out: List[Dict[str, Any]] = []
    try:
        stream = ga_service.search_stream(customer_id=customer_id_clean, query=query)
        for batch in stream:
            for row in batch.results:
                c = row.ad_group_criterion
                ad_group = row.ad_group
                campaign = row.campaign
                campaign_id = str(campaign.id)
                if google_ads_filters and google_ads_filters.get("campaignNamePatterns"):
                    camp_name = getattr(campaign, "name", None) or ""
                    patterns = google_ads_filters.get("campaignNamePatterns", [])
                    if patterns and not any(p.lower() in (camp_name or "").lower() for p in patterns):
                        continue
                kw = getattr(c, "keyword", None)
                keyword_text = kw.text if kw and kw.text else ""
                match_type = kw.match_type.name if kw and hasattr(kw.match_type, "name") else (str(kw.match_type) if kw else "")
                campaign_name = (getattr(campaign, "name", None) or "").strip() or None
                ad_group_name = (getattr(ad_group, "name", None) or "").strip() or None
                rows_out.append({
                    "keyword_criterion_id": str(c.criterion_id),
                    "ad_group_id": str(ad_group.id),
                    "campaign_id": campaign_id,
                    "keyword_text": keyword_text,
                    "match_type": match_type,
                    "keyword_level": "AD_GROUP",
                    "campaign_name": campaign_name,
                    "ad_group_name": ad_group_name,
                })
        logger.info("fetch_keyword_criteria_snapshot: %s keywords for project %s", len(rows_out), project)
    except GoogleAdsException as ex:
        logger.error("Google Ads API error: %s", ex)
    return rows_out


def fetch_negative_keywords_snapshot(
    project: str,
    google_ads_filters: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Fetch campaign- and ad group-level negative keywords. TIER 2."""
    client = get_client()
    customer_id_clean = _customer_id_clean(project)
    ga_service = client.get_service("GoogleAdsService")
    rows_out: List[Dict[str, Any]] = []
    try:
        q_campaign = """
            SELECT campaign.id, campaign.name, campaign_criterion.criterion_id,
                   campaign_criterion.negative,
                   campaign_criterion.keyword.text, campaign_criterion.keyword.match_type
            FROM campaign_criterion
            WHERE campaign_criterion.type = 'KEYWORD'
              AND campaign_criterion.negative = TRUE
        """
        stream = ga_service.search_stream(customer_id=customer_id_clean, query=q_campaign)
        for batch in stream:
            for row in batch.results:
                camp = row.campaign
                c = row.campaign_criterion
                campaign_id = str(camp.id)
                if google_ads_filters and google_ads_filters.get("campaignNamePatterns"):
                    camp_name = getattr(camp, "name", None) or ""
                    patterns = google_ads_filters.get("campaignNamePatterns", [])
                    if patterns and not any(p.lower() in (camp_name or "").lower() for p in patterns):
                        continue
                kw = getattr(c, "keyword", None)
                keyword_text = kw.text if kw and kw.text else ""
                match_type = kw.match_type.name if kw and hasattr(kw.match_type, "name") else (str(kw.match_type) if kw else "")
                campaign_name = (getattr(camp, "name", None) or "").strip() or None
                rows_out.append({
                    "campaign_id": campaign_id,
                    "ad_group_id": "",
                    "criterion_id": str(c.criterion_id),
                    "keyword_text": keyword_text,
                    "match_type": match_type,
                    "keyword_level": "CAMPAIGN",
                    "campaign_name": campaign_name,
                    "ad_group_name": None,
                })
        q_ad_group = """
            SELECT campaign.id, campaign.name, ad_group.id, ad_group.name,
                   ad_group_criterion.criterion_id, ad_group_criterion.keyword.text, ad_group_criterion.keyword.match_type
            FROM ad_group_criterion
            WHERE ad_group_criterion.type = 'KEYWORD'
              AND ad_group_criterion.negative = TRUE
        """
        stream = ga_service.search_stream(customer_id=customer_id_clean, query=q_ad_group)
        for batch in stream:
            for row in batch.results:
                camp = row.campaign
                ad_grp = row.ad_group
                c = row.ad_group_criterion
                campaign_id = str(camp.id)
                if google_ads_filters and google_ads_filters.get("campaignNamePatterns"):
                    camp_name = getattr(camp, "name", None) or ""
                    patterns = google_ads_filters.get("campaignNamePatterns", [])
                    if patterns and not any(p.lower() in (camp_name or "").lower() for p in patterns):
                        continue
                kw = getattr(c, "keyword", None)
                keyword_text = kw.text if kw and kw.text else ""
                match_type = kw.match_type.name if kw and hasattr(kw.match_type, "name") else (str(kw.match_type) if kw else "")
                campaign_name = (getattr(camp, "name", None) or "").strip() or None
                ad_group_name = (getattr(ad_grp, "name", None) or "").strip() or None
                rows_out.append({
                    "campaign_id": campaign_id,
                    "ad_group_id": str(ad_grp.id),
                    "criterion_id": str(c.criterion_id),
                    "keyword_text": keyword_text,
                    "match_type": match_type,
                    "keyword_level": "AD_GROUP",
                    "campaign_name": campaign_name,
                    "ad_group_name": ad_group_name,
                })
        logger.info("fetch_negative_keywords_snapshot: %s negative keywords for project %s", len(rows_out), project)
    except GoogleAdsException as ex:
        logger.error("Google Ads API error: %s", ex)
    return rows_out


def fetch_ad_creative_snapshot(
    project: str,
    google_ads_filters: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Fetch all ad types (RSA, ETA, call, app, etc.) creative snapshot. TIER 2."""
    import json as _json

    client = get_client()
    customer_id_clean = _customer_id_clean(project)
    ga_service = client.get_service("GoogleAdsService")
    query = """
        SELECT ad_group.id, campaign.id, campaign.name, ad_group_ad.status,
               ad_group_ad.ad.id, ad_group_ad.ad.type,
               ad_group_ad.ad.final_urls,
               ad_group_ad.ad.responsive_search_ad.headlines,
               ad_group_ad.ad.responsive_search_ad.descriptions,
               ad_group_ad.ad.responsive_search_ad.path1,
               ad_group_ad.ad.responsive_search_ad.path2,
               ad_group_ad.ad.expanded_text_ad.headline_part1,
               ad_group_ad.ad.expanded_text_ad.headline_part2,
               ad_group_ad.ad.expanded_text_ad.description,
               ad_group_ad.policy_summary.approval_status,
               ad_group_ad.policy_summary.review_status,
               ad_group_ad.policy_summary.policy_topic_entries
        FROM ad_group_ad
    """
    rows_out: List[Dict[str, Any]] = []
    try:
        stream = ga_service.search_stream(customer_id=customer_id_clean, query=query)
        for batch in stream:
            for row in batch.results:
                ad_grp = row.ad_group
                camp = row.campaign
                ad = row.ad_group_ad
                campaign_id = str(camp.id)
                ad_status = getattr(ad, "status", None)
                status = getattr(ad_status, "name", None) if ad_status and hasattr(ad_status, "name") else (str(ad_status) if ad_status else None)
                if google_ads_filters and google_ads_filters.get("campaignNamePatterns"):
                    camp_name = getattr(camp, "name", None) or ""
                    patterns = google_ads_filters.get("campaignNamePatterns", [])
                    if patterns and not any(p.lower() in (camp_name or "").lower() for p in patterns):
                        continue
                ad_res = getattr(ad.ad, "responsive_search_ad", None)
                ad_eta = getattr(ad.ad, "expanded_text_ad", None)
                headlines = []
                descriptions = []
                if ad_res:
                    for h in getattr(ad_res, "headlines", []) or []:
                        text_val = getattr(h, "text", None)
                        if text_val is None:
                            asset = getattr(h, "asset", None)
                            text_val = getattr(asset, "text", None) if asset else None
                        pinned = getattr(h, "pinned_field", None)
                        pinned_name = getattr(pinned, "name", None) if pinned else None
                        headlines.append({"text": str(text_val) if text_val else "", "pinned_field": pinned_name})
                    for d in getattr(ad_res, "descriptions", []) or []:
                        text_val = getattr(d, "text", None)
                        if text_val is None:
                            asset = getattr(d, "asset", None)
                            text_val = getattr(asset, "text", None) if asset else None
                        pinned = getattr(d, "pinned_field", None)
                        pinned_name = getattr(pinned, "name", None) if pinned else None
                        descriptions.append({"text": str(text_val) if text_val else "", "pinned_field": pinned_name})
                elif ad_eta:
                    part1 = getattr(ad_eta, "headline_part1", None)
                    part2 = getattr(ad_eta, "headline_part2", None)
                    if part1:
                        headlines.append({"text": str(part1), "pinned_field": None})
                    if part2:
                        headlines.append({"text": str(part2), "pinned_field": None})
                    desc = getattr(ad_eta, "description", None)
                    if desc:
                        descriptions.append({"text": str(desc), "pinned_field": None})
                headlines_json = _json.dumps(headlines) if headlines else None
                descriptions_json = _json.dumps(descriptions) if descriptions else None
                final_urls = ",".join(getattr(ad.ad, "final_urls", []) or []) if getattr(ad.ad, "final_urls", None) else None
                path1 = getattr(ad_res, "path1", None) if ad_res else None
                path2 = getattr(ad_res, "path2", None) if ad_res else None
                policy_summary = getattr(ad, "policy_summary", None)
                policy_summary_json = None
                if policy_summary:
                    entries = []
                    for pt in getattr(policy_summary, "policy_topic_entries", []) or []:
                        topic = getattr(pt, "topic", None)
                        etype = getattr(pt, "type", None)
                        entries.append({
                            "topic": str(topic) if topic else None,
                            "type": etype.name if etype and hasattr(etype, "name") else str(etype) if etype else None,
                        })
                    policy_summary_json = _json.dumps({
                        "approval_status": getattr(policy_summary, "approval_status", None) and getattr(policy_summary.approval_status, "name", None),
                        "review_status": getattr(policy_summary, "review_status", None) and getattr(policy_summary.review_status, "name", None),
                        "policy_topic_entries": entries,
                    })
                rows_out.append({
                    "ad_group_id": str(ad_grp.id),
                    "campaign_id": campaign_id,
                    "ad_id": str(ad.ad.id),
                    "ad_type": ad.ad.type.name if hasattr(ad.ad.type, "name") else str(ad.ad.type),
                    "status": (status[:32] if status and len(status) > 32 else status) or None,
                    "headlines_json": (headlines_json or "")[:65535] if headlines_json else None,
                    "descriptions_json": (descriptions_json or "")[:65535] if descriptions_json else None,
                    "final_urls": (final_urls or "")[:65535] if final_urls else None,
                    "path1": str(path1)[:512] if path1 else None,
                    "path2": str(path2)[:512] if path2 else None,
                    "policy_summary_json": (policy_summary_json or "")[:65535] if policy_summary_json else None,
                })
        logger.info("fetch_ad_creative_snapshot: %s ads (all types) for project %s", len(rows_out), project)
    except GoogleAdsException as ex:
        logger.error("Google Ads API error: %s", ex)
    return rows_out


def _extract_audience_info(criterion, audience_type: str) -> tuple:
    """Extract audience_id and audience_name from criterion by type."""
    aud_id, aud_name = None, None
    if audience_type == "USER_LIST":
        ul = getattr(criterion, "user_list", None)
        if ul:
            aud_id = getattr(ul, "user_list", None) or getattr(ul, "resource_name", None)
            aud_id = str(aud_id) if aud_id else None
    elif audience_type == "USER_INTEREST":
        ui = getattr(criterion, "user_interest", None)
        if ui:
            aud_id = getattr(ui, "user_interest_category", None) or getattr(ui, "resource_name", None)
            aud_id = str(aud_id) if aud_id else None
    elif audience_type in ("CUSTOM_AFFINITY", "CUSTOM_INTENT"):
        ca = getattr(criterion, "custom_affinity", None) or getattr(criterion, "custom_intent", None)
        if ca:
            aud_id = getattr(ca, "custom_affinity", None) or getattr(ca, "custom_intent", None) or getattr(ca, "resource_name", None)
            aud_id = str(aud_id) if aud_id else None
    elif audience_type == "CUSTOM_AUDIENCE":
        ca = getattr(criterion, "custom_audience", None)
        if ca:
            aud_id = getattr(ca, "custom_audience", None) or getattr(ca, "resource_name", None)
            aud_id = str(aud_id) if aud_id else None
    elif audience_type == "COMBINED_AUDIENCE":
        ca = getattr(criterion, "combined_audience", None)
        if ca:
            aud_id = getattr(ca, "combined_audience", None) or getattr(ca, "resource_name", None)
            aud_id = str(aud_id) if aud_id else None
    return aud_id, aud_name


def fetch_audience_targeting_snapshot(
    project: str,
    google_ads_filters: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Fetch audience targeting snapshot (campaign + ad group level). In-market, custom intent, remarketing; observe vs target. TIER 2."""
    client = get_client()
    customer_id_clean = _customer_id_clean(project)
    ga_service = client.get_service("GoogleAdsService")
    audience_types = ("USER_LIST", "USER_INTEREST", "CUSTOM_AFFINITY", "CUSTOM_INTENT", "COMBINED_AUDIENCE", "CUSTOM_AUDIENCE")
    rows_out: List[Dict[str, Any]] = []

    def process_criterion(c, camp, ad_group_id: str, campaign_id: str, camp_name: str) -> None:
        if google_ads_filters and google_ads_filters.get("campaignNamePatterns"):
            patterns = google_ads_filters.get("campaignNamePatterns", [])
            if patterns and not any(p.lower() in (camp_name or "").lower() for p in patterns):
                return
        audience_type = c.type.name if hasattr(c.type, "name") else str(c.type)
        if audience_type not in audience_types:
            return
        aud_id, aud_name = _extract_audience_info(c, audience_type)
        bid_mod = getattr(c, "bid_modifier", None)
        if bid_mod is not None:
            bid_mod = float(bid_mod)
        neg = getattr(c, "negative", None)
        targeting_mode = None
        rows_out.append({
            "campaign_id": campaign_id,
            "ad_group_id": ad_group_id,
            "criterion_id": str(c.criterion_id),
            "audience_type": audience_type,
            "audience_id": aud_id[:256] if aud_id and len(aud_id) > 256 else aud_id,
            "audience_name": aud_name[:512] if aud_name and len(aud_name) > 512 else aud_name,
            "targeting_mode": targeting_mode,
            "bid_modifier": bid_mod,
            "negative": bool(neg) if neg is not None else False,
        })

    try:
        q_campaign = """
            SELECT campaign.id, campaign.name, campaign_criterion.criterion_id, campaign_criterion.type,
                   campaign_criterion.bid_modifier, campaign_criterion.negative,
                   campaign_criterion.user_list.user_list, campaign_criterion.user_interest.user_interest_category,
                   campaign_criterion.combined_audience.combined_audience
            FROM campaign_criterion
            WHERE campaign_criterion.type IN ('USER_LIST', 'USER_INTEREST', 'CUSTOM_AFFINITY', 'CUSTOM_INTENT', 'COMBINED_AUDIENCE', 'CUSTOM_AUDIENCE')
              AND campaign_criterion.status != 'REMOVED'
              AND campaign.status != 'REMOVED'
        """
        stream = ga_service.search_stream(customer_id=customer_id_clean, query=q_campaign)
        for batch in stream:
            for row in batch.results:
                c = row.campaign_criterion
                camp = row.campaign
                process_criterion(c, camp, "", str(camp.id), getattr(camp, "name", None) or "")

        q_ad_group = """
            SELECT campaign.id, campaign.name, ad_group.id, ad_group_criterion.criterion_id, ad_group_criterion.type,
                   ad_group_criterion.bid_modifier, ad_group_criterion.negative,
                   ad_group_criterion.user_list.user_list, ad_group_criterion.user_interest.user_interest_category,
                   ad_group_criterion.combined_audience.combined_audience
            FROM ad_group_criterion
            WHERE ad_group_criterion.type IN ('USER_LIST', 'USER_INTEREST', 'CUSTOM_AFFINITY', 'CUSTOM_INTENT', 'COMBINED_AUDIENCE', 'CUSTOM_AUDIENCE')
              AND ad_group_criterion.status != 'REMOVED'
              AND campaign.status != 'REMOVED'
              AND ad_group.status != 'REMOVED'
        """
        stream = ga_service.search_stream(customer_id=customer_id_clean, query=q_ad_group)
        for batch in stream:
            for row in batch.results:
                c = row.ad_group_criterion
                ad_grp = row.ad_group
                camp = row.campaign
                process_criterion(c, camp, str(ad_grp.id), str(camp.id), getattr(camp, "name", None) or "")

        logger.info("fetch_audience_targeting_snapshot: %s audience criteria for project %s", len(rows_out), project)
    except GoogleAdsException as ex:
        logger.error("Google Ads API error: %s", ex)
    return rows_out
