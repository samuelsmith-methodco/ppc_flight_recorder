"""
PPC Flight Recorder – Google Ads API client (standalone).
"""

import json as _json
import logging
from typing import Any, Dict, List, Optional

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

try:
    from google.protobuf.json_format import MessageToDict
except ImportError:
    MessageToDict = None  # type: ignore[misc, assignment]

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


def _safe_str(v: Any, max_len: int = 65535) -> Optional[str]:
    if v is None:
        return None
    s = str(v)
    return s[:max_len] if len(s) > max_len else s


def _change_resource_to_str(msg: Any, max_len: int = 65535) -> Optional[str]:
    """Serialize old_resource/new_resource proto to string (JSON if available, else str)."""
    if msg is None:
        return None
    try:
        if MessageToDict is not None and hasattr(msg, "DESCRIPTOR"):
            d = MessageToDict(msg, preserving_proto_field_name=True)
            s = _json.dumps(d, default=str)
        else:
            s = str(msg)
    except Exception:
        s = str(msg)
    return s[:max_len] if len(s) > max_len else s


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
) -> tuple:
    """Fetch current campaign control state (settings only). One row per campaign.
    Returns (control_state_rows, geo_targeting_rows) for ppc_campaign_control_state_daily and ppc_campaign_geo_targeting_daily.
    """
    import json as _json

    client = get_client()
    customer_id_clean = _customer_id_clean(project)
    ga_service = client.get_service("GoogleAdsService")
    # v23 uses campaign.start_date_time and campaign.end_date_time (format "yyyy-MM-dd HH:mm:ss"); older API used campaign.start_date/end_date.
    query_with_targets = """
        SELECT campaign.id, campaign.name, campaign.status, campaign.advertising_channel_type, campaign.advertising_channel_sub_type,
               campaign.bidding_strategy_type, campaign.bidding_strategy,
               campaign.start_date_time, campaign.end_date_time,
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
               campaign.start_date_time, campaign.end_date_time,
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

        geo_location_rows: List[Dict[str, Any]] = []
        geo_proximity_rows: List[Dict[str, Any]] = []
        ad_schedule_by_campaign: Dict[str, List[Dict[str, Any]]] = {}
        audience_count_by_campaign: Dict[str, int] = {}
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
            # v23: Core location targeting – campaign + criterion + geo_target_type_setting (Presence vs Interest).
            loc_stream = ga_service.search_stream(
                customer_id=customer_id_clean,
                query="SELECT campaign.id, campaign.name, "
                "campaign.geo_target_type_setting.positive_geo_target_type, campaign.geo_target_type_setting.negative_geo_target_type, "
                "campaign_criterion.criterion_id, campaign_criterion.negative, campaign_criterion.location.geo_target_constant "
                "FROM campaign_criterion WHERE campaign_criterion.type = 'LOCATION' AND campaign.status != 'REMOVED'",
            )
            for gbatch in loc_stream:
                for grow in gbatch.results:
                    cid = str(grow.campaign.id)
                    crit_id = getattr(grow.campaign_criterion, "criterion_id", None)
                    if crit_id is None:
                        continue
                    loc = getattr(grow.campaign_criterion, "location", None)
                    gt = getattr(loc, "geo_target_constant", None) if loc else None
                    pos_type = getattr(grow.campaign, "geo_target_type_setting", None)
                    pos_enum = getattr(pos_type, "positive_geo_target_type", None) if pos_type else None
                    neg_enum = getattr(pos_type, "negative_geo_target_type", None) if pos_type else None
                    pos_str = pos_enum.name if pos_enum and hasattr(pos_enum, "name") else (str(pos_enum) if pos_enum else None)
                    neg_str = neg_enum.name if neg_enum and hasattr(neg_enum, "name") else (str(neg_enum) if neg_enum else None)
                    geo_location_rows.append({
                        "campaign_id": cid,
                        "campaign_name": getattr(grow.campaign, "name", None) or "",
                        "criterion_id": str(crit_id),
                        "criterion_type": "LOCATION",
                        "geo_target_constant": str(gt) if gt else None,
                        "negative": bool(getattr(grow.campaign_criterion, "negative", False)),
                        "positive_geo_target_type": pos_str,
                        "negative_geo_target_type": neg_str,
                    })
        except GoogleAdsException as e:
            logger.warning("Control state: location (geo) query failed: %s", e)
        def _consume_proximity_stream(stream, rows_out):
            for pbatch in stream:
                for prow in pbatch.results:
                    prox = getattr(prow.campaign_criterion, "proximity", None)
                    if not prox:
                        continue
                    cid = str(prow.campaign.id)
                    crit_id = getattr(prow.campaign_criterion, "criterion_id", None)
                    if crit_id is None:
                        continue
                    ru = getattr(prox, "radius_units", None)
                    ru_str = getattr(ru, "name", None) if ru is not None else None
                    if ru_str is None and ru is not None:
                        ru_str = str(ru)
                    addr = getattr(prox, "address", None)
                    street = getattr(addr, "street_address", None) if addr else None
                    city = getattr(addr, "city_name", None) if addr else None
                    gp = getattr(prox, "geo_point", None)
                    lat_micro = None
                    lng_micro = None
                    if gp is not None:
                        lat_micro = getattr(gp, "latitude_in_micro_degrees", None) or getattr(gp, "latitude_micros", None)
                        lng_micro = getattr(gp, "longitude_in_micro_degrees", None) or getattr(gp, "longitude_micros", None)
                        if lat_micro is not None:
                            lat_micro = int(lat_micro)
                        if lng_micro is not None:
                            lng_micro = int(lng_micro)
                    rows_out.append({
                        "campaign_id": cid,
                        "campaign_name": getattr(prow.campaign, "name", None) or "",
                        "criterion_id": str(crit_id),
                        "criterion_type": "PROXIMITY",
                        "radius": getattr(prox, "radius", None),
                        "radius_units": ru_str,
                        "proximity_street_address": str(street)[:1024] if street else None,
                        "proximity_city_name": str(city)[:256] if city else None,
                        "latitude_micro": lat_micro,
                        "longitude_micro": lng_micro,
                    })

        try:
            # v23: Proximity – radius, radius_units, geo_point (lat/long in micro-degrees); address when selectable. Fallbacks if fields not available.
            prox_query_full = (
                "SELECT campaign.id, campaign.name, campaign_criterion.criterion_id, "
                "campaign_criterion.proximity.radius, campaign_criterion.proximity.radius_units, "
                "campaign_criterion.proximity.geo_point.latitude_in_micro_degrees, campaign_criterion.proximity.geo_point.longitude_in_micro_degrees, "
                "campaign_criterion.proximity.address.street_address, campaign_criterion.proximity.address.city_name "
                "FROM campaign_criterion WHERE campaign_criterion.type = 'PROXIMITY' AND campaign.status != 'REMOVED'"
            )
            prox_query_no_addr = (
                "SELECT campaign.id, campaign.name, campaign_criterion.criterion_id, "
                "campaign_criterion.proximity.radius, campaign_criterion.proximity.radius_units, "
                "campaign_criterion.proximity.geo_point.latitude_in_micro_degrees, campaign_criterion.proximity.geo_point.longitude_in_micro_degrees "
                "FROM campaign_criterion WHERE campaign_criterion.type = 'PROXIMITY' AND campaign.status != 'REMOVED'"
            )
            prox_query_min = (
                "SELECT campaign.id, campaign.name, campaign_criterion.criterion_id, "
                "campaign_criterion.proximity.radius, campaign_criterion.proximity.radius_units "
                "FROM campaign_criterion WHERE campaign_criterion.type = 'PROXIMITY' AND campaign.status != 'REMOVED'"
            )
            try:
                prox_stream = ga_service.search_stream(customer_id=customer_id_clean, query=prox_query_full)
                _consume_proximity_stream(prox_stream, geo_proximity_rows)
            except GoogleAdsException:
                try:
                    prox_stream = ga_service.search_stream(customer_id=customer_id_clean, query=prox_query_no_addr)
                    _consume_proximity_stream(prox_stream, geo_proximity_rows)
                except GoogleAdsException:
                    prox_stream = ga_service.search_stream(customer_id=customer_id_clean, query=prox_query_min)
                    _consume_proximity_stream(prox_stream, geo_proximity_rows)
        except GoogleAdsException as e:
            logger.warning("Control state: proximity (geo radius) query failed: %s", e)
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
                loc_includes = [r["geo_target_constant"] for r in geo_location_rows if r["campaign_id"] == campaign_id and not r.get("negative") and r.get("geo_target_constant")]
                loc_excludes = [r["geo_target_constant"] for r in geo_location_rows if r["campaign_id"] == campaign_id and r.get("negative") and r.get("geo_target_constant")]
                geo_target_ids = ",".join(loc_includes) if loc_includes else None
                geo_negative_ids = ",".join(loc_excludes) if loc_excludes else None
                prox_list = [{"radius": r.get("radius"), "radius_units": r.get("radius_units")} for r in geo_proximity_rows if r["campaign_id"] == campaign_id]
                geo_radius_json = _json.dumps(prox_list) if prox_list else None
                sched_list = ad_schedule_by_campaign.get(campaign_id)
                ad_schedule_json = _json.dumps(sched_list) if sched_list else None
                audience_target_count = audience_count_by_campaign.get(campaign_id)
                def _date_str(ymd: Any) -> Optional[str]:
                    if ymd is None:
                        return None
                    s = str(ymd).strip()
                    if len(s) >= 8 and s.isdigit():
                        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
                    return s if s else None
                def _campaign_date_str(val: Any) -> Optional[str]:
                    if val is None:
                        return None
                    s = str(val).strip()
                    if len(s) >= 10 and s[4:5] == "-":
                        return s[:10]
                    return _date_str(val)
                campaign_start_date = _campaign_date_str(getattr(camp, "start_date_time", None)) or _date_str(getattr(camp, "start_date", None))
                campaign_end_date = _campaign_date_str(getattr(camp, "end_date_time", None)) or _date_str(getattr(camp, "end_date", None))
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
                    "account_timezone": account_timezone,
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
                })
        # Fetch reach + geo name for LOCATION via GeoTargetConstantService.SuggestGeoTargetConstants.
        reach_by_constant: Dict[str, Optional[int]] = {}
        name_by_constant: Dict[str, Optional[str]] = {}
        unique_gt = {r["geo_target_constant"] for r in geo_location_rows if r.get("geo_target_constant")}
        if unique_gt:
            try:
                geo_constant_service = client.get_service("GeoTargetConstantService")
                request = client.get_type("SuggestGeoTargetConstantsRequest")
                request.locale = "en"
                request.geo_targets.geo_target_constants.extend(sorted(unique_gt))
                response = geo_constant_service.suggest_geo_target_constants(request=request)
                for suggestion in getattr(response, "geo_target_constant_suggestions", []) or []:
                    gtc = getattr(suggestion, "geo_target_constant", None)
                    resource_name = None
                    if gtc is not None:
                        resource_name = getattr(gtc, "resource_name", None) or (str(gtc) if gtc else None)
                    if resource_name is None:
                        resource_name = getattr(suggestion, "resource_name", None)
                    if resource_name is not None:
                        rn = str(resource_name)
                        reach = getattr(suggestion, "reach", None)
                        reach_by_constant[rn] = int(reach) if reach is not None else None
                        name_val = getattr(suggestion, "search_term", None) or getattr(gtc, "name", None) if gtc else None
                        name_by_constant[rn] = _safe_str(name_val, 512) if name_val else None
            except GoogleAdsException as e:
                logger.warning("SuggestGeoTargetConstants (reach) failed: %s", e)
            except Exception as e:
                logger.warning("SuggestGeoTargetConstants (reach) failed: %s", e)

        for r in rows_out:
            ids_str = r.get("geo_target_ids")
            if ids_str:
                names = [name_by_constant.get(i.strip(), i.strip()) for i in ids_str.split(",") if i.strip()]
                r["geo_target_names"] = (",".join(n for n in names if n))[:4096] if names else None
            else:
                r["geo_target_names"] = None
            neg_str = r.get("geo_negative_ids")
            if neg_str:
                names = [name_by_constant.get(i.strip(), i.strip()) for i in neg_str.split(",") if i.strip()]
                r["geo_negative_names"] = (",".join(n for n in names if n))[:4096] if names else None
            else:
                r["geo_negative_names"] = None

        geo_targeting_rows: List[Dict[str, Any]] = []
        ord_by_campaign_type: Dict[tuple, int] = {}
        for r in geo_location_rows:
            cid = r["campaign_id"]
            k = (cid, "LOCATION")
            ord_by_campaign_type[k] = ord_by_campaign_type.get(k, 0) + 1
            gt = r.get("geo_target_constant")
            geo_targeting_rows.append({
                "campaign_id": cid,
                "campaign_name": (r.get("campaign_name") or "")[:512],
                "criterion_id": r["criterion_id"],
                "criterion_type": "LOCATION",
                "ordinal": ord_by_campaign_type[k],
                "geo_target_constant": _safe_str(gt, 256) if gt else None,
                "geo_name": name_by_constant.get(gt) if gt else None,
                "negative": r.get("negative"),
                "positive_geo_target_type": _safe_str(r.get("positive_geo_target_type"), 64),
                "negative_geo_target_type": _safe_str(r.get("negative_geo_target_type"), 64),
                "proximity_street_address": None,
                "proximity_city_name": None,
                "radius": None,
                "radius_units": None,
                "latitude_micro": None,
                "longitude_micro": None,
                "estimated_reach": reach_by_constant.get(gt) if gt else None,
            })
        for r in geo_proximity_rows:
            cid = r["campaign_id"]
            k = (cid, "PROXIMITY")
            ord_by_campaign_type[k] = ord_by_campaign_type.get(k, 0) + 1
            radius_val = r.get("radius")
            ru = r.get("radius_units")
            geo_targeting_rows.append({
                "campaign_id": cid,
                "campaign_name": (r.get("campaign_name") or "")[:512],
                "criterion_id": r["criterion_id"],
                "criterion_type": "PROXIMITY",
                "ordinal": ord_by_campaign_type[k],
                "geo_target_constant": None,
                "geo_name": None,
                "negative": None,
                "positive_geo_target_type": None,
                "negative_geo_target_type": None,
                "proximity_street_address": _safe_str(r.get("proximity_street_address"), 1024),
                "proximity_city_name": _safe_str(r.get("proximity_city_name"), 256),
                "radius": float(radius_val) if radius_val is not None else None,
                "radius_units": (ru[:16] if ru and len(ru) > 16 else ru) if ru else None,
                "latitude_micro": r.get("latitude_micro"),
                "longitude_micro": r.get("longitude_micro"),
                "estimated_reach": None,
            })
        logger.info("fetch_campaign_control_state: %s campaigns, %s geo targeting rows for project %s", len(rows_out), len(geo_targeting_rows), project)
    except GoogleAdsException as ex:
        logger.error("Google Ads API error: %s", ex)
        return ([], [])
    return (rows_out, geo_targeting_rows)


def fetch_ad_group_device_modifiers(
    project: str,
    google_ads_filters: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Fetch ad group-level device bid modifiers (MOBILE, DESKTOP, TABLET). One row per (ad_group, device_type).
    Returns list of dicts: campaign_id, ad_group_id, device_type, bid_modifier for ppc_ad_group_device_modifier_daily.
    """
    client = get_client()
    customer_id_clean = _customer_id_clean(project)
    ga_service = client.get_service("GoogleAdsService")
    rows_out: List[Dict[str, Any]] = []
    try:
        query = (
            "SELECT campaign.id, ad_group.id, ad_group_bid_modifier.device.type, ad_group_bid_modifier.bid_modifier "
            "FROM ad_group_bid_modifier "
            "WHERE ad_group.status != 'REMOVED' AND campaign.status != 'REMOVED'"
        )
        stream = ga_service.search_stream(customer_id=customer_id_clean, query=query)
        for batch in stream:
            for row in batch.results:
                mod = getattr(row, "ad_group_bid_modifier", None)
                if not mod:
                    continue
                device = getattr(mod, "device", None)
                device_type = None
                if device is not None:
                    device_type = getattr(device, "type", None)
                    if device_type is not None and hasattr(device_type, "name"):
                        device_type = device_type.name
                    elif device_type is not None:
                        device_type = str(device_type)
                if not device_type:
                    continue
                bid_mod = getattr(mod, "bid_modifier", None)
                campaign_id = str(getattr(row.campaign, "id", "")) if getattr(row, "campaign", None) else ""
                ad_group_id = str(getattr(row.ad_group, "id", "")) if getattr(row, "ad_group", None) else ""
                if not campaign_id or not ad_group_id:
                    continue
                rows_out.append({
                    "campaign_id": campaign_id,
                    "ad_group_id": ad_group_id,
                    "device_type": device_type[:32] if device_type else None,
                    "bid_modifier": float(bid_mod) if bid_mod is not None else None,
                })
        logger.info("fetch_ad_group_device_modifiers: %s rows for project %s", len(rows_out), project)
    except GoogleAdsException as e:
        logger.warning("Ad group device modifiers query failed: %s", e)
    return rows_out


def fetch_change_events(
    project: str,
    snapshot_date: str,
    google_ads_filters: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Fetch change history (ChangeEvent) for the given date. Captures 'actions taken' in the account (including by automated rules).
    Returns list of dicts for ppc_change_event_daily. Only last 30 days are queryable; LIMIT 10000 per query.
    snapshot_date: YYYY-MM-DD string for the day to fetch.
    """
    client = get_client()
    customer_id_clean = _customer_id_clean(project)
    ga_service = client.get_service("GoogleAdsService")
    rows_out: List[Dict[str, Any]] = []
    try:
        start_ts = f"{snapshot_date} 00:00:00"
        end_ts = f"{snapshot_date} 23:59:59"
        query = (
            "SELECT change_event.resource_name, change_event.change_date_time, change_event.change_resource_type, "
            "change_event.change_resource_name, change_event.resource_change_operation, change_event.changed_fields, "
            "change_event.user_email, change_event.client_type, change_event.old_resource, change_event.new_resource "
            "FROM change_event "
            f"WHERE change_event.change_date_time >= '{start_ts}' AND change_event.change_date_time <= '{end_ts}' "
            "ORDER BY change_event.change_date_time "
            "LIMIT 10000"
        )
        stream = ga_service.search_stream(customer_id=customer_id_clean, query=query)
        for batch in stream:
            for row in batch.results:
                ev = getattr(row, "change_event", None)
                if not ev:
                    continue
                rn = getattr(ev, "resource_name", None)
                if not rn:
                    continue
                change_dt = getattr(ev, "change_date_time", None)
                change_dt_str = str(change_dt) if change_dt else None
                resource_type = getattr(ev, "change_resource_type", None)
                if resource_type is not None and hasattr(resource_type, "name"):
                    resource_type = resource_type.name
                else:
                    resource_type = str(resource_type) if resource_type else None
                change_rn = getattr(ev, "change_resource_name", None)
                change_rn_str = str(change_rn) if change_rn else None
                op = getattr(ev, "resource_change_operation", None)
                if op is not None and hasattr(op, "name"):
                    op = op.name
                else:
                    op = str(op) if op else None
                changed_f = getattr(ev, "changed_fields", None)
                if changed_f and hasattr(changed_f, "paths"):
                    changed_fields_str = ",".join(str(p) for p in changed_f.paths)[:4096]
                elif changed_f and isinstance(changed_f, (list, tuple)):
                    changed_fields_str = ",".join(str(p) for p in changed_f)[:4096]
                else:
                    changed_fields_str = str(changed_f)[:4096] if changed_f else None
                user_email = getattr(ev, "user_email", None)
                user_email = str(user_email)[:256] if user_email else None
                client_type = getattr(ev, "client_type", None)
                if client_type is not None and hasattr(client_type, "name"):
                    client_type = client_type.name
                else:
                    client_type = str(client_type)[:64] if client_type else None
                old_res = getattr(ev, "old_resource", None)
                new_res = getattr(ev, "new_resource", None)
                old_value = _change_resource_to_str(old_res)
                new_value = _change_resource_to_str(new_res)
                rows_out.append({
                    "change_event_resource_name": str(rn)[:512],
                    "change_date_time": change_dt_str[:48] if change_dt_str else None,
                    "change_resource_type": resource_type[:64] if resource_type else None,
                    "change_resource_name": change_rn_str[:512] if change_rn_str else None,
                    "resource_change_operation": op[:32] if op else None,
                    "changed_fields": changed_fields_str,
                    "user_email": user_email,
                    "client_type": client_type,
                    "old_value": old_value,
                    "new_value": new_value,
                })
        logger.info("fetch_change_events: %s events for project %s date %s", len(rows_out), project, snapshot_date)
    except GoogleAdsException as e:
        logger.warning("Change event query failed: %s", e)
    return rows_out


def fetch_conversion_actions(
    project: str,
    google_ads_filters: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Fetch conversion action definitions: list of conversion events, primary/secondary (include_in_conversions_metric),
    attribution model, lookback windows, counting type. Returns list of dicts for ppc_conversion_action_daily.
    """
    client = get_client()
    customer_id_clean = _customer_id_clean(project)
    ga_service = client.get_service("GoogleAdsService")
    rows_out: List[Dict[str, Any]] = []
    try:
        query = (
            "SELECT conversion_action.resource_name, conversion_action.name, conversion_action.type, "
            "conversion_action.status, conversion_action.category, conversion_action.include_in_conversions_metric, "
            "conversion_action.attribution_model_settings.attribution_model, "
            "conversion_action.click_through_lookback_window_days, conversion_action.counting_type "
            "FROM conversion_action"
        )
        stream = ga_service.search_stream(customer_id=customer_id_clean, query=query)
        for batch in stream:
            for row in batch.results:
                ca = getattr(row, "conversion_action", None)
                if not ca:
                    continue
                rn = getattr(ca, "resource_name", None)
                if not rn:
                    continue
                name = getattr(ca, "name", None)
                name = str(name)[:512] if name else None
                typ = getattr(ca, "type", None)
                if typ is not None and hasattr(typ, "name"):
                    typ = typ.name
                else:
                    typ = str(typ)[:64] if typ else None
                status = getattr(ca, "status", None)
                if status is not None and hasattr(status, "name"):
                    status = status.name
                else:
                    status = str(status)[:32] if status else None
                category = getattr(ca, "category", None)
                if category is not None and hasattr(category, "name"):
                    category = category.name
                else:
                    category = str(category)[:64] if category else None
                include_primary = getattr(ca, "include_in_conversions_metric", None)
                if include_primary is None:
                    include_primary = None
                else:
                    include_primary = bool(include_primary)
                attr_settings = getattr(ca, "attribution_model_settings", None)
                attribution_model = None
                if attr_settings:
                    am = getattr(attr_settings, "attribution_model", None)
                    if am is not None and hasattr(am, "name"):
                        attribution_model = am.name
                    else:
                        attribution_model = str(am)[:64] if am else None
                lookback_days = getattr(ca, "click_through_lookback_window_days", None)
                if lookback_days is not None:
                    lookback_days = int(lookback_days)
                counting_type = getattr(ca, "counting_type", None)
                if counting_type is not None and hasattr(counting_type, "name"):
                    counting_type = counting_type.name
                else:
                    counting_type = str(counting_type)[:32] if counting_type else None
                rows_out.append({
                    "conversion_action_resource_name": str(rn)[:512],
                    "name": name,
                    "type": typ,
                    "status": status,
                    "category": category,
                    "include_in_conversions_metric": include_primary,
                    "attribution_model": attribution_model,
                    "click_through_lookback_window_days": lookback_days,
                    "counting_type": counting_type,
                })
        logger.info("fetch_conversion_actions: %s rows for project %s", len(rows_out), project)
    except GoogleAdsException as e:
        logger.warning("Conversion actions query failed: %s", e)
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


def _fetch_ad_asset_urls_map(
    customer_id_clean: str,
    ga_service: Any,
    google_ads_filters: Optional[Dict[str, Any]] = None,
) -> Dict[tuple, List[str]]:
    """Fetch (ad_group_id, ad_id) -> list of asset URLs/references via ad_group_ad_asset_view + asset.
    YouTube videos become watch URLs; other assets (image, text, etc.) use resource_name as reference.
    """
    from collections import defaultdict
    # ad_group_id, ad_id -> list of URLs or asset resource names
    map_out: Dict[tuple, List[str]] = defaultdict(list)
    query = """
        SELECT ad_group.id, ad_group_ad.ad.id, asset.id, asset.resource_name, asset.type,
               asset.youtube_video_asset.youtube_video_id
        FROM ad_group_ad_asset_view
        WHERE ad_group_ad_asset_view.enabled = true
    """
    try:
        stream = ga_service.search_stream(customer_id=customer_id_clean, query=query)
        for batch in stream:
            for row in batch.results:
                ad_grp = getattr(row, "ad_group", None)
                ad = getattr(row, "ad_group_ad", None)
                asset = getattr(row, "asset", None)
                if not ad_grp or not ad or not asset:
                    continue
                ad_group_id = str(ad_grp.id)
                ad_id = str(ad.ad.id) if getattr(ad, "ad", None) else None
                if not ad_id:
                    continue
                asset_type = getattr(asset, "type", None)
                type_name = asset_type.name if asset_type and hasattr(asset_type, "name") else str(asset_type) if asset_type else None
                url_or_ref: Optional[str] = None
                if type_name == "YOUTUBE_VIDEO":
                    yt_id = getattr(getattr(asset, "youtube_video_asset", None), "youtube_video_id", None)
                    if yt_id:
                        url_or_ref = "https://www.youtube.com/watch?v=" + str(yt_id).strip()
                if not url_or_ref:
                    res_name = getattr(asset, "resource_name", None)
                    if res_name:
                        url_or_ref = str(res_name).strip()
                if url_or_ref:
                    map_out[(ad_group_id, ad_id)].append(url_or_ref)
    except GoogleAdsException as ex:
        logger.warning("ad_group_ad_asset_view query failed (asset_urls will be empty): %s", ex)
    return dict(map_out)


def fetch_ad_creative_snapshot(
    project: str,
    google_ads_filters: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Fetch all ad types (RSA, ETA, call, app, etc.) creative snapshot. TIER 2.
    Includes asset_urls for any ad that has linked assets (image, video, text, etc.) via ad_group_ad_asset_view.
    """
    import json as _json

    client = get_client()
    customer_id_clean = _customer_id_clean(project)
    ga_service = client.get_service("GoogleAdsService")
    # Fetch asset URLs/references per ad first (ad_group_id, ad_id) -> [urls]
    asset_urls_by_ad = _fetch_ad_asset_urls_map(customer_id_clean, ga_service, google_ads_filters)
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
                # Asset URLs: from ad_group_ad_asset_view (all asset types: image, video, text, etc.)
                ad_key = (str(ad_grp.id), str(ad.ad.id))
                asset_urls_list: List[str] = list(asset_urls_by_ad.get(ad_key, []))
                asset_urls_json = _json.dumps(asset_urls_list) if asset_urls_list else None
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
                    "asset_urls": (asset_urls_json or "")[:65535] if asset_urls_json else None,
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

    def process_criterion(c, camp, ad_group_id: str, campaign_id: str, camp_name: str, row: Any = None, ad_group: Any = None) -> None:
        if google_ads_filters and google_ads_filters.get("campaignNamePatterns"):
            patterns = google_ads_filters.get("campaignNamePatterns", [])
            if patterns and not any(p.lower() in (camp_name or "").lower() for p in patterns):
                return
        audience_type = c.type.name if hasattr(c.type, "name") else str(c.type)
        if audience_type not in audience_types:
            return
        aud_id, aud_name = _extract_audience_info(c, audience_type)
        if row is not None and aud_name is None:
            ul = getattr(row, "user_list", None)
            if ul and audience_type == "USER_LIST":
                aud_name = getattr(ul, "name", None)
            if aud_name is None:
                ui = getattr(row, "user_interest", None)
                if ui and audience_type == "USER_INTEREST":
                    aud_name = getattr(ui, "name", None) or getattr(ui, "user_interest_category", None)
            if aud_name is None:
                ca = getattr(row, "combined_audience", None)
                if ca and audience_type == "COMBINED_AUDIENCE":
                    aud_name = getattr(ca, "name", None)
        targeting_mode = None
        # TargetingSetting (observation vs targeting) is on campaign or ad_group, not on the criterion.
        entity = ad_group if ad_group is not None else camp
        ts = getattr(entity, "targeting_setting", None)
        if ts:
            restrictions = getattr(ts, "target_restrictions", []) or []
            for r in restrictions:
                dim = getattr(r, "targeting_dimension", None)
                dim_name = getattr(dim, "name", None) if dim and hasattr(dim, "name") else str(dim) if dim else None
                if dim_name != "AUDIENCE":
                    continue
                bid_only = getattr(r, "bid_only", None)
                if bid_only is not None:
                    targeting_mode = "OBSERVATION" if bid_only else "TARGETING"
                    break
            if targeting_mode is None and restrictions:
                bid_only = getattr(restrictions[0], "bid_only", None)
                if bid_only is not None:
                    targeting_mode = "OBSERVATION" if bid_only else "TARGETING"
        bid_mod = getattr(c, "bid_modifier", None)
        if bid_mod is not None:
            bid_mod = float(bid_mod)
        neg = getattr(c, "negative", None)
        c_status = getattr(c, "status", None)
        status = getattr(c_status, "name", None) if c_status and hasattr(c_status, "name") else (str(c_status) if c_status else None)
        audience_size = None
        if row is not None and audience_type == "USER_LIST":
            ul = getattr(row, "user_list", None)
            if ul:
                sz = getattr(ul, "size_for_display", None) or getattr(ul, "size_for_search", None)
                if sz is not None:
                    try:
                        audience_size = int(sz)
                    except (TypeError, ValueError):
                        pass
        rows_out.append({
            "campaign_id": campaign_id,
            "ad_group_id": (ad_group_id or "").strip(),
            "criterion_id": str(c.criterion_id),
            "audience_type": audience_type,
            "audience_id": aud_id[:256] if aud_id and len(aud_id) > 256 else aud_id,
            "audience_name": (aud_name[:512] if aud_name and len(aud_name) > 512 else aud_name) if aud_name else None,
            "targeting_mode": (targeting_mode[:32] if targeting_mode and len(targeting_mode) > 32 else targeting_mode) if targeting_mode else None,
            "status": (status[:32] if status and len(status) > 32 else status) if status else None,
            "bid_modifier": bid_mod,
            "negative": bool(neg) if neg is not None else False,
            "audience_size": audience_size,
        })

    try:
        q_campaign = """
            SELECT campaign.id, campaign.name, campaign.targeting_setting.target_restrictions,
                   campaign_criterion.criterion_id, campaign_criterion.type, campaign_criterion.status,
                   campaign_criterion.bid_modifier, campaign_criterion.negative,
                   campaign_criterion.user_list.user_list, campaign_criterion.user_interest.user_interest_category,
                   campaign_criterion.combined_audience.combined_audience,
                   user_list.name, user_list.size_for_display, user_list.size_for_search,
                   user_interest.name
            FROM campaign_criterion
            WHERE campaign_criterion.type IN ('USER_LIST', 'USER_INTEREST', 'CUSTOM_AFFINITY', 'CUSTOM_INTENT', 'COMBINED_AUDIENCE', 'CUSTOM_AUDIENCE')
        """
        try:
            stream = ga_service.search_stream(customer_id=customer_id_clean, query=q_campaign)
        except GoogleAdsException as e:
            if "UNRECOGNIZED_FIELD" in str(e) or "user_list.name" in str(e) or "user_interest.name" in str(e):
                q_campaign = """
                    SELECT campaign.id, campaign.name, campaign.targeting_setting.target_restrictions,
                           campaign_criterion.criterion_id, campaign_criterion.type, campaign_criterion.status,
                           campaign_criterion.bid_modifier, campaign_criterion.negative,
                           campaign_criterion.user_list.user_list, campaign_criterion.user_interest.user_interest_category,
                           campaign_criterion.combined_audience.combined_audience
                    FROM campaign_criterion
                    WHERE campaign_criterion.type IN ('USER_LIST', 'USER_INTEREST', 'CUSTOM_AFFINITY', 'CUSTOM_INTENT', 'COMBINED_AUDIENCE', 'CUSTOM_AUDIENCE')
                """
                stream = ga_service.search_stream(customer_id=customer_id_clean, query=q_campaign)
            else:
                raise
        for batch in stream:
            for row in batch.results:
                c = row.campaign_criterion
                camp = row.campaign
                process_criterion(c, camp, "", str(camp.id), getattr(camp, "name", None) or "", row=row, ad_group=None)

        q_ad_group = """
            SELECT campaign.id, campaign.name, ad_group.id, ad_group.targeting_setting.target_restrictions,
                   ad_group_criterion.criterion_id, ad_group_criterion.type, ad_group_criterion.status,
                   ad_group_criterion.bid_modifier, ad_group_criterion.negative,
                   ad_group_criterion.user_list.user_list, ad_group_criterion.user_interest.user_interest_category,
                   ad_group_criterion.combined_audience.combined_audience,
                   user_list.name, user_list.size_for_display, user_list.size_for_search,
                   user_interest.name
            FROM ad_group_criterion
            WHERE ad_group_criterion.type IN ('USER_LIST', 'USER_INTEREST', 'CUSTOM_AFFINITY', 'CUSTOM_INTENT', 'COMBINED_AUDIENCE', 'CUSTOM_AUDIENCE')
        """
        try:
            stream_ag = ga_service.search_stream(customer_id=customer_id_clean, query=q_ad_group)
        except GoogleAdsException as e:
            if "UNRECOGNIZED_FIELD" in str(e) or "user_list.name" in str(e) or "user_interest.name" in str(e):
                q_ad_group = """
                    SELECT campaign.id, campaign.name, ad_group.id, ad_group.targeting_setting.target_restrictions,
                           ad_group_criterion.criterion_id, ad_group_criterion.type, ad_group_criterion.status,
                           ad_group_criterion.bid_modifier, ad_group_criterion.negative,
                           ad_group_criterion.user_list.user_list, ad_group_criterion.user_interest.user_interest_category,
                           ad_group_criterion.combined_audience.combined_audience
                    FROM ad_group_criterion
                    WHERE ad_group_criterion.type IN ('USER_LIST', 'USER_INTEREST', 'CUSTOM_AFFINITY', 'CUSTOM_INTENT', 'COMBINED_AUDIENCE', 'CUSTOM_AUDIENCE')
                """
                stream_ag = ga_service.search_stream(customer_id=customer_id_clean, query=q_ad_group)
            else:
                raise
        for batch in stream_ag:
            for row in batch.results:
                c = row.ad_group_criterion
                ad_grp = row.ad_group
                camp = row.campaign
                ag_id = str(ad_grp.id) if ad_grp and getattr(ad_grp, "id", None) is not None else ""
                process_criterion(c, camp, ag_id, str(camp.id), getattr(camp, "name", None) or "", row=row, ad_group=ad_grp)

        logger.info("fetch_audience_targeting_snapshot: %s audience criteria for project %s", len(rows_out), project)
    except GoogleAdsException as ex:
        logger.error("Google Ads API error: %s", ex)
    return rows_out
