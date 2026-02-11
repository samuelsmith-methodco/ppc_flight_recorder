-- =============================================================================
-- PPC Flight Recorder Tables – run in your Snowflake database/schema
-- =============================================================================
-- Migration: ppc_campaign_control_diff_daily is now created (day-over-day control state changes).
-- Migration: Geo names for control state (run if ppc_campaign_control_state_daily exists without them):
--   ALTER TABLE ppc_campaign_control_state_daily ADD COLUMN IF NOT EXISTS geo_target_names VARCHAR(4096);
--   ALTER TABLE ppc_campaign_control_state_daily ADD COLUMN IF NOT EXISTS geo_negative_names VARCHAR(4096);
-- Migration: Change event old/new values (run if ppc_change_event_daily exists without them):
--   ALTER TABLE ppc_change_event_daily ADD COLUMN IF NOT EXISTS old_value VARCHAR(65535);
--   ALTER TABLE ppc_change_event_daily ADD COLUMN IF NOT EXISTS new_value VARCHAR(65535);
-- Migration: TIER 4 policy_summary for ppc_ad_creative_snapshot_daily (run if table exists):
--   ALTER TABLE ppc_ad_creative_snapshot_daily ADD COLUMN policy_summary_json VARCHAR(65535);
-- Migration: status column for ppc_ad_creative_snapshot_daily (run if table exists):
--   ALTER TABLE ppc_ad_creative_snapshot_daily ADD COLUMN IF NOT EXISTS status VARCHAR(32);
-- Migration: TIER 3 geo/device/timezone columns (run if table exists):
--   ALTER TABLE ppc_campaign_control_state_daily ADD COLUMN IF NOT EXISTS geo_negative_ids VARCHAR(4096);
--   ALTER TABLE ppc_campaign_control_state_daily ADD COLUMN IF NOT EXISTS geo_radius_json VARCHAR(65535);
--   ALTER TABLE ppc_campaign_control_state_daily ADD COLUMN IF NOT EXISTS location_presence_interest_json VARCHAR(4096);
--   ALTER TABLE ppc_campaign_control_state_daily ADD COLUMN IF NOT EXISTS account_timezone VARCHAR(64);
--   ALTER TABLE ppc_campaign_control_state_daily ADD COLUMN IF NOT EXISTS device_modifiers_json VARCHAR(4096);
-- Migration: If ppc_campaign_outcomes_daily already exists without campaign_name:
--   ALTER TABLE ppc_campaign_outcomes_daily ADD COLUMN campaign_name VARCHAR(512);
-- Migration: Primary keys updated to include ad_group_id (fix duplicate rows). Existing tables must be recreated or migrated:
--   ppc_keyword_snapshot_daily: new PK (keyword_criterion_id, snapshot_date, customer_id, ad_group_id). Recreate table and copy data, or create new table and backfill.
--   ppc_negative_keyword_snapshot_daily: new PK (snapshot_date, customer_id, campaign_id, ad_group_id, criterion_id). Same.
--   ppc_negative_keyword_diff_daily: new PK (snapshot_date, customer_id, campaign_id, ad_group_id, criterion_id). Same.
-- Migration: Keyword/negative keyword level and "Added to" (campaign_name, ad_group_name). Run if tables exist:
--   ALTER TABLE ppc_keyword_snapshot_daily ADD COLUMN IF NOT EXISTS keyword_level VARCHAR(32);
--   ALTER TABLE ppc_keyword_snapshot_daily ADD COLUMN IF NOT EXISTS campaign_name VARCHAR(512);
--   ALTER TABLE ppc_keyword_snapshot_daily ADD COLUMN IF NOT EXISTS ad_group_name VARCHAR(512);
--   ALTER TABLE ppc_negative_keyword_snapshot_daily ADD COLUMN IF NOT EXISTS ad_group_id VARCHAR(64) DEFAULT '';
--   ALTER TABLE ppc_negative_keyword_snapshot_daily ADD COLUMN IF NOT EXISTS keyword_level VARCHAR(32);
--   ALTER TABLE ppc_negative_keyword_snapshot_daily ADD COLUMN IF NOT EXISTS campaign_name VARCHAR(512);
--   ALTER TABLE ppc_negative_keyword_snapshot_daily ADD COLUMN IF NOT EXISTS ad_group_name VARCHAR(512);
--   UPDATE ppc_negative_keyword_snapshot_daily SET ad_group_id = '' WHERE ad_group_id IS NULL;
--   ALTER TABLE ppc_negative_keyword_diff_daily ADD COLUMN IF NOT EXISTS ad_group_id VARCHAR(64) DEFAULT '';
--   UPDATE ppc_negative_keyword_diff_daily SET ad_group_id = '' WHERE ad_group_id IS NULL;
-- Migration: Google Ads tables use customer_id (Google Ads customer ID) instead of project name.
--   For existing tables run for each Google Ads table: ALTER TABLE <tbl> RENAME COLUMN project TO customer_id;
--   Then backfill: UPDATE <tbl> SET customer_id = '8945413609' WHERE customer_id = 'the-pinch'; (repeat per project).
-- GA4 tables (ppc_ga4_*) keep project. Google Ads tables: control_state, outcomes, outcomes_diff, *_dims use customer_id.
-- Google Ads provides at ad_group: id, name, campaign; at keyword_view: criterion_id,
-- keyword.text, match_type, ad_group, campaign; metrics: impressions, clicks, cost_micros,
-- conversions, conversions_value, ctr, average_cpc, search_impression_share (etc.).
-- =============================================================================

-- ppc_campaign_control_state_daily: Campaign settings snapshot (one row per campaign per day).
-- Column notes:
--   campaign_id, snapshot_date, customer_id: PK. customer_id = Google Ads customer ID.
--   campaign_name: Display name from Google Ads.
--   status: ENABLED, PAUSED, or REMOVED.
--   advertising_channel_type: SEARCH, DISPLAY, SHOPPING, etc.
--   advertising_channel_sub_type: Often NULL/UNSPECIFIED for standard Search; set for Shopping, Performance Max, etc.
--   daily_budget_micros, daily_budget_amount: Budget in micros and dollars. Populated for standard and shared budgets.
--   budget_delivery_method: STANDARD or ACCELERATED.
--   bidding_strategy_type: e.g. MAXIMIZE_CONVERSIONS, TARGET_CPA, TARGET_ROAS, MANUAL_CPC, or portfolio strategy name.
--   target_cpa_micros, target_cpa_amount: Set when campaign or its bidding strategy has a target CPA (e.g. TARGET_CPA, or Maximize conversions with optional target CPA); NULL otherwise.
--   target_roas: Set only when bidding_strategy_type is TARGET_ROAS; NULL otherwise.
--   target_impression_share_location: For TARGET_IMPRESSION_SHARE strategy; NULL for other strategies.
--   target_impression_share_location_fraction_micros: For TARGET_IMPRESSION_SHARE; NULL otherwise.
--   geo_target_ids: Comma-separated geo target constant resource names for included locations (campaign_criterion type LOCATION, negative=false).
--   geo_negative_ids: Comma-separated geo target IDs for excluded locations (negative=true).
--   geo_target_names: Comma-separated human-readable names for geo_target_ids (from GeoTargetConstantService).
--   geo_negative_names: Comma-separated human-readable names for geo_negative_ids (from GeoTargetConstantService).
--   geo_radius_json: JSON array of proximity targets (radius, units, lat/long). Empty if no radius targeting.
--   account_timezone: Customer account timezone (e.g. America/New_York). Used for ad schedule interpretation.
--   network_settings_*: Which networks the campaign targets (Google Search, Search Partners, Display, etc.).
--   ad_schedule_json: JSON array of {day_of_week, start_hour, start_minute, end_hour, end_minute, bid_modifier}. Empty if no schedule.
--   audience_target_count: Count of audience criteria (USER_LIST, USER_INTEREST, etc.) on the campaign.
--   campaign_type: Display label combining channel type and sub type (e.g. "Search", "Display").
--   networks: Summary of enabled networks (e.g. "Search, Search Partners, Display").
--   campaign_start_date, campaign_end_date: Campaign date range from Google Ads.
--   location: Summary of location targeting (geo_target_ids or short description).
--   active_bid_adj: Audience segments used for bid adjustment (e.g. None or "User interest And List").
CREATE TABLE IF NOT EXISTS ppc_campaign_control_state_daily (
    campaign_id VARCHAR(64) NOT NULL,
    snapshot_date DATE NOT NULL,
    customer_id VARCHAR(128) NOT NULL,
    campaign_name VARCHAR(512),
    status VARCHAR(32),
    advertising_channel_type VARCHAR(64),
    advertising_channel_sub_type VARCHAR(64),
    daily_budget_micros NUMBER(20, 0),
    daily_budget_amount NUMBER(12, 2),
    budget_delivery_method VARCHAR(64),
    bidding_strategy_type VARCHAR(64),
    target_cpa_micros NUMBER(20, 0),
    target_cpa_amount NUMBER(12, 2),
    target_roas NUMBER(8, 4),
    target_impression_share_location VARCHAR(32),
    target_impression_share_location_fraction_micros NUMBER(20, 0),
    geo_target_ids VARCHAR(4096),
    geo_negative_ids VARCHAR(4096),
    geo_target_names VARCHAR(4096),
    geo_negative_names VARCHAR(4096),
    geo_radius_json VARCHAR(65535),
    account_timezone VARCHAR(64),
    network_settings_target_google_search BOOLEAN,
    network_settings_target_search_network BOOLEAN,
    network_settings_target_content_network BOOLEAN,
    network_settings_target_partner_search_network BOOLEAN,
    ad_schedule_json VARCHAR(65535),
    audience_target_count INTEGER,
    campaign_type VARCHAR(128),
    networks VARCHAR(256),
    campaign_start_date DATE,
    campaign_end_date DATE,
    location VARCHAR(4096),
    active_bid_adj VARCHAR(256),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (campaign_id, snapshot_date, customer_id)
);

-- Add/remove columns on existing tables (run once per env; Snowflake: ADD/DROP COLUMN IF EXISTS supported in recent versions)
-- Remove location_effective_reach_estimate (moved to ppc_campaign_geo_targeting_daily.effective_reach_estimate):
-- ALTER TABLE ppc_campaign_control_state_daily DROP COLUMN IF EXISTS location_effective_reach_estimate;
-- Remove device_modifiers_json, devices, location_presence_interest_json (no longer stored in control state):
-- ALTER TABLE ppc_campaign_control_state_daily DROP COLUMN IF EXISTS device_modifiers_json;
-- ALTER TABLE ppc_campaign_control_state_daily DROP COLUMN IF EXISTS devices;
-- ALTER TABLE ppc_campaign_control_state_daily DROP COLUMN IF EXISTS location_presence_interest_json;
-- ALTER TABLE ppc_campaign_control_state_daily ADD COLUMN IF NOT EXISTS campaign_type VARCHAR(128);
-- ALTER TABLE ppc_campaign_control_state_daily ADD COLUMN IF NOT EXISTS networks VARCHAR(256);
-- ALTER TABLE ppc_campaign_control_state_daily ADD COLUMN IF NOT EXISTS campaign_start_date DATE;
-- ALTER TABLE ppc_campaign_control_state_daily ADD COLUMN IF NOT EXISTS campaign_end_date DATE;
-- ALTER TABLE ppc_campaign_control_state_daily ADD COLUMN IF NOT EXISTS location VARCHAR(4096);
-- ALTER TABLE ppc_campaign_control_state_daily ADD COLUMN IF NOT EXISTS active_bid_adj VARCHAR(256);

-- ppc_campaign_control_diff_daily: Day-over-day changes in campaign control state.
--   changed_metric_name: Field that changed (e.g. daily_budget_amount, status, geo_target_ids).
--   old_value, new_value: Previous and new value. Rows only when value actually changed.
CREATE TABLE IF NOT EXISTS ppc_campaign_control_diff_daily (
    campaign_id VARCHAR(64) NOT NULL,
    snapshot_date DATE NOT NULL,
    customer_id VARCHAR(128) NOT NULL,
    changed_metric_name VARCHAR(128) NOT NULL,
    old_value VARCHAR(65535),
    new_value VARCHAR(65535),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (campaign_id, snapshot_date, customer_id, changed_metric_name)
);

-- ppc_campaign_geo_targeting_daily: Geo & location targeting audit (one row per campaign criterion per day). v23 GAQL spec.
--   criterion_id: campaign_criterion.criterion_id (stable across days for diff tracking).
--   criterion_type: LOCATION | PROXIMITY.
--   negative: TRUE = exclusion (location only). NULL for PROXIMITY.
--   positive_geo_target_type / negative_geo_target_type: From campaign.geo_target_type_setting (DONT_CARE | AREA_OF_INTEREST | LOCATION_OF_PRESENCE).
--   proximity_street_address, proximity_city_name: For PROXIMITY when API returns. NULL for LOCATION.
--   estimated_reach: From GeoTargetConstantService.SuggestGeoTargetConstants.reach.
--   ordinal: 1-based display order within (campaign_id, criterion_type).
--   latitude_micro, longitude_micro: Center of PROXIMITY radius (micro-degrees); from proximity.geo_point when selectable. NULL for LOCATION. Often null if API does not expose geo_point in GAQL.
-- Migration: Schema changed (criterion_id PK, campaign_name, geo_target_type_setting, estimated_reach, etc.). To apply:
--   DROP TABLE IF EXISTS ppc_campaign_geo_targeting_diff_daily;
--   DROP TABLE IF EXISTS ppc_campaign_geo_targeting_daily;
--   Then run this file to recreate both tables.
CREATE TABLE IF NOT EXISTS ppc_campaign_geo_targeting_daily (
    snapshot_date DATE NOT NULL,
    customer_id VARCHAR(128) NOT NULL,
    campaign_id VARCHAR(64) NOT NULL,
    campaign_name VARCHAR(512),
    criterion_id VARCHAR(64) NOT NULL,
    criterion_type VARCHAR(32) NOT NULL,
    ordinal INTEGER NOT NULL,
    geo_target_constant VARCHAR(256),
    geo_name VARCHAR(512),
    negative BOOLEAN,
    positive_geo_target_type VARCHAR(64),
    negative_geo_target_type VARCHAR(64),
    proximity_street_address VARCHAR(1024),
    proximity_city_name VARCHAR(256),
    radius NUMBER(10, 2),
    radius_units VARCHAR(16),
    latitude_micro NUMBER(20, 0),
    longitude_micro NUMBER(20, 0),
    estimated_reach NUMBER(18, 0),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (snapshot_date, customer_id, campaign_id, criterion_type, criterion_id)
);

-- ppc_campaign_geo_targeting_diff_daily: Day-over-day geo targeting changes (like ppc_campaign_control_diff_daily).
--   criterion_id: Stable id for add/remove/change. changed_metric_name: criterion_added | criterion_removed | field names.
CREATE TABLE IF NOT EXISTS ppc_campaign_geo_targeting_diff_daily (
    snapshot_date DATE NOT NULL,
    customer_id VARCHAR(128) NOT NULL,
    campaign_id VARCHAR(64) NOT NULL,
    criterion_type VARCHAR(32) NOT NULL,
    criterion_id VARCHAR(64) NOT NULL,
    changed_metric_name VARCHAR(128) NOT NULL,
    old_value VARCHAR(65535),
    new_value VARCHAR(65535),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (snapshot_date, customer_id, campaign_id, criterion_type, criterion_id, changed_metric_name)
);

-- ppc_campaign_outcomes_daily: Campaign performance metrics (one row per campaign per outcome_date).
--   outcome_date: Date of metrics (from segments.date in Google Ads).
--   cost_micros, cost_amount: Cost in micros and dollars.
--   conversions_value: Value of conversions (revenue attributed).
--   ctr: Click-through rate (%). cpc: Cost per click. cpa: Cost per acquisition. roas: Return on ad spend. cvr: Conversion rate (%).
--   search_impression_share_pct: % of eligible impressions the campaign received.
--   search_rank_lost_impression_share_pct: % of impressions lost due to ad rank.
CREATE TABLE IF NOT EXISTS ppc_campaign_outcomes_daily (
    campaign_id VARCHAR(64) NOT NULL,
    outcome_date DATE NOT NULL,
    customer_id VARCHAR(128) NOT NULL,
    campaign_name VARCHAR(512),
    impressions INTEGER DEFAULT 0,
    clicks INTEGER DEFAULT 0,
    cost_micros NUMBER(20, 0) DEFAULT 0,
    cost_amount NUMBER(12, 2) DEFAULT 0,
    conversions NUMBER(12, 4) DEFAULT 0,
    conversions_value NUMBER(14, 2) DEFAULT 0,
    ctr NUMBER(8, 4),
    cpc NUMBER(10, 4),
    cpa NUMBER(10, 2),
    roas NUMBER(10, 4),
    cvr NUMBER(8, 2),
    search_impression_share_pct NUMBER(8, 2),
    search_rank_lost_impression_share_pct NUMBER(8, 2),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (campaign_id, outcome_date, customer_id)
);

-- ppc_ga4_traffic_acquisition_daily: Legacy GA4 traffic acquisition only. Prefer ppc_ga4_acquisition_daily for all report types.
--   project: GA4 property ID (e.g. the-pinch). dimension_type/dimension_value: Breakdown (e.g. session_medium/social).
CREATE TABLE IF NOT EXISTS ppc_ga4_traffic_acquisition_daily (
    project VARCHAR(128) NOT NULL,
    acquisition_date DATE NOT NULL,
    dimension_type VARCHAR(64) NOT NULL,
    dimension_value VARCHAR(512) NOT NULL,
    sessions INTEGER DEFAULT 0,
    engaged_sessions INTEGER DEFAULT 0,
    total_revenue NUMBER(14, 2) DEFAULT 0,
    event_count NUMBER(20, 0) DEFAULT 0,
    key_events INTEGER DEFAULT 0,
    active_users INTEGER DEFAULT 0,
    average_session_duration_sec NUMBER(12, 2),
    engagement_rate NUMBER(8, 4),
    bounce_rate NUMBER(8, 4),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (project, acquisition_date, dimension_type, dimension_value)
);

-- ppc_ga4_acquisition_daily: GA4 acquisition metrics for traffic, user, and overview report types.
--   report_type: traffic_acquisition | user_acquisition | acquisition_overview.
--   dimension_type, dimension_value: Breakdown (e.g. session_medium, organic; or first_user_medium, organic).
CREATE TABLE IF NOT EXISTS ppc_ga4_acquisition_daily (
    project VARCHAR(128) NOT NULL,
    acquisition_date DATE NOT NULL,
    report_type VARCHAR(64) NOT NULL,
    dimension_type VARCHAR(64) NOT NULL,
    dimension_value VARCHAR(512) NOT NULL,
    sessions INTEGER DEFAULT 0,
    engaged_sessions INTEGER DEFAULT 0,
    total_revenue NUMBER(14, 2) DEFAULT 0,
    event_count NUMBER(20, 0) DEFAULT 0,
    key_events INTEGER DEFAULT 0,
    active_users INTEGER DEFAULT 0,
    average_session_duration_sec NUMBER(12, 2),
    engagement_rate NUMBER(8, 4),
    bounce_rate NUMBER(8, 4),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (project, acquisition_date, report_type, dimension_type, dimension_value)
);

-- ppc_ga4_acquisition_diff_daily: Day-over-day changes in GA4 acquisition metrics.
CREATE TABLE IF NOT EXISTS ppc_ga4_acquisition_diff_daily (
    project VARCHAR(128) NOT NULL,
    acquisition_date DATE NOT NULL,
    report_type VARCHAR(64) NOT NULL,
    dimension_type VARCHAR(64) NOT NULL,
    dimension_value VARCHAR(512) NOT NULL,
    changed_metric_name VARCHAR(128) NOT NULL,
    old_value VARCHAR(65535),
    new_value VARCHAR(65535),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (project, acquisition_date, report_type, dimension_type, dimension_value, changed_metric_name)
);

-- ppc_campaign_outcomes_diff_daily: Day-over-day changes in campaign outcomes (impressions, cost, conversions, etc.).
CREATE TABLE IF NOT EXISTS ppc_campaign_outcomes_diff_daily (
    campaign_id VARCHAR(64) NOT NULL,
    outcome_date DATE NOT NULL,
    customer_id VARCHAR(128) NOT NULL,
    changed_metric_name VARCHAR(128) NOT NULL,
    old_value VARCHAR(65535),
    new_value VARCHAR(65535),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (campaign_id, outcome_date, customer_id, changed_metric_name)
);

-- ppc_ad_group_outcomes_daily: Ad group performance metrics (same structure as campaign outcomes).
CREATE TABLE IF NOT EXISTS ppc_ad_group_outcomes_daily (
    ad_group_id VARCHAR(64) NOT NULL,
    campaign_id VARCHAR(64) NOT NULL,
    outcome_date DATE NOT NULL,
    customer_id VARCHAR(128) NOT NULL,
    ad_group_name VARCHAR(512),
    impressions INTEGER DEFAULT 0,
    clicks INTEGER DEFAULT 0,
    cost_micros NUMBER(20, 0) DEFAULT 0,
    cost_amount NUMBER(12, 2) DEFAULT 0,
    conversions NUMBER(12, 4) DEFAULT 0,
    conversions_value NUMBER(14, 2) DEFAULT 0,
    ctr NUMBER(8, 4),
    cpc NUMBER(10, 4),
    cpa NUMBER(10, 2),
    roas NUMBER(10, 4),
    cvr NUMBER(8, 2),
    search_impression_share_pct NUMBER(8, 2),
    search_rank_lost_impression_share_pct NUMBER(8, 2),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (ad_group_id, outcome_date, customer_id)
);

-- ppc_ad_group_outcomes_diff_daily: Day-over-day changes in ad group outcomes.
CREATE TABLE IF NOT EXISTS ppc_ad_group_outcomes_diff_daily (
    ad_group_id VARCHAR(64) NOT NULL,
    campaign_id VARCHAR(64) NOT NULL,
    outcome_date DATE NOT NULL,
    customer_id VARCHAR(128) NOT NULL,
    changed_metric_name VARCHAR(128) NOT NULL,
    old_value VARCHAR(65535),
    new_value VARCHAR(65535),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (ad_group_id, outcome_date, customer_id, changed_metric_name)
);

-- ppc_ad_group_snapshot_daily: Ad group structure snapshot (status, name). Used for add/remove/rename detection.
--   status: ENABLED, PAUSED, or REMOVED. API filters REMOVED, so removals detected by absence from snapshot.
CREATE TABLE IF NOT EXISTS ppc_ad_group_snapshot_daily (
    ad_group_id VARCHAR(64) NOT NULL,
    campaign_id VARCHAR(64) NOT NULL,
    snapshot_date DATE NOT NULL,
    customer_id VARCHAR(128) NOT NULL,
    ad_group_name VARCHAR(512),
    status VARCHAR(32),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (ad_group_id, snapshot_date, customer_id)
);

-- ppc_ad_group_change_daily: Ad group structure changes. change_type: ADDED | REMOVED | STATUS_CHANGED | RENAMED | UPDATED.
CREATE TABLE IF NOT EXISTS ppc_ad_group_change_daily (
    snapshot_date DATE NOT NULL,
    customer_id VARCHAR(128) NOT NULL,
    campaign_id VARCHAR(64) NOT NULL,
    ad_group_id VARCHAR(64) NOT NULL,
    change_type VARCHAR(32) NOT NULL,
    ad_group_name VARCHAR(512),
    status VARCHAR(32),
    old_value VARCHAR(65535),
    new_value VARCHAR(65535),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (snapshot_date, customer_id, campaign_id, ad_group_id)
);

-- ppc_ad_group_device_modifier_daily: Device targeting (ad group-level bid modifiers: MOBILE, DESKTOP, TABLET). One row per (ad_group, device_type) per day.
--   device_type: MOBILE, DESKTOP, TABLET. bid_modifier: multiplier (e.g. 1.2 = +20%). Capture frequency: daily; date of change from _diff_daily.
CREATE TABLE IF NOT EXISTS ppc_ad_group_device_modifier_daily (
    snapshot_date DATE NOT NULL,
    customer_id VARCHAR(128) NOT NULL,
    campaign_id VARCHAR(64) NOT NULL,
    ad_group_id VARCHAR(64) NOT NULL,
    device_type VARCHAR(32) NOT NULL,
    bid_modifier NUMBER(8, 4),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (snapshot_date, customer_id, campaign_id, ad_group_id, device_type)
);

-- ppc_ad_group_device_modifier_diff_daily: Day-over-day device modifier changes (add/remove/change). changed_metric_name: device_modifier_added | device_modifier_removed | bid_modifier.
CREATE TABLE IF NOT EXISTS ppc_ad_group_device_modifier_diff_daily (
    snapshot_date DATE NOT NULL,
    customer_id VARCHAR(128) NOT NULL,
    campaign_id VARCHAR(64) NOT NULL,
    ad_group_id VARCHAR(64) NOT NULL,
    device_type VARCHAR(32) NOT NULL,
    changed_metric_name VARCHAR(128) NOT NULL,
    old_value VARCHAR(65535),
    new_value VARCHAR(65535),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (snapshot_date, customer_id, campaign_id, ad_group_id, device_type, changed_metric_name)
);

-- ppc_change_event_daily: Change history (ChangeEvent) – actions taken in the account (UI, API, scripts, automated rules).
--   Captures "actions taken" for audit; rule logic/trigger events are not exposed by the API.
--   change_date: Date of the change (for partitioning). change_event_resource_name: Unique event id from API.
--   change_resource_type: CAMPAIGN, AD_GROUP, etc. resource_change_operation: CREATE, UPDATE, DELETE.
--   changed_fields: Comma-separated field paths. user_email, client_type: Who/what made the change (e.g. GOOGLE_ADS for rules).
--   old_value, new_value: Serialized old/new resource (JSON or text); only changed fields are populated in the API payload.
--   Only last 30 days are queryable via API; sync daily to retain history.
CREATE TABLE IF NOT EXISTS ppc_change_event_daily (
    change_date DATE NOT NULL,
    customer_id VARCHAR(128) NOT NULL,
    change_event_resource_name VARCHAR(512) NOT NULL,
    change_date_time VARCHAR(48),
    change_resource_type VARCHAR(64),
    change_resource_name VARCHAR(512),
    resource_change_operation VARCHAR(32),
    changed_fields VARCHAR(4096),
    user_email VARCHAR(256),
    client_type VARCHAR(64),
    old_value VARCHAR(65535),
    new_value VARCHAR(65535),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (change_date, customer_id, change_event_resource_name)
);

-- ppc_keyword_outcomes_daily: Keyword-level performance metrics (one row per keyword per outcome_date).
--   keyword_criterion_id: Google Ads criterion_id for the keyword. match_type: EXACT, PHRASE, BROAD.
CREATE TABLE IF NOT EXISTS ppc_keyword_outcomes_daily (
    keyword_criterion_id VARCHAR(64) NOT NULL,
    ad_group_id VARCHAR(64) NOT NULL,
    campaign_id VARCHAR(64) NOT NULL,
    outcome_date DATE NOT NULL,
    customer_id VARCHAR(128) NOT NULL,
    keyword_text VARCHAR(1024),
    match_type VARCHAR(32),
    impressions INTEGER DEFAULT 0,
    clicks INTEGER DEFAULT 0,
    cost_micros NUMBER(20, 0) DEFAULT 0,
    cost_amount NUMBER(12, 2) DEFAULT 0,
    conversions NUMBER(12, 4) DEFAULT 0,
    conversions_value NUMBER(14, 2) DEFAULT 0,
    ctr NUMBER(8, 4),
    cpc NUMBER(10, 4),
    cpa NUMBER(10, 2),
    roas NUMBER(10, 4),
    cvr NUMBER(8, 2),
    search_impression_share_pct NUMBER(8, 2),
    search_rank_lost_impression_share_pct NUMBER(8, 2),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (keyword_criterion_id, outcome_date, customer_id)
);

-- ppc_keyword_outcomes_diff_daily: Day-over-day changes in keyword outcomes.
CREATE TABLE IF NOT EXISTS ppc_keyword_outcomes_diff_daily (
    keyword_criterion_id VARCHAR(64) NOT NULL,
    outcome_date DATE NOT NULL,
    customer_id VARCHAR(128) NOT NULL,
    changed_metric_name VARCHAR(128) NOT NULL,
    old_value VARCHAR(65535),
    new_value VARCHAR(65535),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (keyword_criterion_id, outcome_date, customer_id, changed_metric_name)
);

-- ppc_keyword_snapshot_daily: Keyword structure snapshot for add/remove/match-type change detection.
-- keyword_level: AD_GROUP (positive keywords are always at ad group level). Same criterion_id can appear in multiple ad groups; PK includes ad_group_id.
CREATE TABLE IF NOT EXISTS ppc_keyword_snapshot_daily (
    keyword_criterion_id VARCHAR(64) NOT NULL,
    ad_group_id VARCHAR(64) NOT NULL,
    campaign_id VARCHAR(64) NOT NULL,
    snapshot_date DATE NOT NULL,
    customer_id VARCHAR(128) NOT NULL,
    keyword_text VARCHAR(1024),
    match_type VARCHAR(32),
    keyword_level VARCHAR(32),
    campaign_name VARCHAR(512),
    ad_group_name VARCHAR(512),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (keyword_criterion_id, snapshot_date, customer_id, ad_group_id)
);

-- ppc_keyword_change_daily: Keyword structure changes. change_type: ADDED | REMOVED | MATCH_TYPE_CHANGED.
CREATE TABLE IF NOT EXISTS ppc_keyword_change_daily (
    snapshot_date DATE NOT NULL,
    customer_id VARCHAR(128) NOT NULL,
    campaign_id VARCHAR(64) NOT NULL,
    ad_group_id VARCHAR(64) NOT NULL,
    keyword_criterion_id VARCHAR(64) NOT NULL,
    change_type VARCHAR(32) NOT NULL,
    keyword_text VARCHAR(1024),
    match_type VARCHAR(32),
    old_value VARCHAR(65535),
    new_value VARCHAR(65535),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (snapshot_date, customer_id, campaign_id, ad_group_id, keyword_criterion_id)
);

-- ppc_negative_keyword_snapshot_daily: Campaign- and ad group-level negative keywords.
-- keyword_level: CAMPAIGN | AD_GROUP. ad_group_id empty for campaign-level. Same criterion_id can be campaign- and ad group-level; PK includes ad_group_id.
CREATE TABLE IF NOT EXISTS ppc_negative_keyword_snapshot_daily (
    snapshot_date DATE NOT NULL,
    customer_id VARCHAR(128) NOT NULL,
    campaign_id VARCHAR(64) NOT NULL,
    ad_group_id VARCHAR(64) NOT NULL DEFAULT '',
    criterion_id VARCHAR(64) NOT NULL,
    keyword_text VARCHAR(1024),
    match_type VARCHAR(32),
    keyword_level VARCHAR(32),
    campaign_name VARCHAR(512),
    ad_group_name VARCHAR(512),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (snapshot_date, customer_id, campaign_id, ad_group_id, criterion_id)
);

-- ppc_negative_keyword_diff_daily: Negative keyword changes. change_type: ADDED | REMOVED | MATCH_TYPE_CHANGED | KEYWORD_TEXT_CHANGED | UPDATED. ad_group_id empty for campaign-level.
CREATE TABLE IF NOT EXISTS ppc_negative_keyword_diff_daily (
    snapshot_date DATE NOT NULL,
    customer_id VARCHAR(128) NOT NULL,
    campaign_id VARCHAR(64) NOT NULL,
    ad_group_id VARCHAR(64) NOT NULL DEFAULT '',
    criterion_id VARCHAR(64) NOT NULL,
    change_type VARCHAR(32) NOT NULL,
    keyword_text VARCHAR(1024),
    match_type VARCHAR(32),
    old_value VARCHAR(65535),
    new_value VARCHAR(65535),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (snapshot_date, customer_id, campaign_id, ad_group_id, criterion_id)
);

-- ppc_ad_creative_snapshot_daily: Ad creative snapshot for all ad types (RSA, ETA, call, app, etc.). Includes all ads (ENABLED, PAUSED, REMOVED).
--   ad_type: Ad type (e.g. RESPONSIVE_SEARCH_AD, EXPANDED_TEXT_AD, CALL_AD, APP_AD). One row per ad.
--   status: ad_group_ad.status (ENABLED, PAUSED, REMOVED).
--   headlines_json: JSON array of {text, pinned_field} for each headline (RSA/ETA).
--   descriptions_json: JSON array of {text, pinned_field} for each description (RSA/ETA).
--   path1, path2: Display path (e.g. example.com/Path1/Path2); RSA only.
--   policy_summary_json: JSON with approval_status, review_status, policy_topic_entries. Set when ad has policy issues; helps track disapprovals.
--   asset_urls: JSON array of URLs for asset-based ads (IMAGE_AD image URL, VIDEO_AD YouTube watch URL, etc.). NULL for text-only ad types.
CREATE TABLE IF NOT EXISTS ppc_ad_creative_snapshot_daily (
    snapshot_date DATE NOT NULL,
    customer_id VARCHAR(128) NOT NULL,
    ad_group_id VARCHAR(64) NOT NULL,
    campaign_id VARCHAR(64) NOT NULL,
    ad_id VARCHAR(64) NOT NULL,
    ad_type VARCHAR(64),
    status VARCHAR(32),
    headlines_json VARCHAR(65535),
    descriptions_json VARCHAR(65535),
    final_urls VARCHAR(65535),
    path1 VARCHAR(512),
    path2 VARCHAR(512),
    policy_summary_json VARCHAR(65535),
    asset_urls VARCHAR(65535),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (snapshot_date, customer_id, ad_group_id, ad_id)
);

-- Add asset_urls to existing tables (run once if table was created before this column existed):
-- ALTER TABLE ppc_ad_creative_snapshot_daily ADD COLUMN asset_urls VARCHAR(65535);

-- ppc_ad_creative_diff_daily: Ad creative changes. changed_metric_name: headlines_json | descriptions_json | final_urls | path1 | path2 | policy_summary_json | status | asset_urls.
CREATE TABLE IF NOT EXISTS ppc_ad_creative_diff_daily (
    snapshot_date DATE NOT NULL,
    customer_id VARCHAR(128) NOT NULL,
    ad_group_id VARCHAR(64) NOT NULL,
    ad_id VARCHAR(64) NOT NULL,
    changed_metric_name VARCHAR(128) NOT NULL,
    old_value VARCHAR(65535),
    new_value VARCHAR(65535),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (snapshot_date, customer_id, ad_group_id, ad_id, changed_metric_name)
);

-- ppc_audience_targeting_snapshot_daily: Audience criteria (campaign and ad group level).
--   audience_type: USER_LIST (remarketing) | USER_INTEREST (in-market) | CUSTOM_AFFINITY | CUSTOM_INTENT | COMBINED_AUDIENCE.
--   ad_group_id: Empty string '' for campaign-level audiences (never NULL in app; normalize on read).
--   audience_name: From API (user_list.name, etc.) when available.
--   targeting_mode: OBSERVATION (bid_only=true) vs TARGETING (bid_only=false) from criterion.targeting_setting.
--   audience_size: User count / estimated size at snapshot time (e.g. user_list.size_for_display); NULL if not available.
--   status: Criterion status (e.g. ENABLED, REMOVED). All criteria are fetched (including REMOVED).
--   bid_modifier: Bid adjustment for this audience. negative: TRUE if exclusion.
CREATE TABLE IF NOT EXISTS ppc_audience_targeting_snapshot_daily (
    snapshot_date DATE NOT NULL,
    customer_id VARCHAR(128) NOT NULL,
    campaign_id VARCHAR(64) NOT NULL,
    ad_group_id VARCHAR(64) NOT NULL,
    criterion_id VARCHAR(64) NOT NULL,
    audience_type VARCHAR(64) NOT NULL,
    audience_id VARCHAR(256),
    audience_name VARCHAR(512),
    targeting_mode VARCHAR(32),
    audience_size NUMBER(18, 0),
    status VARCHAR(32),
    bid_modifier NUMBER(6, 4),
    negative BOOLEAN,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (snapshot_date, customer_id, campaign_id, ad_group_id, criterion_id)
);

-- Add audience_size to existing tables (run once if table was created before): ALTER TABLE ppc_audience_targeting_snapshot_daily ADD COLUMN audience_size NUMBER(18, 0);
-- Add status to existing tables (run once if table was created before): ALTER TABLE ppc_audience_targeting_snapshot_daily ADD COLUMN status VARCHAR(32);

-- ppc_audience_targeting_diff_daily: Audience targeting changes. change_type: ADDED | REMOVED | MODE_CHANGED | BID_MODIFIER_CHANGED | UPDATED.
--   old_size, new_size: Audience size at time of change (for size-at-change tracking).
CREATE TABLE IF NOT EXISTS ppc_audience_targeting_diff_daily (
    snapshot_date DATE NOT NULL,
    customer_id VARCHAR(128) NOT NULL,
    campaign_id VARCHAR(64) NOT NULL,
    ad_group_id VARCHAR(64) NOT NULL,
    criterion_id VARCHAR(64) NOT NULL,
    change_type VARCHAR(32) NOT NULL,
    audience_type VARCHAR(64),
    audience_id VARCHAR(256),
    audience_name VARCHAR(512),
    targeting_mode VARCHAR(32),
    old_value VARCHAR(65535),
    new_value VARCHAR(65535),
    old_size NUMBER(18, 0),
    new_size NUMBER(18, 0),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (snapshot_date, customer_id, campaign_id, ad_group_id, criterion_id)
);

-- Add old_size, new_size to existing diff table (run once if needed): ALTER TABLE ppc_audience_targeting_diff_daily ADD COLUMN old_size NUMBER(18, 0), ADD COLUMN new_size NUMBER(18, 0);

-- ppc_campaign_dims: Campaign dimension lookup. Resolves campaign_id to name, status, channel. last_seen_date: when last updated.
CREATE TABLE IF NOT EXISTS ppc_campaign_dims (
    campaign_id VARCHAR(64) NOT NULL,
    customer_id VARCHAR(128) NOT NULL,
    campaign_name VARCHAR(512),
    status VARCHAR(32),
    advertising_channel_type VARCHAR(64),
    last_seen_date DATE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (campaign_id, customer_id)
);

-- ppc_ad_group_dims: Ad group dimension lookup. Resolves ad_group_id to name, campaign_id.
CREATE TABLE IF NOT EXISTS ppc_ad_group_dims (
    ad_group_id VARCHAR(64) NOT NULL,
    campaign_id VARCHAR(64) NOT NULL,
    customer_id VARCHAR(128) NOT NULL,
    ad_group_name VARCHAR(512),
    last_seen_date DATE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (ad_group_id, customer_id)
);

-- ppc_keyword_dims: Keyword dimension lookup. Resolves keyword_criterion_id to keyword_text, match_type, ad_group_id, campaign_id.
CREATE TABLE IF NOT EXISTS ppc_keyword_dims (
    keyword_criterion_id VARCHAR(64) NOT NULL,
    ad_group_id VARCHAR(64) NOT NULL,
    campaign_id VARCHAR(64) NOT NULL,
    customer_id VARCHAR(128) NOT NULL,
    keyword_text VARCHAR(1024),
    match_type VARCHAR(32),
    last_seen_date DATE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (keyword_criterion_id, customer_id)
);

