-- =============================================================================
-- PPC Flight Recorder Tables – run in your Snowflake database/schema
-- =============================================================================
-- Migration: ppc_campaign_control_diff_daily is now created (day-over-day control state changes).
-- Migration: If ppc_campaign_outcomes_daily already exists without campaign_name:
--   ALTER TABLE ppc_campaign_outcomes_daily ADD COLUMN campaign_name VARCHAR(512);
-- Migration: Google Ads tables use customer_id (Google Ads customer ID) instead of project name.
--   For existing tables run for each Google Ads table: ALTER TABLE <tbl> RENAME COLUMN project TO customer_id;
--   Then backfill: UPDATE <tbl> SET customer_id = '8945413609' WHERE customer_id = 'the-pinch'; (repeat per project).
-- GA4 tables (ppc_ga4_*) keep project. Google Ads tables: control_state, outcomes, outcomes_diff, *_dims use customer_id.
-- Google Ads provides at ad_group: id, name, campaign; at keyword_view: criterion_id,
-- keyword.text, match_type, ad_group, campaign; metrics: impressions, clicks, cost_micros,
-- conversions, conversions_value, ctr, average_cpc, search_impression_share (etc.).
-- =============================================================================

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
    network_settings_target_google_search BOOLEAN,
    network_settings_target_search_network BOOLEAN,
    network_settings_target_content_network BOOLEAN,
    network_settings_target_partner_search_network BOOLEAN,
    ad_schedule_json VARCHAR(65535),
    audience_target_count INTEGER,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (campaign_id, snapshot_date, customer_id)
);

-- Campaign control state diff: day-over-day changes (budget, bidding strategy, status, etc.)
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

-- Legacy: traffic-only; use ppc_ga4_acquisition_daily for all report types (traffic, user, overview).
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

-- GA4 acquisition: all report types (traffic_acquisition, user_acquisition, acquisition_overview). PPC Flight Recorder rule: state table.
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

-- GA4 acquisition diff: day-over-day changes in metrics. PPC Flight Recorder rule: diff table for GA4.
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

-- Google Ads campaign outcomes diff: day-over-day changes in performance.
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

-- Ad group level outcomes (same metrics as campaign). Google Ads: ad_group.id, campaign.id, segments.date, metrics.*.
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

-- Ad group outcomes diff: day-over-day changes.
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

-- Keyword level outcomes. Google Ads: ad_group_criterion.criterion_id (keyword), ad_group.id, campaign.id, keyword.text, segments.date, metrics.*.
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

-- Keyword outcomes diff: day-over-day changes.
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

-- Dimension/lookup tables: resolve IDs to names and key details (for viewing outcomes by name).
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

