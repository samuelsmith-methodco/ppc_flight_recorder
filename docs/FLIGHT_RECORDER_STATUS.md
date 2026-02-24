# PPC Flight Recorder — Implementation Status

This document maps each item from [checklist.md](checklist.md) to the current implementation: what is **done**, what is **not done**, and **why**.

---

## TIER 1 — NON-NEGOTIABLE

### 1. Budget Allocation (by top reporting bucket)

| Requirement | Status | Implementation |
|-------------|--------|----------------|
| Date/time of change | ✅ Done | `ppc_campaign_control_diff_daily.snapshot_date`; day-over-day diff captures when budget changed |
| Old value → new value | ✅ Done | `ppc_campaign_control_diff_daily.old_value`, `new_value` for `daily_budget_amount`, `daily_budget_micros` |
| % of total spend per bucket | ❌ Not done | No aggregation by bucket (Brand vs Non-Brand vs PMax). Control state is per-campaign; % would require spend rollup by `advertising_channel_sub_type` or similar |
| Who changed it | ⚠️ Partial | `ppc_change_event_daily` has `user_email`, `client_type` for ChangeEvents. Budget changes appear in change events, but not explicitly linked to `ppc_campaign_control_diff_daily` |
| Reason code (manual note or AI-inferred) | ❌ Not done | Google Ads API does not expose reason codes. Would require manual entry or AI inference from change context |

**Tables:** `ppc_campaign_control_state_daily`, `ppc_campaign_control_diff_daily`, `ppc_change_event_daily`

---

### 2. Bidding Strategy + Bid Targets

| Requirement | Status | Implementation |
|-------------|--------|----------------|
| Strategy type | ✅ Done | `ppc_campaign_control_state_daily.bidding_strategy_type`; diff captures changes |
| Target values | ✅ Done | `target_cpa_micros`, `target_cpa_amount`, `target_roas`, `target_impression_share_*` in control state; diff captures changes |
| Effective date | ✅ Done | Implicit via `snapshot_date` (day-over-day diff shows when change took effect) |
| Learning phase reset flag | ❌ Not done | Google Ads API does not expose a learning-phase reset flag. Would need to infer from strategy change + performance reset |
| Bid modifiers by dimension | ⚠️ Partial | **Device:** `ppc_ad_group_device_modifier_daily` (MOBILE, DESKTOP, TABLET). **Geo/audience:** Not at modifier level; audience bid_modifier in `ppc_audience_targeting_snapshot_daily` |

**Tables:** `ppc_campaign_control_state_daily`, `ppc_campaign_control_diff_daily`, `ppc_ad_group_device_modifier_daily`, `ppc_audience_targeting_snapshot_daily`

---

### 3. Conversion Definitions & Attribution Settings

| Requirement | Status | Implementation |
|-------------|--------|----------------|
| Conversion events list | ✅ Done | `ppc_conversion_action_daily` (one row per conversion action); `ppc_conversion_action_diff_daily` for add/remove/field changes |
| Primary/secondary designation | ✅ Done | `include_in_conversions_metric`, `action_optimization` in `ppc_conversion_action_daily` |
| Attribution model | ✅ Done | `attribution_model` in `ppc_conversion_action_daily` |
| Lookback windows | ✅ Done | `click_through_lookback_window_days` in `ppc_conversion_action_daily` |
| Any changes to event logic | ✅ Done | `ppc_conversion_action_diff_daily` tracks field changes |
| GA4 ↔ Google Ads mapping | ❌ Not done | No explicit mapping table. Would require custom logic to link `conversion_source` (e.g. "Website (Google Analytics (GA4))") to GA4 property |

**Tables:** `ppc_conversion_action_daily`, `ppc_conversion_action_diff_daily`

---

### 4. Campaign On / Off State

| Requirement | Status | Implementation |
|-------------|--------|----------------|
| Campaign ID | ✅ Done | `ppc_campaign_control_state_daily.campaign_id`; status changes in `ppc_campaign_control_diff_daily` |
| State change | ✅ Done | `status` (ENABLED, PAUSED, REMOVED); diff captures `changed_metric_name = 'status'` with old/new value |
| Timestamp | ✅ Done | `snapshot_date` (daily granularity); `ppc_change_event_daily.change_date_time` for more precise time when ChangeEvent exists |
| Trigger (manual vs rule vs automated) | ⚠️ Partial | `ppc_change_event_daily.client_type` indicates source (e.g. `GOOGLE_ADS` for automated rules). Not explicitly joined to control diff; would need to correlate by resource and date |

**Tables:** `ppc_campaign_control_state_daily`, `ppc_campaign_control_diff_daily`, `ppc_ad_group_change_daily` (for ad group add/remove/status), `ppc_change_event_daily`

---

## TIER 2 — HIGH-IMPACT OPTIMIZATION LEVERS

### 5. Keyword & Search Term Control

| Requirement | Status | Implementation |
|-------------|--------|----------------|
| Keyword added/removed | ✅ Done | `ppc_keyword_change_daily` (ADDED, REMOVED, MATCH_TYPE_CHANGED); `ppc_keyword_snapshot_daily` |
| Match type | ✅ Done | `ppc_keyword_snapshot_daily.match_type`; `ppc_keyword_change_daily` tracks match type changes |
| Negative keyword additions | ✅ Done | `ppc_negative_keyword_snapshot_daily`, `ppc_negative_keyword_diff_daily` (ADDED, REMOVED, MATCH_TYPE_CHANGED, KEYWORD_TEXT_CHANGED, UPDATED) |
| Source (search term, intuition, rule) | ❌ Not done | Google Ads API does not expose why a keyword was added. Would require manual tagging or inference |

**Tables:** `ppc_keyword_snapshot_daily`, `ppc_keyword_change_daily`, `ppc_negative_keyword_snapshot_daily`, `ppc_negative_keyword_diff_daily`

---

### 6. Ad Creative Changes (RSA + Assets)

| Requirement | Status | Implementation |
|-------------|--------|----------------|
| Asset versioning | ⚠️ Partial | `ppc_ad_creative_snapshot_daily` stores current state per day; `ppc_ad_creative_diff_daily` captures field-level changes. No explicit version number |
| What changed | ✅ Done | `ppc_ad_creative_diff_daily.changed_metric_name` (headlines_json, descriptions_json, path1, path2, policy_summary_json, status, asset_urls); old/new value |
| Performance before/after | ❌ Not done | Creative diff does not join to outcome metrics. Would require correlating ad_id with `ppc_ad_group_outcomes_daily` or keyword/ad-level metrics |
| Pinning logic | ✅ Done | `headlines_json`, `descriptions_json` include `pinned_field` per asset |

**Tables:** `ppc_ad_creative_snapshot_daily`, `ppc_ad_creative_diff_daily`

---

### 7. Audience Signals & Targeting

| Requirement | Status | Implementation |
|-------------|--------|----------------|
| Audience added/removed | ✅ Done | `ppc_audience_targeting_diff_daily` (ADDED, REMOVED, MODE_CHANGED, BID_MODIFIER_CHANGED, UPDATED) |
| Mode (observe vs target) | ✅ Done | `ppc_audience_targeting_snapshot_daily.targeting_mode` (OBSERVATION vs TARGETING) |
| Size at time of change | ✅ Done | `ppc_audience_targeting_snapshot_daily.audience_size`; `ppc_audience_targeting_diff_daily.old_size`, `new_size` |

**Tables:** `ppc_audience_targeting_snapshot_daily`, `ppc_audience_targeting_diff_daily`

---

## TIER 3 — STRUCTURAL / SETTINGS

### 8. Geo & Location Targeting

| Requirement | Status | Implementation |
|-------------|--------|----------------|
| Geo settings | ✅ Done | `ppc_campaign_geo_targeting_daily` (criterion_id, criterion_type, geo_target_constant, geo_name, negative, radius, proximity, etc.) |
| Inclusion logic | ✅ Done | `positive_geo_target_type`, `negative_geo_target_type` (presence vs interest when API exposes); `negative` flag |
| Effective reach estimate | ✅ Done | `ppc_campaign_geo_targeting_daily.estimated_reach`; `ppc_campaign_geo_targeting_diff_daily` for add/remove/field changes |

**Tables:** `ppc_campaign_geo_targeting_daily`, `ppc_campaign_geo_targeting_diff_daily`

---

### 9. Device Targeting

| Requirement | Status | Implementation |
|-------------|--------|----------------|
| Device modifiers | ✅ Done | `ppc_ad_group_device_modifier_daily` (MOBILE, DESKTOP, TABLET bid_modifier) |
| Date of change | ✅ Done | `ppc_ad_group_device_modifier_diff_daily.snapshot_date`; diff captures add/remove/bid_modifier changes |

**Tables:** `ppc_ad_group_device_modifier_daily`, `ppc_ad_group_device_modifier_diff_daily`

---

### 10. Ad Scheduling

| Requirement | Status | Implementation |
|-------------|--------|----------------|
| Schedule logic | ✅ Done | `ppc_campaign_control_state_daily.ad_schedule_json` (day-of-week, start/end hour/minute, bid_modifier per slot) |
| Modifiers | ✅ Done | Bid modifiers per schedule slot in `ad_schedule_json` |
| Timezone context | ✅ Done | `ppc_campaign_control_state_daily.account_timezone` |

**Tables:** `ppc_campaign_control_state_daily` (ad_schedule_json, account_timezone)

---

## TIER 4 — HYGIENE & AUTOMATION

### 11. Automated Rules

| Requirement | Status | Implementation |
|-------------|--------|----------------|
| Rule logic | ❌ Not done | Google Ads API does not expose rule definitions or trigger conditions |
| Trigger events | ❌ Not done | Same limitation; API only exposes actions taken, not rule config |
| Actions taken | ✅ Done | `ppc_change_event_daily` captures resource_change_operation (CREATE, UPDATE, DELETE), changed_fields, client_type (e.g. GOOGLE_ADS for automated rules) |

**Tables:** `ppc_change_event_daily`

---

### 12. Policy Disapprovals & Fixes

| Requirement | Status | Implementation |
|-------------|--------|----------------|
| Disapproval reason | ⚠️ Partial | `ppc_ad_creative_snapshot_daily.policy_summary_json` stores approval_status, review_status, policy_topic_entries when ad has policy issues |
| Duration | ❌ Not done | No explicit tracking of how long an ad was disapproved |
| Resolution | ❌ Not done | No explicit resolution event; would need to infer from policy_summary_json change (e.g. APPROVED after prior DISAPPROVED) |

**Tables:** `ppc_ad_creative_snapshot_daily`, `ppc_ad_creative_diff_daily` (policy_summary_json changes)

---

## Summary

| Tier | Fully Done | Partial | Not Done |
|------|------------|---------|----------|
| **Tier 1** | 2 of 4 | 2 of 4 | — |
| **Tier 2** | 2 of 3 | 1 of 3 | — |
| **Tier 3** | 3 of 3 | — | — |
| **Tier 4** | 0 of 2 | 1 of 2 | 1 of 2 |

**Key gaps (Google Ads API limitations):**
- Who changed / reason code (API does not expose at budget/keyword level)
- % of total spend per bucket (requires aggregation logic)
- Learning phase reset flag (not exposed)
- Keyword source (search term vs intuition vs rule)
- Automated rule logic and trigger events (API only exposes actions)
- Policy disapproval duration and resolution (partial via policy_summary_json)

**Recommendations:**
1. Add a reporting view to compute % spend per bucket from `ppc_campaign_outcomes_daily` + `ppc_campaign_control_state_daily`.
2. Correlate `ppc_change_event_daily` with control/keyword/creative diffs by resource ID + date to infer "who" and "trigger".
3. For policy disapprovals, derive duration/resolution from day-over-day `policy_summary_json` changes.
