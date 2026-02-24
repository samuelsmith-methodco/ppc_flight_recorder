# PPC Flight Recorder (Standalone)

This is a **separate project** from Leonardo. It can be run on its own with its own environment and credentials. No dependency on the Leonardo backend or frontend.

## What it does

- Fetches **Google Ads** campaign control state (budget, bidding, status), **geo & location targeting** (include/exclude locations, presence vs interest, radius), and daily outcomes at **campaign**, **ad group**, and **keyword** levels (impressions, clicks, cost, conversions, ROAS, etc.).
- Optionally fetches **GA4** traffic acquisition by session dimension (channel group, source/medium, campaign, etc.) via an Apps Script URL.
- Writes daily snapshots and day-over-day diffs to **Snowflake** (control state, campaign/ad group/keyword outcomes and outcome diffs, GA4 tables).

## Setup

### 1. Create virtualenv and install dependencies

```bash
cd ppc_flight_recorder
python -m venv .venv
.venv\Scripts\activate   # Windows
# source .venv/bin/activate   # macOS/Linux
pip install -r requirements.txt
```

### 2. Environment and credentials

Copy the example env file and set your values:

```bash
copy env.example.txt .env   # Windows
# cp env.example.txt .env   # macOS/Linux
```

Edit `.env` and set:

| Variable | Description |
|----------|-------------|
| **Snowflake** | `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA` |
| **Google Ads** | `GOOGLE_ADS_DEVELOPER_TOKEN`, `GOOGLE_ADS_CLIENT_ID`, `GOOGLE_ADS_CLIENT_SECRET`, `GOOGLE_ADS_REFRESH_TOKEN`, `GOOGLE_ADS_LOGIN_CUSTOMER_ID`, and per-project `GOOGLE_ADS_CUSTOMER_ID_*` or `GOOGLE_ADS_CUSTOMER_ID` |
| **GA4** | `GA4_MARKETING_API_URL` – deployed Apps Script “exec” URL. The handler for `type: "traffic_acquisition_daily"` lives in this repo under `gs_backend/` (TrafficAcquisitionDaily.gs + Post.gs); deploy that web app and use its URL here. |
| **PPC** | `PPC_PROJECTS` – comma-separated list (e.g. `the-pinch,the-nickel`); default `the-pinch` |
| **Scheduler** (server only) | `SYNC_SCHEDULE_TIMEZONE` (e.g. `America/New_York`), `SYNC_SCHEDULE_HOUR` (0–23), `SYNC_SCHEDULE_MINUTE` (0–59); default 9:30 PM EST |

Do **not** commit `.env`.

**HTTP proxy:** If you use `http_proxy` or `https_proxy`, the value must include the scheme (e.g. `http://127.0.0.1:3213`). A value like `127.0.0.1:3213` will cause a parse error.

**Google Ads API v23:** Control-state queries use v23-safe fields only. See the [v23 fields reference](https://developers.google.com/google-ads/api/fields/v23/) when changing GAQL. Removed in v23: `campaign_criterion.location.presence_or_interest`, `campaign_criterion.proximity.latitude_in_micro_degrees` / `longitude_in_micro_degrees`, `ad_group_criterion.device.type`.

### 3. Snowflake tables

Run the DDL in your Snowflake database/schema (same account you use in `.env`):

```bash
# In Snowflake worksheet or CLI, run:
sql/ppc-flight-recorder-tables.sql
```

Creates: `ppc_campaign_control_state_daily`, `ppc_campaign_control_diff_daily`, `ppc_campaign_geo_targeting_daily`, `ppc_campaign_geo_targeting_diff_daily`, `ppc_campaign_outcomes_daily`, `ppc_campaign_outcomes_diff_daily`, `ppc_ad_group_outcomes_daily`, `ppc_ad_group_outcomes_diff_daily`, `ppc_keyword_outcomes_daily`, `ppc_keyword_outcomes_diff_daily`, keyword/negative keyword snapshot and diff tables, ad group snapshot and change tables, `ppc_ad_creative_snapshot_daily`, `ppc_ad_creative_diff_daily`, audience targeting tables, `ppc_ga4_traffic_acquisition_daily`, `ppc_ga4_acquisition_daily`, `ppc_ga4_acquisition_diff_daily`.

## Run

From the `ppc_flight_recorder` folder:

```bash
# Default: sync yesterday for all projects in PPC_PROJECTS
python sync.py

# Specific date
python sync.py --date 2026-02-06

# Single project
python sync.py --project the-pinch

# Include GA4 traffic acquisition
python sync.py --ga4

# Historical backfill: sync last 1 year with batch size 30 days, GA4 included, and diffs computed
python sync.py --start-date 2024-02-06 --end-date 2025-02-06 --batch-days 30 --ga4 --diffs
```

### Partial sync (control-state only modes)

Use one of these flags to update only specific tables (no outcomes, no GA4):

```bash
# All control state tables (campaign, audience, ad creative, keywords, etc.) but no outcomes/GA4
python sync.py --date 2026-02-10 --control-state-only

# Campaign control state + geo targeting (ppc_campaign_control_state_daily, ppc_campaign_control_diff_daily, ppc_campaign_geo_targeting_daily, ppc_campaign_geo_targeting_diff_daily)
python sync.py --date 2026-02-10 --control-state-campaign-only

# Keyword and negative keyword snapshots and diffs only
python sync.py --date 2026-02-10 --control-state-keyword-only

# Ad group snapshot and diff only (ppc_ad_group_snapshot_daily, ppc_ad_group_change_daily)
python sync.py --date 2026-02-10 --control-state-adgroup-only

# Ad creative (all ad types) snapshot and diff only (ppc_ad_creative_snapshot_daily, ppc_ad_creative_diff_daily)
python sync.py --date 2026-02-10 --control-state-adcreative-only

# Audience targeting snapshot and diff only (ppc_audience_targeting_snapshot_daily, ppc_audience_targeting_diff_daily)
python sync.py --date 2026-02-10 --control-state-audience-only

# Device targeting only (ppc_ad_group_device_modifier_daily, ppc_ad_group_device_modifier_diff_daily)
python sync.py --date 2026-02-10 --control-state-device-only

# Change history only – actions taken (ppc_change_event_daily; e.g. by automated rules, UI, API)
python sync.py --date 2026-02-10 --control-state-changes-only

# Conversion definitions only (ppc_conversion_action_daily, ppc_conversion_action_diff_daily – conversion events, primary/secondary, attribution, lookback)
python sync.py --date 2026-02-10 --control-state-conversions-only
```

### CLI reference (sync.py)

| Option | Description |
|--------|-------------|
| `--date YYYY-MM-DD` | Snapshot date (default: yesterday) |
| `--project NAME` | Single project (default: all from PPC_PROJECTS) |
| `--ga4` | Also fetch and store GA4 traffic acquisition |
| `--start-date`, `--end-date` | Historical backfill range (use with `--batch-days`) |
| `--batch-days N` | Chunk size in days for historical fetch (default: 30) |
| `--no-diffs` | Skip outcome/GA4 diffs during historical backfill (default) |
| `--diffs` | Compute outcome/GA4 diffs during historical backfill |
| `--control-state-only` | All control state tables (no outcomes, no GA4) |
| `--control-state-campaign-only` | Only campaign control state, control diff, geo targeting, and geo targeting diff tables |
| `--control-state-keyword-only` | Only keyword and negative keyword snapshot/diff tables |
| `--control-state-adgroup-only` | Only ad group snapshot and change tables |
| `--control-state-adcreative-only` | Only ad creative snapshot and diff tables |
| `--control-state-audience-only` | Only audience targeting snapshot and diff tables |
| `--control-state-device-only` | Only device targeting (ad group device modifiers) snapshot and diff tables |
| `--control-state-changes-only` | Only change history / actions taken (ppc_change_event_daily) |
| `--control-state-conversions-only` | Only conversion definitions (ppc_conversion_action_daily, ppc_conversion_action_diff_daily) |

**Checking geo in Google Ads:** For locations and radius targeting, check **Campaigns** → campaign → **Locations**. Per-criterion geo (including presence/interest when available) is stored in `ppc_campaign_geo_targeting_daily`.

Cron example (daily at 2 AM with GA4):

```cron
0 2 * * * cd /path/to/ppc_flight_recorder && .venv/Scripts/python sync.py --ga4
```

### FastAPI server with daily scheduler (Uvicorn, port 9001)

Runs a small API and a **daily scheduler** that runs the sync (including GA4 and diffs) at a set time every day.

From the `ppc_flight_recorder` folder, activate the virtualenv then start Uvicorn:

```bash
# Windows
.venv\Scripts\activate
uvicorn server:app --host 0.0.0.0 --port 9001
```

```bash
# macOS / Linux
source .venv/bin/activate
uvicorn server:app --host 0.0.0.0 --port 9001
```

The server listens on **port 9001**. To use a different port: `uvicorn server:app --host 0.0.0.0 --port 8000`

- **Scheduler**: Runs daily at `SYNC_SCHEDULE_HOUR:SYNC_SCHEDULE_MINUTE` in `SYNC_SCHEDULE_TIMEZONE` (default **9:30 PM America/New_York**). Syncs **yesterday** only, all projects in `PPC_PROJECTS`, with GA4 and outcome/GA4 diffs.
- **Endpoints**:
  - `GET /health` – health check
  - `GET /schedule` – current schedule and next run time
  - `POST /sync` – trigger sync once. Optional body: `{"date": "YYYY-MM-DD"}`, `{"control_state_only": true}`, `{"control_state_keyword_only": true}`, or `{"control_state_adgroup_only": true}`. Default: yesterday, full sync with GA4 and diffs.

Set in `.env` to change the daily run time (e.g. 9:30 PM EST):

- `SYNC_SCHEDULE_TIMEZONE=America/New_York`
- `SYNC_SCHEDULE_HOUR=21`
- `SYNC_SCHEDULE_MINUTE=30`

## Project structure

```
ppc_flight_recorder/
  .env                 # Your credentials (create from env.example.txt; do not commit)
  .gitignore
  config.py            # Loads .env and exposes config / get_google_ads_customer_id(project)
  snowflake_connection.py
  storage.py           # Snowflake upserts (control state, diff, outcomes, GA4)
  google_ads_client.py  # Google Ads API: control state + campaigns
  ga4_client.py        # GA4 traffic acquisition via Apps Script
  sync.py              # Entry point: python sync.py
  server.py             # FastAPI + Uvicorn (port 9001) with daily sync scheduler
  sql/
    ppc-flight-recorder-tables.sql
  requirements.txt
  env.example.txt      # Template for .env
  README.md
```

## Separation from Leonardo

- **No imports from Leonardo** – all code and config live in `ppc_flight_recorder/`.
- **Own .env** – Snowflake, Google Ads, and GA4 URL are configured here only.
- **Run independently** – use this folder’s venv and `python sync.py`; no need to start the Leonardo backend.

The Leonardo repo can still contain the older backend integration (e.g. `backend/scripts/ppc_flight_recorder_sync.py`); this standalone project is the one intended to be run separately.
FYI:https://developers.google.com/google-ads/api/fields/v20/campaign#campaign.end_date_time