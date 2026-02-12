# PPC Flight Recorder Checklist

These are the levers that move revenue and efficiency. Track implementation status in [FLIGHT_RECORDER_STATUS.md](FLIGHT_RECORDER_STATUS.md).

---

## TIER 1 — NON-NEGOTIABLE (CAPTURE 100% OF THE TIME)

These are the levers that actually move revenue and efficiency. If the flight recorder misses these, the system is blind.

### 1. Budget Allocation (by top reporting bucket)

**Examples**
- [ ] Brand vs Non-Brand vs Performance Max
- [ ] Daily budget caps
- [ ] Budget reallocation between campaigns

**Change frequency:** Daily to weekly (often reactive)

**Why it matters:** Budget is strategy in PPC. Most real "decisions" happen here, not in keywords.

**Flight recorder must capture**
- [ ] Date/time of change
- [ ] Old value → new value
- [ ] % of total spend per bucket
- [ ] Who changed it
- [ ] Reason code (manual note or AI-inferred)

---

### 2. Bidding Strategy + Bid Targets

**Examples**
- [ ] Max Conversions → tCPA
- [ ] tROAS targets
- [ ] Manual CPC → Smart bidding
- [ ] Bid adjustments (device, geo, audience)

**Change frequency:** Weekly to monthly (but impacts daily performance)

**Why it matters:** This tells Google what winning means. Silent killer when changed without context.

**Flight recorder must capture**
- [ ] Strategy type
- [ ] Target values
- [ ] Effective date
- [ ] Learning phase reset flag
- [ ] Bid modifiers by dimension

---

### 3. Conversion Definitions & Attribution Settings

**Examples**
- [ ] What counts as a "conversion"
- [ ] Primary vs secondary conversions
- [ ] Attribution model (data-driven, last click)
- [ ] GA4 ↔ Google Ads mapping

**Change frequency:** Rare, but catastrophic when changed

**Why it matters:** This rewires the brain of the system. 90% of "why did performance break?" moments live here.

**Flight recorder must capture**
- [ ] Conversion events list
- [ ] Primary/secondary designation
- [ ] Attribution model
- [ ] Lookback windows
- [ ] Any changes to event logic

---

### 4. Campaign On / Off State

**Examples**
- [ ] Pausing campaigns
- [ ] Launching new campaigns
- [ ] Emergency shutdowns

**Change frequency:** Sporadic, but high impact

**Why it matters:** Binary decisions with outsized consequences

**Flight recorder must capture**
- [ ] Campaign ID
- [ ] State change
- [ ] Timestamp
- [ ] Trigger (manual vs rule vs automated)

---

## TIER 2 — HIGH-IMPACT OPTIMIZATION LEVERS

These don't change what you're buying, but they heavily affect how efficiently you buy it.

### 5. Keyword & Search Term Control

**Examples**
- [ ] Adding/removing keywords
- [ ] Match type changes
- [ ] Negative keywords (especially search-term based)

**Change frequency:** Daily to weekly

**Why it matters:** Controls intent purity and CPC inflation. This is where experienced operators earn their keep.

**Flight recorder must capture**
- [ ] Keyword added/removed
- [ ] Match type
- [ ] Negative keyword additions
- [ ] Source (search term, intuition, rule)

---

### 6. Ad Creative Changes (RSA + Assets)

**Examples**
- [ ] Headline swaps
- [ ] Description changes
- [ ] Asset pinning/unpinning
- [ ] Image/video asset updates

**Change frequency:** Weekly to monthly

**Why it matters:** Creative drives CTR → Quality Score → CPC. Hard to diagnose without history.

**Flight recorder must capture**
- [ ] Asset versioning
- [ ] What changed
- [ ] Performance before/after
- [ ] Pinning logic

---

### 7. Audience Signals & Targeting

**Examples**
- [ ] In-market segments
- [ ] Custom intent audiences
- [ ] Remarketing pools
- [ ] Observation vs targeting

**Change frequency:** Weekly to monthly

**Why it matters:** Influences auction eligibility and bid bias. Often misunderstood, frequently misused.

**Flight recorder must capture**
- [ ] Audience added/removed
- [ ] Mode (observe vs target)
- [ ] Size at time of change

---

## TIER 3 — STRUCTURAL / SETTINGS (LOWER FREQUENCY, STILL IMPORTANT)

These are usually "set and forget," but when they change, they explain weirdness.

### 8. Geo & Location Targeting

**Examples**
- [ ] Include/exclude locations
- [ ] "Presence" vs "interest"
- [ ] Radius targeting

**Change frequency:** Monthly or campaign-specific

**Why it matters:** Impacts reach and CPC volatility. Silent leak when misconfigured.

**Flight recorder must capture**
- [ ] Geo settings
- [ ] Inclusion logic
- [ ] Effective reach estimate

---

### 9. Device Targeting

**Examples**
- [ ] Mobile bid adjustments
- [ ] Desktop exclusions
- [ ] Tablet modifiers

**Change frequency:** Monthly

**Why it matters:** Strongly tied to conversion behavior

**Flight recorder must capture**
- [ ] Device modifiers
- [ ] Date of change

---

### 10. Ad Scheduling

**Examples**
- [ ] Day-parting
- [ ] Hourly bid modifiers

**Change frequency:** Monthly or seasonal

**Why it matters:** Helps efficiency, rarely drives step-change results

**Flight recorder must capture**
- [ ] Schedule logic
- [ ] Modifiers
- [ ] Timezone context

---

## TIER 4 — HYGIENE & AUTOMATION (LOW SIGNAL, LOG LIGHTLY)

Track these for completeness, but don't overweight them.

### 11. Automated Rules

**Examples**
- [ ] Pause on CPA thresholds
- [ ] Budget increase rules
- [ ] Keyword pausing logic

**Change frequency:** Rare

**Why it matters:** Can override human intent unexpectedly

**Flight recorder must capture**
- [ ] Rule logic
- [ ] Trigger events
- [ ] Actions taken

---

### 12. Policy Disapprovals & Fixes

**Examples**
- [ ] Ad disapprovals
- [ ] Appeals
- [ ] Landing page fixes

**Change frequency:** Ad hoc

**Why it matters:** Explains sudden drops in delivery

**Flight recorder must capture**
- [ ] Disapproval reason
- [ ] Duration
- [ ] Resolution
