-- Backfill ppc_campaign_control_diff_daily for Spacious Accommodations (campaign_id 23740783102).
-- Snapshot date: 2026-04-08. customer_id: 8945413609.
-- Run once in the target Snowflake database/schema.

MERGE INTO ppc_campaign_control_diff_daily AS target
USING (
    SELECT
        '23740783102' AS campaign_id,
        '2026-04-08'::DATE AS snapshot_date,
        '8945413609' AS customer_id,
        'campaign_added' AS changed_metric_name,
        NULL AS old_value,
        'Spacious Accommodations' AS new_value
) AS source
ON target.campaign_id = source.campaign_id
   AND target.snapshot_date = source.snapshot_date
   AND target.customer_id = source.customer_id
   AND target.changed_metric_name = source.changed_metric_name
WHEN MATCHED THEN UPDATE SET
    old_value = source.old_value,
    new_value = source.new_value
WHEN NOT MATCHED THEN INSERT (campaign_id, snapshot_date, customer_id, changed_metric_name, old_value, new_value)
VALUES (source.campaign_id, source.snapshot_date, source.customer_id, source.changed_metric_name, source.old_value, source.new_value);
