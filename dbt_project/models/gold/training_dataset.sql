-- ────────────────────────────────────────────────────────────────────
-- Gold model: training dataset view joining features to matured labels
--
-- This is the dbt equivalent of the CREATE OR REPLACE VIEW in
-- generate_labels.py. It joins gold_feature_snapshot to gold_labels
-- on (customer_id, snapshot_date) and exposes only rows with labels.
--
-- The ML training job reads from this view directly.
-- ────────────────────────────────────────────────────────────────────

{{
    config(
        materialized='view'
    )
}}

SELECT
    f.*,
    l.churned,
    l.label_horizon_days,
    l.maturity_date

FROM {{ ref('gold_feature_snapshot') }} f
INNER JOIN {{ ref('gold_labels') }} l
    ON f.customer_id = l.customer_id
   AND f.snapshot_date = l.snapshot_date
