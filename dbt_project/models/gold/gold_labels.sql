-- ────────────────────────────────────────────────────────────────────
-- Gold model: delayed churn labels for point-in-time feature snapshots
--
-- This is the dbt equivalent of generate_labels.py.
--
-- A customer is labeled churned = 1 if a cancellation event exists in
-- the half-open window (as_of_date, as_of_date + label_horizon_days].
-- Otherwise churned = 0.
--
-- KEY dbt CONCEPT: business parameters live in dbt_project.yml vars
-- rather than being hardcoded, so the same model serves any horizon.
--
-- The label maturity guard (refuse to write labels before the horizon
-- has elapsed) belongs at the orchestration layer — run this model only
-- once as_of_date + label_horizon_days has passed.
--
-- Override:  dbt run -s gold_labels --vars '{as_of_date: "2026-07-01"}'
-- ────────────────────────────────────────────────────────────────────

{{
    config(
        materialized='table',
        file_format='delta'
    )
}}

{%- set as_of_raw = var('as_of_date', '') | trim -%}
{%- if as_of_raw -%}
    {%- set as_of = "DATE '" ~ as_of_raw ~ "'" -%}
{%- else -%}
    {%- set as_of = "DATE_SUB(CURRENT_DATE(), 1)" -%}
{%- endif -%}

{%- set horizon = var('label_horizon_days', 30) | int -%}

WITH snapshot_customers AS (
    -- Every customer that has a feature snapshot for this date.
    SELECT DISTINCT customer_id
    FROM {{ ref('gold_feature_snapshot') }}
    WHERE snapshot_date = {{ as_of }}
),

cancellations AS (
    -- Cancellations strictly after as_of_date, through the end of the
    -- maturity date. Bounds are expressed as whole-day boundaries so the
    -- window is unambiguous.
    SELECT DISTINCT
        customer_id,
        1 AS churned
    FROM {{ ref('stg_events_bronze') }}
    WHERE event_type = 'cancellation'
      AND event_ts >= CAST(DATE_ADD({{ as_of }}, 1) AS TIMESTAMP)
      AND event_ts <  CAST(DATE_ADD({{ as_of }}, {{ horizon + 1 }}) AS TIMESTAMP)
)

SELECT
    sc.customer_id,
    COALESCE(c.churned, 0)                    AS churned,
    CAST({{ as_of }} AS DATE)                 AS snapshot_date,
    {{ horizon }}                             AS label_horizon_days,
    DATE_ADD({{ as_of }}, {{ horizon }})      AS maturity_date,
    CURRENT_TIMESTAMP()                       AS _labeled_at

FROM snapshot_customers sc
LEFT JOIN cancellations c ON sc.customer_id = c.customer_id
