-- ────────────────────────────────────────────────────────────────────
-- Gold model: point-in-time feature snapshot per customer
--
-- This is the dbt equivalent of build_features.py. It aggregates
-- events up to and including as_of_date into windowed features
-- (7d / 30d / 90d) and enriches them with static Silver attributes.
--
-- KEY dbt CONCEPTS demonstrated here:
--   1. CTEs (WITH blocks) — dbt's bread and butter for readable SQL
--   2. var() — project-level variables declared in dbt_project.yml
--   3. Jinja for-loops — generate repetitive SQL without copy-paste
--   4. ref() — references to other dbt models for automatic lineage
--
-- POINT-IN-TIME SAFETY:
--   Every event filter is bounded above by the end of as_of_date. This
--   is the leakage boundary — no future events are ever included.
--
-- as_of_date defaults to yesterday. Override with:
--   dbt run -s gold_feature_snapshot --vars '{as_of_date: "2026-08-01"}'
-- ────────────────────────────────────────────────────────────────────

{{
    config(
        materialized='table',
        file_format='delta'
    )
}}

{#-
  Resolve the snapshot date into a SQL expression.
  An empty var means "yesterday", evaluated by the warehouse.
  A supplied date becomes a literal, e.g. DATE '2026-08-01'.
-#}
{%- set as_of_raw = var('as_of_date', '') | trim -%}
{%- if as_of_raw -%}
    {%- set as_of = "DATE '" ~ as_of_raw ~ "'" -%}
{%- else -%}
    {%- set as_of = "DATE_SUB(CURRENT_DATE(), 1)" -%}
{%- endif -%}

{#- Upper bound: strictly before the start of the following day. -#}
{%- set upper_bound = "CAST(DATE_ADD(" ~ as_of ~ ", 1) AS TIMESTAMP)" -%}

{%- set windows = [7, 30, 90] -%}
{%- set simple_counts = ['support_call', 'plan_change', 'billing', 'cancellation'] -%}

WITH events AS (
    -- All events up to and including the snapshot date (point-in-time boundary).
    SELECT *
    FROM {{ ref('telco_events_silver') }}
    WHERE event_ts < {{ upper_bound }}
),

customers AS (
    SELECT DISTINCT customer_id
    FROM events
),

-- ── Payment features: count + failures per window ────────────────────
{% for days in windows %}
payment_{{ days }}d AS (
    SELECT
        customer_id,
        COUNT(*)                                                   AS payment_count_{{ days }}d,
        SUM(CASE WHEN payment_status = 'failed' THEN 1 ELSE 0 END) AS payment_failures_{{ days }}d
    FROM events
    WHERE event_type = 'payment'
      AND event_ts >= CAST(DATE_SUB({{ as_of }}, {{ days }}) AS TIMESTAMP)
    GROUP BY customer_id
),
{% endfor %}

-- ── Complaint features: count + high-severity count ──────────────────
{% for days in windows %}
complaint_{{ days }}d AS (
    SELECT
        customer_id,
        COUNT(*)                                                      AS complaint_count_{{ days }}d,
        SUM(CASE WHEN complaint_severity = 'high' THEN 1 ELSE 0 END)  AS complaint_high_{{ days }}d
    FROM events
    WHERE event_type = 'complaint'
      AND event_ts >= CAST(DATE_SUB({{ as_of }}, {{ days }}) AS TIMESTAMP)
    GROUP BY customer_id
),
{% endfor %}

-- ── Simple per-type counts: support_call, plan_change, billing, cancellation ──
{% for event_type in simple_counts %}
{% for days in windows %}
{{ event_type }}_{{ days }}d AS (
    SELECT
        customer_id,
        COUNT(*) AS {{ event_type }}_count_{{ days }}d
    FROM events
    WHERE event_type = '{{ event_type }}'
      AND event_ts >= CAST(DATE_SUB({{ as_of }}, {{ days }}) AS TIMESTAMP)
    GROUP BY customer_id
),
{% endfor %}
{% endfor %}

-- ── Usage features ───────────────────────────────────────────────────
latest_usage AS (
    SELECT
        customer_id,
        usage_gb AS latest_usage_gb
    FROM (
        SELECT
            customer_id,
            usage_gb,
            ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY event_ts DESC) AS _rn
        FROM events
        WHERE event_type = 'usage'
    )
    WHERE _rn = 1
),

usage_recent_30d AS (
    SELECT
        customer_id,
        AVG(usage_gb) AS avg_usage_30d
    FROM events
    WHERE event_type = 'usage'
      AND event_ts >= CAST(DATE_SUB({{ as_of }}, 30) AS TIMESTAMP)
    GROUP BY customer_id
),

usage_prior_30d AS (
    SELECT
        customer_id,
        AVG(usage_gb) AS avg_usage_prior_30d
    FROM events
    WHERE event_type = 'usage'
      AND event_ts >= CAST(DATE_SUB({{ as_of }}, 60) AS TIMESTAMP)
      AND event_ts <  CAST(DATE_SUB({{ as_of }}, 30) AS TIMESTAMP)
    GROUP BY customer_id
),

-- ── Days since last activity ─────────────────────────────────────────
last_activity AS (
    SELECT
        customer_id,
        DATEDIFF({{ as_of }}, MAX(event_ts)) AS days_since_last_activity
    FROM events
    GROUP BY customer_id
),

-- ── Silver static attributes (latest snapshot per customer) ──────────
silver_latest AS (
    SELECT
        customer_id,
        tenure,
        monthly_charges,
        total_charges,
        contract,
        internet_service,
        payment_method,
        tech_support,
        senior_citizen
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY snapshot_date DESC) AS _rn
        FROM {{ ref('telco_silver') }}
    )
    WHERE _rn = 1
)

-- ── Final assembly ───────────────────────────────────────────────────
SELECT
    c.customer_id,

    -- Payment features
    {% for days in windows %}
    COALESCE(p{{ days }}.payment_count_{{ days }}d, 0)     AS payment_count_{{ days }}d,
    COALESCE(p{{ days }}.payment_failures_{{ days }}d, 0)  AS payment_failures_{{ days }}d,
    CASE
        WHEN COALESCE(p{{ days }}.payment_count_{{ days }}d, 0) > 0
        THEN CAST(p{{ days }}.payment_failures_{{ days }}d AS DOUBLE)
             / p{{ days }}.payment_count_{{ days }}d
    END                                                    AS payment_failure_rate_{{ days }}d,
    {% endfor %}

    -- Complaint features
    {% for days in windows %}
    COALESCE(cmp{{ days }}.complaint_count_{{ days }}d, 0) AS complaint_count_{{ days }}d,
    COALESCE(cmp{{ days }}.complaint_high_{{ days }}d, 0)  AS complaint_high_{{ days }}d,
    {% endfor %}

    -- Simple per-type counts
    {% for event_type in simple_counts %}
    {% for days in windows %}
    COALESCE({{ event_type }}_{{ days }}d.{{ event_type }}_count_{{ days }}d, 0) AS {{ event_type }}_count_{{ days }}d,
    {% endfor %}
    {% endfor %}

    -- Usage features
    lu.latest_usage_gb,
    ur.avg_usage_30d,
    up.avg_usage_prior_30d,
    CASE
        WHEN up.avg_usage_prior_30d IS NOT NULL AND up.avg_usage_prior_30d > 0
        THEN (ur.avg_usage_30d - up.avg_usage_prior_30d) / up.avg_usage_prior_30d * 100
    END                                                    AS usage_delta_pct,

    -- Activity recency
    la.days_since_last_activity,

    -- Silver static attributes
    s.tenure,
    s.monthly_charges,
    s.total_charges,
    s.contract,
    s.internet_service,
    s.payment_method,
    s.tech_support,
    s.senior_citizen,

    -- Snapshot metadata
    CAST({{ as_of }} AS DATE)                              AS snapshot_date,
    CURRENT_TIMESTAMP()                                    AS _built_at

FROM customers c

{% for days in windows %}
LEFT JOIN payment_{{ days }}d   p{{ days }}   ON c.customer_id = p{{ days }}.customer_id
{% endfor %}

{% for days in windows %}
LEFT JOIN complaint_{{ days }}d cmp{{ days }} ON c.customer_id = cmp{{ days }}.customer_id
{% endfor %}

{% for event_type in simple_counts %}
{% for days in windows %}
LEFT JOIN {{ event_type }}_{{ days }}d ON c.customer_id = {{ event_type }}_{{ days }}d.customer_id
{% endfor %}
{% endfor %}

LEFT JOIN latest_usage     lu ON c.customer_id = lu.customer_id
LEFT JOIN usage_recent_30d ur ON c.customer_id = ur.customer_id
LEFT JOIN usage_prior_30d  up ON c.customer_id = up.customer_id
LEFT JOIN last_activity    la ON c.customer_id = la.customer_id
LEFT JOIN silver_latest    s  ON c.customer_id = s.customer_id
