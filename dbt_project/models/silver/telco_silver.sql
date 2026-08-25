-- ────────────────────────────────────────────────────────────────────
-- Silver model: conformed, deduplicated customer table (incremental MERGE)
--
-- This is the dbt equivalent of transform.py's Silver MERGE logic.
--
-- KEY dbt CONCEPTS demonstrated here:
--   1. materialized='incremental' — only processes new/changed data
--   2. unique_key — tells dbt what columns identify a row for MERGE
--   3. incremental_strategy='merge' — uses MERGE INTO (not INSERT)
--   4. is_incremental() — Jinja macro that returns true on subsequent
--      runs (false on full refresh)
--
-- Run `dbt run --full-refresh -s telco_silver` to rebuild from scratch.
-- ────────────────────────────────────────────────────────────────────

{{
    config(
        materialized='incremental',
        unique_key=['customer_id', 'snapshot_date'],
        incremental_strategy='merge',
        file_format='delta',
        on_schema_change='sync_all_columns'
    )
}}

SELECT
    customer_id,
    snapshot_date,
    gender,
    senior_citizen,
    partner,
    dependents,
    tenure,
    phone_service,
    multiple_lines,
    internet_service,
    online_security,
    online_backup,
    device_protection,
    tech_support,
    streaming_tv,
    streaming_movies,
    contract,
    paperless_billing,
    payment_method,
    monthly_charges,
    total_charges,
    churn,
    churn_label,
    _transformed_at

FROM {{ ref('stg_telco_bronze') }}

-- On incremental runs, only process rows not already in the table.
-- The MERGE strategy handles updates automatically via unique_key.
{% if is_incremental() %}
WHERE snapshot_date >= (SELECT MAX(snapshot_date) FROM {{ this }})
{% endif %}
