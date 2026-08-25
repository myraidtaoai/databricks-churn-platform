-- ────────────────────────────────────────────────────────────────────
-- Silver model: quality-gated events (insert-only, keyed on event_id)
--
-- This is the dbt equivalent of transform_events.py / EVENT_RULES.
--
-- The PySpark pipeline quarantines rows that fail a QUARANTINE-severity
-- rule (bad rows go to telco_quarantine, good rows continue) and only
-- logs WARN-severity violations without dropping rows. dbt has no
-- runtime quarantine sink, so the same QUARANTINE-severity predicates
-- are applied here as a WHERE filter — a row that would have been
-- quarantined in production is excluded from Silver. WARN-severity
-- checks (amount / usage_gb non-negative, schema_version present) stay
-- as schema.yml tests on stg_events_bronze: they should never fire, but
-- if they do, they surface the anomaly without removing the row here,
-- matching the WARN contract.
--
-- KEY dbt CONCEPT: materialized='incremental' + insert-only merge —
-- events are immutable, so there's no update clause, only new event_ids.
-- ────────────────────────────────────────────────────────────────────

{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='merge',
        file_format='delta',
        on_schema_change='sync_all_columns'
    )
}}

SELECT
    event_id,
    customer_id,
    event_type,
    event_ts,
    event_date,
    schema_version,
    amount,
    usage_gb,
    payment_status,
    support_topic,
    plan_from,
    plan_to,
    complaint_severity,
    cancellation_reason,
    ingestion_timestamp,
    CURRENT_TIMESTAMP() AS _transformed_at

FROM {{ ref('stg_events_bronze') }}

WHERE
    -- event_id_not_null (QUARANTINE)
    event_id IS NOT NULL AND event_id != ''
    -- customer_id_not_null (QUARANTINE)
    AND customer_id IS NOT NULL AND customer_id != ''
    -- event_timestamp_plausible (QUARANTINE)
    AND event_ts >= TIMESTAMP '2020-01-01'
    AND event_ts <  TIMESTAMP '2030-01-01'
    -- event_type_in_contract (QUARANTINE)
    AND event_type IN (
        'billing', 'cancellation', 'complaint', 'payment',
        'plan_change', 'support_call', 'usage'
    )

-- On incremental runs, only scan newly-ingested rows. The MERGE's
-- unique_key (event_id) still guards against re-inserting a row that
-- was already merged in.
{% if is_incremental() %}
AND ingestion_timestamp > (SELECT MAX(ingestion_timestamp) FROM {{ this }})
{% endif %}
