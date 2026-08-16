-- ────────────────────────────────────────────────────────────────────
-- Staging model: standardize the events Bronze table for downstream use.
--
-- Keeps column naming consistent and casts event_ts to a proper
-- timestamp so Gold models can do date arithmetic directly.
-- ────────────────────────────────────────────────────────────────────

SELECT
    event_id,
    customer_id,
    event_type,
    CAST(event_ts AS TIMESTAMP)                     AS event_ts,
    CAST(event_date AS DATE)                        AS event_date,
    schema_version,

    -- Event-type-specific payload fields
    CAST(amount AS DOUBLE)                          AS amount,
    CAST(usage_gb AS DOUBLE)                        AS usage_gb,
    payment_status,
    support_topic,
    plan_from,
    plan_to,
    complaint_severity,
    cancellation_reason,

    -- Ingestion metadata (if present)
    ingestion_timestamp

FROM {{ source('bronze', 'telco_events_bronze') }}
