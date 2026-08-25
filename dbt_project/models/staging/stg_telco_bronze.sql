-- ────────────────────────────────────────────────────────────────────
-- Staging model: clean and rename columns from the raw Bronze table.
--
-- This is the dbt equivalent of the column selection/casting block in
-- transform.py. No business logic — just type casts, trims, and
-- consistent snake_case naming.
--
-- WHY a staging model?
-- In dbt convention, staging models are 1:1 with sources. They
-- standardize naming and types so downstream Silver/Gold models
-- never touch raw column names directly.
-- ────────────────────────────────────────────────────────────────────

SELECT
    TRIM(customerID)                                AS customer_id,
    CURRENT_DATE()                                  AS snapshot_date,

    -- Demographics
    TRIM(gender)                                    AS gender,
    CAST(SeniorCitizen AS INT)                      AS senior_citizen,
    TRIM(Partner)                                   AS partner,
    TRIM(Dependents)                                AS dependents,

    -- Account
    CAST(tenure AS INT)                             AS tenure,
    TRIM(PhoneService)                              AS phone_service,
    TRIM(MultipleLines)                             AS multiple_lines,
    TRIM(InternetService)                           AS internet_service,
    TRIM(OnlineSecurity)                            AS online_security,
    TRIM(OnlineBackup)                              AS online_backup,
    TRIM(DeviceProtection)                          AS device_protection,
    TRIM(TechSupport)                               AS tech_support,
    TRIM(StreamingTV)                               AS streaming_tv,
    TRIM(StreamingMovies)                           AS streaming_movies,
    TRIM(Contract)                                  AS contract,
    TRIM(PaperlessBilling)                          AS paperless_billing,
    TRIM(PaymentMethod)                             AS payment_method,

    -- Financials
    CAST(MonthlyCharges AS DOUBLE)                  AS monthly_charges,
    CASE
        WHEN TRIM(TotalCharges) = '' THEN NULL
        ELSE CAST(TotalCharges AS DOUBLE)
    END                                             AS total_charges,

    -- Target
    INITCAP(TRIM(Churn))                            AS churn,
    CASE LOWER(TRIM(Churn))
        WHEN 'yes' THEN 1
        WHEN 'no'  THEN 0
        ELSE NULL
    END                                             AS churn_label,

    -- Metadata
    CURRENT_TIMESTAMP()                             AS _transformed_at

FROM {{ source('bronze', 'telco_bronze') }}
