-- ────────────────────────────────────────────────────────────────────
-- Gold model: churn rate summary by contract, internet service, and
-- payment method.
--
-- This is the dbt equivalent of the Gold aggregate at the end of
-- transform.py.  Materialized as a table for dashboard performance.
-- ────────────────────────────────────────────────────────────────────

SELECT
    contract,
    internet_service,
    payment_method,
    COUNT(*)                                AS customers,
    SUM(churn_label)                        AS churned_customers,
    ROUND(AVG(CAST(churn_label AS DOUBLE)), 4)  AS churn_rate,
    AVG(monthly_charges)                    AS avg_monthly_charge,
    AVG(CAST(tenure AS DOUBLE))             AS avg_tenure

FROM {{ ref('telco_silver') }}

GROUP BY
    contract,
    internet_service,
    payment_method
