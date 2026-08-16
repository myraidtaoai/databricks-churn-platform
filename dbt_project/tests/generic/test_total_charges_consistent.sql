-- ────────────────────────────────────────────────────────────────────
-- Custom generic test: total_charges must be present for nonzero tenure
--
-- This replicates the WARN-severity quality rule from quality.py:
--   "total_charges must be present for nonzero tenure"
--
-- In dbt, a test file returns the FAILING rows. If the query returns
-- zero rows, the test passes.
--
-- Usage in schema.yml:
--   tests:
--     - total_charges_consistent
-- ────────────────────────────────────────────────────────────────────

{% test total_charges_consistent(model) %}

SELECT *
FROM {{ model }}
WHERE tenure > 0
  AND total_charges IS NULL

{% endtest %}
