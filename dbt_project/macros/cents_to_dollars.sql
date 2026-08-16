-- ────────────────────────────────────────────────────────────────────
-- Example macro: convert cents to dollars
--
-- This is a simple example to demonstrate how dbt macros work.
-- Macros are reusable Jinja functions you can call from any model.
--
-- Usage in a model:
--   SELECT (( cents_to_dollars('amount_cents') )) AS amount_dollars
--   ...where (( )) stands in for Jinja's double-curly expression tags.
--
--   Note: SQL "--" comments are NOT Jinja comments. Jinja parses the
--   whole file first, so a curly-brace tag inside a "--" line still gets
--   evaluated and can break compilation. Use Jinja comment syntax
--   (hash inside curly braces) when you need to document a tag.
-- ────────────────────────────────────────────────────────────────────

{% macro cents_to_dollars(column_name) %}
    ROUND(CAST({{ column_name }} AS DOUBLE) / 100, 2)
{% endmacro %}
