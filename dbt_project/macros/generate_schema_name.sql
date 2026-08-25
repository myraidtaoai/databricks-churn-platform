-- ────────────────────────────────────────────────────────────────────
-- Override: use the schema name from dbt_project.yml directly
--
-- By default, dbt appends the custom schema to the target schema
-- (e.g., "churn_dev_silver"). This override makes it use the exact
-- schema specified in the model config, matching how the PySpark
-- pipeline writes to a single schema.
--
-- This is a very common override in production dbt projects.
-- ────────────────────────────────────────────────────────────────────

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}
    {%- endif -%}
{%- endmacro %}
