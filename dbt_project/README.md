# dbt Project — Telco Churn Platform

This dbt project handles the SQL transformation layer of the churn platform. It replaces (or runs alongside) the PySpark transformation scripts for Bronze → Silver → Gold.

The ML pipeline (training, scoring, promotion, rollback) stays as PySpark — dbt is a transformation tool, not an ML orchestrator.

## Setup

### 1. Install dbt-databricks

```bash
# Create or activate your virtual environment
cd databricks-churn-platform
source .venv/bin/activate   # or: python -m venv .venv && source .venv/bin/activate

# Install dbt with the Databricks adapter
pip install dbt-databricks
```

Verify the installation:

```bash
dbt --version
```

You should see `dbt-core` and `dbt-databricks` in the output.

### 2. Configure connection

dbt needs three pieces of information from your Databricks workspace:

| Setting | Where to find it |
|---|---|
| **Host** | Your workspace URL, e.g. `adb-1234567890.12.azuredatabricks.net` |
| **HTTP Path** | SQL Warehouse → Connection Details → HTTP Path |
| **Token** | User Settings → Developer → Access Tokens → Generate New Token |

Set them as environment variables (recommended):

```bash
export DBT_DATABRICKS_HOST="adb-xxx.xx.azuredatabricks.net"
export DBT_DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/xxxxx"
export DBT_DATABRICKS_TOKEN="dapi_xxxxxxxx"
```

Or copy `profiles.yml` to `~/.dbt/profiles.yml` and fill in the values directly.

### 3. Install dbt packages

```bash
cd dbt_project
dbt deps
```

This installs `dbt_utils` (used for composite uniqueness tests and range checks).

### 4. Test the connection

```bash
dbt debug
```

This verifies dbt can reach your Databricks workspace. Every line should show `OK`.

## Running dbt

### Essential commands

```bash
# Run all models (staging → silver → gold)
dbt run

# Run a specific model
dbt run -s telco_silver

# Run everything downstream of Silver
dbt run -s telco_silver+

# Run all tests
dbt test

# Run tests for one model
dbt test -s telco_silver

# Full refresh (rebuild from scratch, ignore incremental logic)
dbt run --full-refresh

# Generate and serve the documentation site
dbt docs generate
dbt docs serve
```

### With variable overrides

```bash
# Use a different catalog and schema (these come from the profile,
# so set them as environment variables, not --vars)
DBT_CATALOG=my_catalog DBT_SCHEMA=churn_alex dbt run

# Only when Bronze is stored separately from the model target
DBT_SOURCE_CATALOG=shared DBT_SOURCE_SCHEMA=churn_bronze dbt run

# Build features for a specific date
dbt run -s gold_feature_snapshot --vars '{as_of_date: "2026-08-01"}'

# Generate labels with a custom horizon
dbt run -s gold_labels --vars '{as_of_date: "2026-07-01", label_horizon_days: 30}'
```

### Running in the Databricks workflow

The `churn_workflow.yml` can include a dbt task alongside the PySpark ML tasks:

```yaml
- task_key: dbt_transform
  dbt_task:
    project_directory: ../dbt_project
    commands:
      - "dbt deps"
      - "dbt run --select staging silver gold"
      - "dbt test"
    warehouse_id: ${var.warehouse_id}
```

## Project structure

```
dbt_project/
├── dbt_project.yml           # Project config: name, vars, materializations
├── profiles.yml              # Connection config (Databricks host/token)
├── packages.yml              # External packages (dbt_utils)
│
├── models/
│   ├── sources.yml           # Bronze tables defined as dbt sources
│   │
│   ├── staging/              # 1:1 with sources — rename, cast, standardize
│   │   ├── stg_telco_bronze.sql
│   │   ├── stg_events_bronze.sql
│   │   └── schema.yml
│   │
│   ├── silver/               # Conformed, deduplicated (incremental MERGE)
│   │   ├── telco_silver.sql
│   │   └── schema.yml
│   │
│   └── gold/                 # Business-ready tables for ML and dashboards
│       ├── gold_feature_snapshot.sql   # Point-in-time features
│       ├── gold_labels.sql             # Delayed churn labels
│       ├── training_dataset.sql        # View: features + labels
│       ├── churn_summary.sql           # Aggregate for dashboards
│       └── schema.yml
│
├── macros/
│   ├── cents_to_dollars.sql            # Example reusable macro
│   └── generate_schema_name.sql        # Schema override (common pattern)
│
└── tests/
    └── generic/
        └── test_total_charges_consistent.sql  # Custom quality test
```

## Key dbt concepts in this project

### Sources vs. Models

**Sources** (`sources.yml`) are tables dbt reads from but does not create. Here, the Bronze tables are sources because they are produced by the PySpark ingestion jobs.

**Models** (`.sql` files) are tables/views dbt creates. Each `.sql` file = one table or view.

### ref() and source()

`{{ ref('telco_silver') }}` references another dbt model. dbt uses this to build a dependency graph (DAG) and run models in the correct order.

`{{ source('bronze', 'telco_bronze') }}` references a source table. It resolves to the fully qualified table name using the catalog/schema from `sources.yml`, which in turn reads them from the active profile target.

### Where catalog and schema come from

By default there is one source of truth: the **profile target** in `profiles.yml`, populated from the `DBT_CATALOG` and `DBT_SCHEMA` environment variables. Models, sources, and the `generate_schema_name` macro all resolve to `target.database` / `target.schema`. If Bronze is intentionally stored elsewhere, set `DBT_SOURCE_CATALOG` and/or `DBT_SOURCE_SCHEMA` for the source location.

Do not set `+schema:` in `dbt_project.yml` using `var()` — dbt evaluates those configs before project vars are bound, and parsing fails with `Required var 'schema' not found`.

### Materializations

| Type | Behavior | Used for |
|---|---|---|
| `view` | Creates a SQL view (no stored data) | Staging models, training_dataset |
| `table` | Drops and recreates the full table each run | Gold models, churn_summary |
| `incremental` | Only processes new/changed rows via MERGE | Silver (telco_silver) |
| `ephemeral` | Inlined as a CTE, no object created | Intermediate CTEs (not used here) |

### Incremental models

`telco_silver.sql` uses `materialized='incremental'` with `incremental_strategy='merge'`. On the first run, dbt creates the full table. On subsequent runs, it only processes rows matching the `{% if is_incremental() %}` filter and uses MERGE to upsert.

Run `dbt run --full-refresh -s telco_silver` to rebuild from scratch.

### Tests

dbt tests come in two flavors:

**Schema tests** (in `schema.yml`): declarative, no SQL needed.
- `not_null`, `unique`, `accepted_values`, `relationships`
- `dbt_utils.unique_combination_of_columns`
- `dbt_utils.accepted_range`

**Custom generic tests** (in `tests/generic/`): SQL that returns failing rows.
- See `test_total_charges_consistent.sql`

### Jinja templating

The `gold_feature_snapshot.sql` model demonstrates Jinja loops (`{% for days in [7, 30, 90] %}`) to generate repetitive SQL without copy-paste. This is a key interview talking point — it shows you can write DRY, maintainable transformations.

### The `generate_schema_name` macro

By default, dbt concatenates `target.schema` + `custom_schema` (e.g., `churn_dev_silver`). The override in `macros/generate_schema_name.sql` makes dbt use the exact schema you specify, matching the PySpark pipeline's behavior of writing everything to one schema. This is an extremely common pattern in production.

## Troubleshooting

**`Package dbt_utils 2 does not contain a dbt_project.yml file`**

iCloud Drive syncs the `Documents` folder and creates duplicate directories with a ` 2` suffix when it detects a conflict. dbt scans every subdirectory of `dbt_packages/` and fails on the copy. Fix:

```bash
dbt clean && dbt deps
```

To stop it recurring, keep `dbt_packages/` and `target/` out of sync by adding them to `.gitignore`, and consider moving the repo out of `~/Documents` to something like `~/dev/`.

**`Required var 'schema' not found in config`**

Do not use `{{ var('schema') }}` inside the `models:` block of `dbt_project.yml`. Those configs are evaluated at parse time, before project vars are bound. Catalog and schema belong in the profile target instead.

**`unexpected HTTP status 404 Not Found` with `:443` in the URL**

`DBT_DATABRICKS_HOST` must be the bare hostname — no `https://` prefix, no trailing slash.

## How this maps to the existing PySpark pipeline

| PySpark script | dbt model | Notes |
|---|---|---|
| `transform.py` (column selection) | `stg_telco_bronze.sql` | Rename, cast, trim |
| `transform.py` (MERGE + quality) | `telco_silver.sql` | Incremental merge; quality rules become dbt tests |
| `transform.py` (Gold aggregate) | `churn_summary.sql` | Simple GROUP BY |
| `build_features.py` | `gold_feature_snapshot.sql` | Windowed aggregates with Jinja loops |
| `generate_labels.py` | `gold_labels.sql` + `training_dataset.sql` | Delayed labels + view |
| `quality.py` (rules) | `schema.yml` (tests) | Declarative instead of imperative |
| `contracts.py` | `sources.yml` + `schema.yml` | Column definitions + tests |

## What stays as PySpark (not dbt)

- Event generator (`generate_events.py`) — file I/O, not SQL
- Auto Loader ingestion (`ingest_events.py`) — streaming checkpoint management
- ML training (`train.py`) — MLflow, XGBoost, SHAP
- Model promotion (`promote.py`) — Unity Catalog model registry
- Batch scoring (`score.py`) — MLflow Spark UDF
- Rollback (`rollback.py`) — model alias management
- Drift monitoring (`monitor.py`) — statistical calculations + table writes
