"""Create cleaned Silver events from Bronze events (idempotent).

Applies ``EVENT_RULES`` to ``telco_events_bronze``: malformed rows (missing
event_id/customer_id, implausible timestamps, unknown event_type) are
quarantined into ``telco_quarantine`` with their violation reason rather
than silently poisoning downstream feature windows and labels.

``telco_events_silver`` is append-only and keyed on ``event_id`` — events
are immutable, so a MERGE only ever inserts new events, never updates.
Re-running against an unchanged Bronze table is a no-op.
"""

from __future__ import annotations

import _path_helper  # noqa: F401 — adds churn_pipeline/ to sys.path

import argparse
import json
from datetime import datetime, timezone

from common import get_spark, table
from ops.run_logger import log_run
from pyspark.sql import functions as F
from quality import EVENT_RULES, apply_quality_rules

spark = get_spark()

parser = argparse.ArgumentParser(
    description="Transform Bronze events into Silver via quality gating (MERGE)."
)
parser.add_argument("--catalog", required=True)
parser.add_argument("--schema", required=True)
parser.add_argument(
    "--run-id",
    default="",
    help="Pipeline run ID for quality-metric lineage.  Defaults to a timestamp.",
)
args = parser.parse_args()

_run_started = datetime.now(timezone.utc)
run_id = args.run_id.strip() or f"transform-events-{_run_started.date().isoformat()}"

# ── Read Bronze ─────────────────────────────────────────────────────────
bronze_table = table(args.catalog, args.schema, "telco_events_bronze")
bronze = spark.table(bronze_table)

silver_columns = [
    "schema_version",
    "event_id",
    "generation_id",
    "event_type",
    "event_timestamp",
    "event_date",
    "customer_id",
    "amount",
    "usage_gb",
    "payment_status",
    "support_topic",
    "plan_from",
    "plan_to",
    "complaint_severity",
    "cancellation_reason",
    "event_ts",
    "ingestion_timestamp",
]
missing_columns = sorted(set(silver_columns).difference(bronze.columns))
if missing_columns:
    raise ValueError(
        f"Bronze events table is missing required columns: {missing_columns}"
    )

silver = bronze.select(*silver_columns).withColumn(
    "_transformed_at", F.lit(datetime.now(timezone.utc)).cast("timestamp")
)

# ── Data quality — quarantine bad rows, continue with the rest ─────────
silver, quality_metrics = apply_quality_rules(
    df=silver,
    rules=EVENT_RULES,
    spark=spark,
    catalog=args.catalog,
    schema=args.schema,
    run_id=run_id,
    stage="bronze_events",
)

quarantined_count = sum(
    m.violating_rows for m in quality_metrics if m.severity == "quarantine"
)

# ── Write Silver via MERGE (insert-only; events are immutable) ─────────
silver_table = table(args.catalog, args.schema, "telco_events_silver")
table_exists = spark.catalog.tableExists(silver_table)

if not table_exists:
    silver.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(silver_table)
    merge_status = "created"
else:
    staging_view = "_events_silver_staging"
    silver.createOrReplaceTempView(staging_view)

    merge_sql = f"""
    MERGE INTO {silver_table} AS target
    USING {staging_view} AS source
    ON target.event_id = source.event_id
    WHEN NOT MATCHED THEN INSERT *
    """
    spark.sql(merge_sql)
    merge_status = "merged"

row_count = spark.table(silver_table).count()

_summary = {
    "status": merge_status,
    "silver_table": silver_table,
    "silver_rows": row_count,
    "quarantined_rows": quarantined_count,
    "quality_rules_evaluated": len(quality_metrics),
}
print(json.dumps(_summary, sort_keys=True))

log_run(
    spark=spark,
    catalog=args.catalog,
    schema=args.schema,
    task_name="transform_events",
    run_id=run_id,
    status="succeeded",
    started_at=_run_started,
    finished_at=datetime.now(timezone.utc),
    output_summary=_summary,
)
