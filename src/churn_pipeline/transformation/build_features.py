"""Build point-in-time feature snapshots from the events Silver table.

For a given ``as_of_date``, this job aggregates only events with
``event_ts <= as_of_date`` (strictly no future leakage).  The output is
``gold_feature_snapshot``, partitioned by ``snapshot_date``.

Windowed aggregates (7 / 30 / 90 days) capture the behavioural signal
that makes churn prediction meaningful:

- Payment failure rate
- Support call volume
- Complaint count and severity distribution
- Usage trend (current vs. prior period)
- Days since last activity
- Plan change count

The job is idempotent: re-running the same ``as_of_date`` overwrites
exactly that partition.

Design decisions documented in docs/architecture.md:
  - Point-in-time boundary is ``event_ts <= as_of_date``, never ``<``.
  - Features use ``event_ts`` (business time), never ``ingestion_timestamp``.
  - Silver static attributes are joined to enrich the snapshot.
"""

from __future__ import annotations

import _path_helper  # noqa: F401 — adds churn_pipeline/ to sys.path

import argparse
import json
from datetime import date, datetime, timedelta, timezone

from common import get_spark, table
from ops.run_logger import log_run
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

spark = get_spark()

parser = argparse.ArgumentParser(
    description="Build point-in-time feature snapshot for a given as_of_date."
)
parser.add_argument("--catalog", required=True)
parser.add_argument("--schema", required=True)
parser.add_argument(
    "--as-of-date",
    default="",
    help="Business date for the snapshot (YYYY-MM-DD). Defaults to yesterday UTC.",
)
args = parser.parse_args()

_run_started = datetime.now(timezone.utc)

as_of_date: date = (
    date.fromisoformat(args.as_of_date)
    if args.as_of_date.strip()
    else _run_started.date() - timedelta(days=1)
)

# ── Load events up to (and including) as_of_date ─────────────────────
events_table = table(args.catalog, args.schema, "telco_events_silver")
events = spark.table(events_table).filter(
    F.col("event_ts") <= F.lit(str(as_of_date) + "T23:59:59")
)

if events.limit(1).count() == 0:
    print(
        json.dumps(
            {
                "status": "skipped",
                "reason": "no events on or before as_of_date",
                "as_of_date": as_of_date.isoformat(),
            },
            sort_keys=True,
        )
    )
    raise SystemExit(0)


# ── Helper: windowed count/sum ───────────────────────────────────────


def _window_aggs(
    df: DataFrame,
    as_of: date,
    event_type: str,
    windows: tuple[int, ...] = (7, 30, 90),
) -> DataFrame:
    """Count events per customer within each lookback window."""
    typed = df.filter(F.col("event_type") == event_type)
    result = typed.select("customer_id").distinct()
    for days in windows:
        start = as_of - timedelta(days=days)
        windowed = (
            typed.filter(F.col("event_ts") >= F.lit(str(start) + "T00:00:00"))
            .groupBy("customer_id")
            .agg(F.count("*").alias(f"{event_type}_count_{days}d"))
        )
        result = result.join(windowed, "customer_id", "left")
    return result


# ── Aggregate features ───────────────────────────────────────────────

# All distinct customers in the events table up to as_of_date.
customers = events.select("customer_id").distinct()

# Payment features: count + failure rate per window.
payment_events = events.filter(F.col("event_type") == "payment")
payment_features = customers
for days in (7, 30, 90):
    start = as_of_date - timedelta(days=days)
    w = payment_events.filter(F.col("event_ts") >= F.lit(str(start) + "T00:00:00"))
    agg = w.groupBy("customer_id").agg(
        F.count("*").alias(f"payment_count_{days}d"),
        F.sum(F.when(F.col("payment_status") == "failed", 1).otherwise(0)).alias(
            f"payment_failures_{days}d"
        ),
    )
    payment_features = payment_features.join(agg, "customer_id", "left")

# Support call features.
support_features = _window_aggs(events, as_of_date, "support_call")

# Complaint features: count + high-severity count.
complaint_events = events.filter(F.col("event_type") == "complaint")
complaint_features = customers
for days in (7, 30, 90):
    start = as_of_date - timedelta(days=days)
    w = complaint_events.filter(F.col("event_ts") >= F.lit(str(start) + "T00:00:00"))
    agg = w.groupBy("customer_id").agg(
        F.count("*").alias(f"complaint_count_{days}d"),
        F.sum(F.when(F.col("complaint_severity") == "high", 1).otherwise(0)).alias(
            f"complaint_high_{days}d"
        ),
    )
    complaint_features = complaint_features.join(agg, "customer_id", "left")

# Usage features: latest usage and delta vs. prior period.
usage_events = events.filter(F.col("event_type") == "usage")
latest_usage = (
    usage_events.withColumn(
        "_rn",
        F.row_number().over(
            Window.partitionBy("customer_id").orderBy(F.col("event_ts").desc())
        ),
    )
    .filter(F.col("_rn") == 1)
    .select(
        "customer_id",
        F.col("usage_gb").alias("latest_usage_gb"),
    )
)
# Average usage in prior 30 days vs. 30 days before that.
usage_recent = (
    usage_events.filter(
        F.col("event_ts") >= F.lit(str(as_of_date - timedelta(days=30)) + "T00:00:00")
    )
    .groupBy("customer_id")
    .agg(F.avg("usage_gb").alias("avg_usage_30d"))
)
usage_prior = (
    usage_events.filter(
        (F.col("event_ts") >= F.lit(str(as_of_date - timedelta(days=60)) + "T00:00:00"))
        & (
            F.col("event_ts")
            < F.lit(str(as_of_date - timedelta(days=30)) + "T00:00:00")
        )
    )
    .groupBy("customer_id")
    .agg(F.avg("usage_gb").alias("avg_usage_prior_30d"))
)
usage_features = (
    customers.join(latest_usage, "customer_id", "left")
    .join(usage_recent, "customer_id", "left")
    .join(usage_prior, "customer_id", "left")
    .withColumn(
        "usage_delta_pct",
        F.when(
            F.col("avg_usage_prior_30d").isNotNull()
            & (F.col("avg_usage_prior_30d") > 0),
            (F.col("avg_usage_30d") - F.col("avg_usage_prior_30d"))
            / F.col("avg_usage_prior_30d")
            * 100,
        ),
    )
)

# Days since last activity.
last_activity = (
    events.groupBy("customer_id")
    .agg(F.max("event_ts").alias("_last_event_ts"))
    .withColumn(
        "days_since_last_activity",
        F.datediff(F.lit(str(as_of_date)), F.col("_last_event_ts")),
    )
    .select("customer_id", "days_since_last_activity")
)

# Plan change count.
plan_change_features = _window_aggs(events, as_of_date, "plan_change")

# Billing features.
billing_features = _window_aggs(events, as_of_date, "billing")

# Cancellation count (a strong direct signal).
cancellation_features = _window_aggs(events, as_of_date, "cancellation")


# ── Join all features ────────────────────────────────────────────────

snapshot = (
    customers.join(payment_features, "customer_id", "left")
    .join(support_features, "customer_id", "left")
    .join(complaint_features, "customer_id", "left")
    .join(usage_features, "customer_id", "left")
    .join(last_activity, "customer_id", "left")
    .join(plan_change_features, "customer_id", "left")
    .join(billing_features, "customer_id", "left")
    .join(cancellation_features, "customer_id", "left")
)

# Enrich with static Silver attributes (tenure, contract type, etc.).
silver_table = table(args.catalog, args.schema, "telco_silver")
silver_cols = [
    "customer_id",
    "tenure",
    "monthly_charges",
    "total_charges",
    "contract",
    "internet_service",
    "payment_method",
    "tech_support",
    "senior_citizen",
]
# Use latest snapshot_date from Silver (the most current static state).
silver = spark.table(silver_table).select(*silver_cols)
# If Silver has snapshot_date, take the latest per customer.
if "snapshot_date" in spark.table(silver_table).columns:
    silver = (
        spark.table(silver_table)
        .withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("customer_id").orderBy(F.col("snapshot_date").desc())
            ),
        )
        .filter(F.col("_rn") == 1)
        .select(*silver_cols)
    )

snapshot = snapshot.join(silver, "customer_id", "left")

# Fill nulls for count/sum features (no events = 0, not null).
count_cols = [
    c
    for c in snapshot.columns
    if c.endswith(
        (
            "_count_7d",
            "_count_30d",
            "_count_90d",
            "_failures_7d",
            "_failures_30d",
            "_failures_90d",
            "_high_7d",
            "_high_30d",
            "_high_90d",
        )
    )
]
for c in count_cols:
    snapshot = snapshot.withColumn(c, F.coalesce(F.col(c), F.lit(0)))

# Add payment failure rate features.
for days in (7, 30, 90):
    snapshot = snapshot.withColumn(
        f"payment_failure_rate_{days}d",
        F.when(
            F.col(f"payment_count_{days}d") > 0,
            F.col(f"payment_failures_{days}d") / F.col(f"payment_count_{days}d"),
        ).otherwise(F.lit(None)),
    )

# Add snapshot metadata.
snapshot = snapshot.withColumn(
    "snapshot_date", F.lit(as_of_date.isoformat()).cast("date")
).withColumn(
    "_built_at",
    F.lit(datetime.now(timezone.utc).isoformat()).cast("timestamp"),
)

# ── Write to Gold ────────────────────────────────────────────────────
gold_table = table(args.catalog, args.schema, "gold_feature_snapshot")

if spark.catalog.tableExists(gold_table):
    # Overwrite only this snapshot_date partition (idempotent).
    (
        snapshot.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"snapshot_date = '{as_of_date.isoformat()}'")
        .option("mergeSchema", "true")
        .saveAsTable(gold_table)
    )
else:
    # First run — replaceWhere requires an existing table, so create it directly.
    snapshot.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(gold_table)

row_count = snapshot.count()
feature_count = len(
    [
        c
        for c in snapshot.columns
        if c not in {"customer_id", "snapshot_date", "_built_at"}
    ]
)

_summary = {
    "status": "completed",
    "gold_table": gold_table,
    "as_of_date": as_of_date.isoformat(),
    "customers": row_count,
    "features": feature_count,
}
print(json.dumps(_summary, sort_keys=True))

log_run(
    spark=spark,
    catalog=args.catalog,
    schema=args.schema,
    task_name="build_features",
    run_id=f"build-features-{as_of_date.isoformat()}",
    status="succeeded",
    started_at=_run_started,
    finished_at=datetime.now(timezone.utc),
    output_summary=_summary,
)
