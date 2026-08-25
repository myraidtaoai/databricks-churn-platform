"""Generate delayed churn labels for point-in-time feature snapshots.

A label for ``as_of_date`` is only written once ``as_of_date +
LABEL_HORIZON_DAYS`` has passed.  This eliminates the most common form
of target leakage in churn models: using an outcome that had not yet
occurred at the time features were computed.

A customer is labeled ``churned = 1`` if a cancellation event exists in
``telco_events_silver`` with ``event_ts`` in the half-open window
``(as_of_date, as_of_date + LABEL_HORIZON_DAYS]``.  Otherwise ``churned = 0``.

The ``training_dataset`` view is created (or replaced) to join
``gold_feature_snapshot`` to ``gold_labels`` on ``(customer_id,
snapshot_date)`` and expose only matured labels.

Idempotent: re-running the same ``as_of_date`` overwrites exactly that
partition in ``gold_labels``.
"""

from __future__ import annotations

import _path_helper  # noqa: F401 — adds churn_pipeline/ to sys.path

import argparse
import json
from datetime import date, datetime, timedelta, timezone

from common import get_spark, table
from ops.run_logger import log_run
from pyspark.sql import functions as F

spark = get_spark()

LABEL_HORIZON_DAYS_DEFAULT = 30

parser = argparse.ArgumentParser(
    description="Generate delayed churn labels for a given as_of_date."
)
parser.add_argument("--catalog", required=True)
parser.add_argument("--schema", required=True)
parser.add_argument(
    "--as-of-date",
    default="",
    help="Snapshot date to label (YYYY-MM-DD). Defaults to yesterday UTC.",
)
parser.add_argument(
    "--label-horizon-days",
    type=int,
    default=LABEL_HORIZON_DAYS_DEFAULT,
    help=f"Days after as_of_date to observe outcome. Default {LABEL_HORIZON_DAYS_DEFAULT}.",
)
args = parser.parse_args()

_run_started = datetime.now(timezone.utc)

as_of_date: date = (
    date.fromisoformat(args.as_of_date)
    if args.as_of_date.strip()
    else _run_started.date() - timedelta(days=1)
)
label_horizon = args.label_horizon_days
maturity_date = as_of_date + timedelta(days=label_horizon)
today = _run_started.date()

# ── Guard: refuse to write immature labels ───────────────────────────
if today < maturity_date:
    print(
        json.dumps(
            {
                "status": "skipped",
                "reason": "label not yet mature",
                "as_of_date": as_of_date.isoformat(),
                "maturity_date": maturity_date.isoformat(),
                "days_remaining": (maturity_date - today).days,
            },
            sort_keys=True,
        )
    )
    log_run(
        spark=spark,
        catalog=args.catalog,
        schema=args.schema,
        task_name="generate_labels",
        run_id=f"labels-{as_of_date.isoformat()}",
        status="succeeded",
        started_at=_run_started,
        finished_at=datetime.now(timezone.utc),
        output_summary={"status": "skipped", "reason": "label not yet mature"},
    )
else:
    # ── Determine churn outcome ──────────────────────────────────────
    events_table = table(args.catalog, args.schema, "telco_events_silver")

    # Window: strictly after as_of_date, up to and including maturity_date.
    label_start = str(as_of_date) + "T23:59:59"
    label_end = str(maturity_date) + "T23:59:59"

    # Customers who cancelled within the horizon.
    cancellations = (
        spark.table(events_table)
        .filter(F.col("event_type") == "cancellation")
        .filter(F.col("event_ts") > F.lit(label_start))
        .filter(F.col("event_ts") <= F.lit(label_end))
        .select("customer_id")
        .distinct()
        .withColumn("churned", F.lit(1))
    )

    # All customers who had a feature snapshot for this date.
    gold_table = table(args.catalog, args.schema, "gold_feature_snapshot")
    snapshot_customers = (
        spark.table(gold_table)
        .filter(F.col("snapshot_date") == F.lit(as_of_date.isoformat()).cast("date"))
        .select("customer_id")
        .distinct()
    )

    if snapshot_customers.limit(1).count() == 0:
        print(
            json.dumps(
                {
                    "status": "skipped",
                    "reason": "no feature snapshot for as_of_date",
                    "as_of_date": as_of_date.isoformat(),
                },
                sort_keys=True,
            )
        )
        log_run(
            spark=spark,
            catalog=args.catalog,
            schema=args.schema,
            task_name="generate_labels",
            run_id=f"labels-{as_of_date.isoformat()}",
            status="succeeded",
            started_at=_run_started,
            finished_at=datetime.now(timezone.utc),
            output_summary={
                "status": "skipped",
                "reason": "no feature snapshot",
            },
        )
    else:
        # Left join: customers without a cancellation get churned = 0.
        labels = (
            snapshot_customers.join(cancellations, "customer_id", "left")
            .withColumn("churned", F.coalesce(F.col("churned"), F.lit(0)))
            .withColumn("snapshot_date", F.lit(as_of_date.isoformat()).cast("date"))
            .withColumn("label_horizon_days", F.lit(label_horizon))
            .withColumn("maturity_date", F.lit(maturity_date.isoformat()).cast("date"))
            .withColumn(
                "_labeled_at",
                F.lit(datetime.now(timezone.utc).isoformat()).cast("timestamp"),
            )
        )

        # ── Write labels ─────────────────────────────────────────────
        labels_table = table(args.catalog, args.schema, "gold_labels")

        (
            labels.write.format("delta")
            .mode("overwrite")
            .option(
                "replaceWhere",
                f"snapshot_date = '{as_of_date.isoformat()}'",
            )
            .option("mergeSchema", "true")
            .saveAsTable(labels_table)
        )

        churn_count = labels.filter(F.col("churned") == 1).count()
        total_count = labels.count()

        # ── Create or replace the training_dataset view ──────────────
        training_view = table(args.catalog, args.schema, "training_dataset")
        spark.sql(f"""
            CREATE OR REPLACE VIEW {training_view} AS
            SELECT f.*, l.churned, l.label_horizon_days, l.maturity_date
            FROM {gold_table} f
            INNER JOIN {labels_table} l
                ON f.customer_id = l.customer_id
                AND f.snapshot_date = l.snapshot_date
        """)

        print(
            json.dumps(
                {
                    "status": "completed",
                    "labels_table": labels_table,
                    "training_view": training_view,
                    "as_of_date": as_of_date.isoformat(),
                    "total_customers": total_count,
                    "churned": churn_count,
                    "churn_rate": (
                        round(churn_count / total_count, 4) if total_count else 0
                    ),
                    "label_horizon_days": label_horizon,
                },
                sort_keys=True,
            )
        )
        log_run(
            spark=spark,
            catalog=args.catalog,
            schema=args.schema,
            task_name="generate_labels",
            run_id=f"labels-{as_of_date.isoformat()}",
            status="succeeded",
            started_at=_run_started,
            finished_at=datetime.now(timezone.utc),
            output_summary={
                "status": "completed",
                "as_of_date": as_of_date.isoformat(),
                "total_customers": total_count,
                "churned": churn_count,
            },
        )
