from pathlib import Path

import yaml

WORKFLOW_PATH = Path(__file__).parents[1] / "resources" / "churn_workflow.yml"
README_PATH = Path(__file__).parents[1] / "README.md"

EXPECTED_JOBS = {
    "churn_event_generator",
    "churn_seed_bronze",
    "churn_data_pipeline",
    "churn_backfill_features",
    "churn_model_pipeline",
    "churn_model_rollback",
    "churn_batch_score",
    "churn_drift_monitor",
    "churn_end_to_end",
}


def load_jobs():
    workflow = yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8"))
    return workflow["resources"]["jobs"]


def test_bundle_exposes_three_stages_and_one_orchestrator():
    jobs = load_jobs()

    assert set(jobs) == EXPECTED_JOBS


def test_job_tasks_preserve_required_execution_order():
    jobs = load_jobs()
    data_tasks = jobs["churn_data_pipeline"]["tasks"]
    model_tasks = jobs["churn_model_pipeline"]["tasks"]
    scoring_tasks = jobs["churn_batch_score"]["tasks"]

    assert [task["task_key"] for task in data_tasks] == [
        "ingest_events",
        "transform_silver_and_gold",
        "transform_events",
        "build_features",
        "generate_labels",
    ]
    assert "depends_on" not in data_tasks[0]  # ingest_events is the first task
    assert data_tasks[1]["depends_on"] == [{"task_key": "ingest_events"}]
    assert data_tasks[2]["depends_on"] == [{"task_key": "ingest_events"}]
    assert data_tasks[3]["depends_on"] == [
        {"task_key": "transform_silver_and_gold"},
        {"task_key": "transform_events"},
    ]
    assert data_tasks[4]["depends_on"] == [{"task_key": "build_features"}]

    assert [task["task_key"] for task in model_tasks] == [
        "train_and_register_model",
        "evaluate_and_promote_candidate",
    ]
    assert model_tasks[1]["depends_on"] == [{"task_key": "train_and_register_model"}]

    assert [task["task_key"] for task in scoring_tasks] == ["batch_score_customers"]


def test_readme_run_commands_match_bundle_job_keys():
    readme = README_PATH.read_text(encoding="utf-8")

    for job_key in EXPECTED_JOBS:
        assert f"databricks bundle run {job_key}" in readme


def test_end_to_end_job_orchestrates_stage_jobs_in_order():
    jobs = load_jobs()
    orchestrator = jobs["churn_end_to_end"]
    tasks = orchestrator["tasks"]

    assert [task["task_key"] for task in tasks] == [
        "run_data_pipeline",
        "run_batch_score",
    ]
    assert tasks[0]["run_job_task"]["job_id"] == (
        "${resources.jobs.churn_data_pipeline.id}"
    )
    assert tasks[1]["depends_on"] == [{"task_key": "run_data_pipeline"}]
    assert tasks[1]["run_job_task"]["job_id"] == (
        "${resources.jobs.churn_batch_score.id}"
    )


def test_scheduled_jobs():
    jobs = load_jobs()
    scheduled_jobs = {job_key for job_key, job in jobs.items() if "schedule" in job}

    assert scheduled_jobs == {
        "churn_event_generator",
        "churn_model_pipeline",
        "churn_end_to_end",
    }
    assert jobs["churn_event_generator"]["schedule"]["pause_status"] == "UNPAUSED"
    assert jobs["churn_model_pipeline"]["schedule"]["pause_status"] == "UNPAUSED"
    assert jobs["churn_end_to_end"]["schedule"]["pause_status"] == "UNPAUSED"


def test_rollback_job_is_manual_and_accepts_a_target_version():
    rollback = load_jobs()["churn_model_rollback"]

    assert "schedule" not in rollback
    assert rollback["max_concurrent_runs"] == 1
    assert rollback["parameters"] == [{"name": "target_version", "default": ""}]

    tasks = rollback["tasks"]
    assert [task["task_key"] for task in tasks] == ["rollback_champion"]
    rollback_task = tasks[0]
    assert rollback_task["spark_python_task"]["python_file"] == (
        "../src/churn_pipeline/modeling/rollback.py"
    )
    assert rollback_task["spark_python_task"]["parameters"][-2:] == [
        "--target-version",
        "{{job.parameters.target_version}}",
    ]


def test_event_generator_is_scheduled_parameterized_and_volume_backed():
    generator = load_jobs()["churn_event_generator"]

    assert generator["schedule"]["quartz_cron_expression"] == "0 0 5 * * ?"
    assert generator["max_concurrent_runs"] == 1
    assert generator["parameters"] == [
        {"name": "event_date", "default": ""},
        {"name": "seed", "default": "20260801"},
        {"name": "drift_level", "default": "0.0"},
    ]
    task = generator["tasks"][0]
    assert task["task_key"] == "generate_customer_events"
    assert task["spark_python_task"]["python_file"] == (
        "../src/churn_pipeline/ingestion/generate_events.py"
    )
    parameters = task["spark_python_task"]["parameters"]
    assert "--output-root" in parameters
    assert any(str(value).startswith("/Volumes/") for value in parameters)
