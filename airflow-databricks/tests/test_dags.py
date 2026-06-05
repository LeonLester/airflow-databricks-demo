"""
DAG integrity tests.

Run with: pytest tests/
"""

import os
import sys
import pytest

# Make the dags/ folder importable so DagBag can find config.py and base_pipeline.py
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))

# Tell Airflow to run in unit test mode (skips DB calls)
os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
# Point DagBag at our dags folder
DAG_FOLDER = os.path.join(os.path.dirname(__file__), "..", "dags")


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models import DagBag
    return DagBag(dag_folder=DAG_FOLDER, include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}, (
        f"DAG import errors:\n{dagbag.import_errors}"
    )


def test_etl_pipeline_exists(dagbag):
    assert "etl_pipeline" in dagbag.dags


def test_etl_pipeline_task_count(dagbag):
    dag = dagbag.get_dag("etl_pipeline")
    assert len(dag.tasks) == 3


def test_etl_pipeline_task_ids(dagbag):
    dag = dagbag.get_dag("etl_pipeline")
    task_ids = {t.task_id for t in dag.tasks}
    assert task_ids == {"bronze_ingest", "silver_transform", "gold_aggregate"}


def test_etl_pipeline_dependency_order(dagbag):
    dag = dagbag.get_dag("etl_pipeline")
    # silver must depend on bronze
    silver = dag.get_task("silver_transform")
    assert "bronze_ingest" in {t.task_id for t in silver.upstream_list}
    # gold must depend on silver
    gold = dag.get_task("gold_aggregate")
    assert "silver_transform" in {t.task_id for t in gold.upstream_list}


def test_etl_pipeline_retries(dagbag):
    dag = dagbag.get_dag("etl_pipeline")
    for task in dag.tasks:
        assert task.retries >= 1, f"Task {task.task_id} has no retries configured"


def test_etl_pipeline_failure_callback(dagbag):
    dag = dagbag.get_dag("etl_pipeline")
    for task in dag.tasks:
        assert task.on_failure_callback is not None, (
            f"Task {task.task_id} missing on_failure_callback"
        )


def test_etl_pipeline_no_cycles(dagbag):
    dag = dagbag.get_dag("etl_pipeline")
    # topological_sort raises if there is a cycle
    dag.topological_sort()


def test_etl_pipeline_schedule(dagbag):
    dag = dagbag.get_dag("etl_pipeline")
    assert dag.schedule_interval == "0 6 * * *"
