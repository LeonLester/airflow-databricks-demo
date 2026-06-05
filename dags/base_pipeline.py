from datetime import timedelta
from airflow import DAG
from config import DATABRICKS_CONN_ID, get_job_id


def on_failure(context):
    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id
    log_url = context["task_instance"].log_url
    print(f"ALERT: {dag_id}.{task_id} failed. Logs: {log_url}")


class BasePipeline:
    """
    Base class for all Airflow/Databricks pipelines.
    Centralises retries, failure callbacks, and connection config.
    Job IDs and environment config live in config.py — not here.
    """

    DATABRICKS_CONN_ID = DATABRICKS_CONN_ID
    get_job_id = staticmethod(get_job_id)

    DEFAULT_ARGS = {
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": on_failure,
    }

    def build(self) -> DAG:
        raise NotImplementedError("Subclasses must implement build()")