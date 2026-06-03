from datetime import datetime
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from config import DATABRICKS_CONN_ID, get_job_id

with DAG(
    dag_id="etl_pipeline",
    start_date=datetime(2026, 6, 3),
    schedule=None,
    catchup=False,
) as dag:

    bronze_ingest = DatabricksRunNowOperator(
        task_id="bronze_ingest",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=get_job_id("bronze_ingest"),
    )

    silver_transform = DatabricksRunNowOperator(
        task_id="silver_transform",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=get_job_id("silver_transform"),
    )

    bronze_ingest >> silver_transform