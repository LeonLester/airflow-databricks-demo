from datetime import datetime
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from config import DATABRICKS_CONN_ID, JOB_IDS

with DAG(
    dag_id="etl_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    bronze_ingest = DatabricksRunNowOperator(
        task_id="bronze_ingest",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=JOB_IDS["bronze_ingest"],
    )