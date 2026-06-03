from datetime import datetime
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from base_pipeline import BasePipeline


class EtlPipeline(BasePipeline):

    def build(self) -> DAG:
        with DAG(
            dag_id="etl_pipeline",
            start_date=datetime(2024, 1, 1),
            schedule=None,
            catchup=False,
            default_args=self.DEFAULT_ARGS,
        ) as dag:

            bronze_ingest = DatabricksRunNowOperator(
                task_id="bronze_ingest",
                databricks_conn_id=self.DATABRICKS_CONN_ID,
                job_id=self.get_job_id("bronze_ingest"),
            )

            silver_transform = DatabricksRunNowOperator(
                task_id="silver_transform",
                databricks_conn_id=self.DATABRICKS_CONN_ID,
                job_id=self.get_job_id("silver_transform"),
            )

            bronze_ingest >> silver_transform

        return dag


#Airflow needs the DAG object at module level to find it
dag = EtlPipeline().build()