from airflow.models import Variable

DATABRICKS_CONN_ID = "databricks_default"


_JOB_IDS = {
    "dev": {
        "bronze_ingest": 421658606944721,
        "silver_transform": 921960675949165,
    },

}

def get_job_id(job_name: str) -> int:
    env = Variable.get("env", default_var="dev")
    return _JOB_IDS[env][job_name]