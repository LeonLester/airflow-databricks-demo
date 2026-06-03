from airflow.models import Variable

DATABRICKS_CONN_ID = "databricks_default"

_JOB_IDS = {
    "dev": {
        "bronze_ingest": 421658606944721,
        "silver_transform": 921960675949165,
    },
    "prod": {
        "bronze_ingest": None,
        "silver_transform": None,
    },
}

def get_job_id(job_name: str) -> int:
    env = Variable.get("env", default_var="dev")
    job_id = _JOB_IDS[env][job_name]
    if job_id is None:
        raise ValueError(f"Job ID for '{job_name}' in env '{env}' is not configured.")
    return job_id