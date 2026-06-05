FROM apache/airflow:2.8.1

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-databricks==6.3.0 \
    pytest==8.1.1 \
    pytest-cov==5.0.0
