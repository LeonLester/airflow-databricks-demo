FROM apache/airflow:2.8.1

USER airflow

COPY pyproject.toml .
RUN pip install --no-cache-dir ".[dev]"
