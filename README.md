# airflow-databricks-orchestration

Learning project exploring how to use **Apache Airflow** to orchestrate **Databricks** jobs properly.

## The problem we're solving

Imagine you work at a company and every morning at 6am you need to:

Airflow lets us define a pipeline as a graph of tasks with dependencies, schedules it, retries failures, alerts you, and gives you a UI to see what ran and what didn't.

### Where Databricks fits, apparently

Databricks is a cloud platform for running Spark.
So the division of labor in this project is:

Airflow: when to run things, in what order, what to do on failure
Databricks: actually process the data — the ETL logic lives here

Airflow doesn't touch the data itself. It just tells Databricks "run this job now, here are the parameters" and waits for a success or failure signal.

So, we're going to build this DAG:

```yml
[Airflow DAG] 
    → triggers Databricks Job A (ingest raw data)
    → waits for it to finish
    → triggers Databricks Job B (clean the data)
    → waits for it to finish  
    → triggers Databricks Job C (aggregate for reporting)
```

## Role

Senior data engineer on a team doing bronze ingestion into Databricks (Spark jobs).

## Hard Requirements

- **Airflow** for orchestration
- **Databricks** for execution (Spark jobs)

## The Real Problem to Solve

Airflow and Databricks are two separate systems with two separate deployment artifacts:

- Airflow deploys **DAG files** (Python)
- Databricks deploys **notebooks / jobs** (also Python, but deployed differently)

Without structure, juniors will:

- Duplicate configuration between the two systems (cluster sizes, retry counts, job params defined in both places)
- Let the two systems drift (Airflow says run job X with param Y, but job X no longer exists in Databricks, or was renamed)
- Hardcode secrets, job IDs, and environment-specific values in DAG files
- Have no clear answer to "where does this config live?"

## What a Good Solution Looks Like

A monorepo where:

1. **Airflow code** (DAGs, operators, callbacks) and **Databricks code** (notebooks) live side by side in version control
2. There is one source of truth for job IDs, cluster configs, and parameters — not duplicated across files
3. Deployment is automated and environment-aware (dev vs prod configs not hardcoded)
4. A junior can follow the pattern without needing to understand both systems deeply
5. Drift is caught early — ideally by CI, not by a 3am failure

## Pipeline

```
bronze_ingest -> silver_transform -> gold_aggregate
```

Each arrow is a hard dependency. Silver only runs if Bronze succeeds. Gold only runs if Silver succeeds. Failure at any step stops the pipeline, retries twice with a 5-minute delay, then fires the failure callback.

- **Bronze**: raw events written to a Delta table as-is, duplicates included. No transformations. The point of Bronze is to have an exact record of what arrived.
- **Silver**: deduplication, null removal, timestamp parsing. This is where data becomes trustworthy.
- **Gold**: aggregated by user and event type. This is what analysts and dashboards read from.

## Repo structure

```
dags/
  config.py          # job IDs per environment, one place
  base_pipeline.py   # retries, failure callbacks, connection config
  etl_pipeline.py    # pipeline definition, nothing else
```

## Architecture Principle

**Airflow orchestrates. Databricks executes. They do not overlap.**

- Airflow should not know about cluster sizing, Spark configs, or data schemas
- Databricks notebooks should not know about scheduling, retries, or pipeline dependencies
- Parameters flow one way: Airflow → Databricks (via notebook params / job params)

## Open Questions to Answer as We Build

### Where do job IDs live, and how do they stay in sync across environments?

`config.py` is the single source of truth for Databricks job IDs. No DAG file contains a job ID directly. When a job is recreated in Databricks (which changes its ID), you update one file. A junior adding a new job touches only `config.py`.

### How do we deploy notebooks to Databricks without manual copy-paste?

Notebooks live in this repo under `notebooks/` and are connected to Databricks via Databricks Repos. Databricks pulls from the repo directly. There is no separate deploy step for notebooks and no manual copy-paste. When a notebook changes and is merged to main, the Databricks workspace reflects it on the next pull.

### How do we prevent the same retry logic from being configured in both Airflow and Databricks?

Retries are configured in `base_pipeline.py` via `DEFAULT_ARGS` and applied to every task through inheritance. They are disabled in the Databricks job config. Configuring retries in both places causes double-retrying on failure, which is no bueno. Airflow owns retries.

### How do we handle dev/staging/prod without duplicating DAG files?

The DAG structure does not change. The Databricks connection and the `env` variable are configured in the managed service instead of the local UI.
Promoting to prod is three steps: create the Databricks jobs in the prod workspace, fill in the prod job IDs in `config.py`, change the `env` variable to `prod`.
