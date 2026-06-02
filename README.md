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

