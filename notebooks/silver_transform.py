from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

# Read from bronze
df = spark.read.table("bronze.raw_events")

# 1. Parse timestamp string into proper timestamp type
df = df.withColumn("event_timestamp", F.to_timestamp("ts")).drop("ts")

# 2. Drop rows missing required fields
df = df.dropna(subset=["event_id", "user_id"])

# 3. Deduplicate — keep the first occurrence of each event_id
window = Window.partitionBy("event_id").orderBy("event_timestamp")
df = (
    df.withColumn("_rank", F.row_number().over(window))
    .filter(F.col("_rank") == 1)
    .drop("_rank")
)

df.show()
print(f"Row count: {df.count()}")

df.write.format("delta").mode("overwrite").saveAsTable("silver.events_cleaned")
print("Successfully written to silver.events_cleaned")