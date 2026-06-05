from pyspark.sql import functions as F

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# Read from silver — already clean and deduplicated
df = spark.read.table("silver.events_cleaned")

# Aggregate: how many events per user per type?
gold_df = (
    df.groupBy("user_id", "event_type")
    .agg(
        F.count("event_id").alias("event_count"),
        F.min("event_timestamp").alias("first_seen"),
        F.max("event_timestamp").alias("last_seen"),
    )
    .orderBy("user_id", "event_type")
)

gold_df.show()
print(f"Row count: {gold_df.count()}")

gold_df.write.format("delta").mode("overwrite").saveAsTable("gold.user_event_summary")
print("Successfully written to gold.user_event_summary")