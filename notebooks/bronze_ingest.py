from pyspark.sql import functions as F

spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

# Simulate raw events arriving from an upstream system.
# In production this would read from an S3 bucket, Kafka, an API, etc.
raw_data = [
    {"event_id": "e001", "user_id": "u1", "event_type": "page_view", "ts": "2024-03-15T10:00:00"},
    {"event_id": "e002", "user_id": "u2", "event_type": "click",     "ts": "2024-03-15T10:01:00"},
    {"event_id": "e003", "user_id": "u1", "event_type": "purchase",  "ts": "2024-03-15T10:02:00"},
    {"event_id": "e004", "user_id": "u3", "event_type": "page_view", "ts": "2024-03-15T10:03:00"},
    {"event_id": "e002", "user_id": "u2", "event_type": "click",     "ts": "2024-03-15T10:01:00"},  # duplicate
]

# Create a Spark DataFrame — this is the distributed data structure Spark works with
df = spark.createDataFrame(raw_data)

# Add ingestion metadata — important for debugging and auditing
df = df.withColumn("ingested_at", F.current_timestamp())

# Show what we've got
df.show()
print(f"Row count: {df.count()}")

# Write to Bronze Delta table — raw, no cleaning, exactly as received
df.write.format("delta").mode("overwrite").saveAsTable("bronze.raw_events")

print("Successfully written to bronze.raw_events")