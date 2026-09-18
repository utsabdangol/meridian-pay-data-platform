import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

# Initialize Spark session
spark = SparkSession.builder \
    .appName("DailyTransactionPipeline") \
    .getOrCreate()

# Ensure namespace and Iceberg target table exist
spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.payments")
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.payments.transaction_events (
        event_id STRING,
        transaction_id STRING,
        event_type STRING,
        event_timestamp TIMESTAMP,
        ingested_at TIMESTAMP,
        merchant_id STRING,
        customer_id STRING,
        amount DOUBLE,
        currency STRING,
        payment_method STRING,
        status STRING
    ) USING iceberg
""")

# Date range matching generate_data.py (30 days starting from 2026-01-01)
BASE_DATE = datetime(2026, 1, 1)
NUM_DAYS = 30

for day_index in range(NUM_DAYS):
    day_date = (BASE_DATE + timedelta(days=day_index)).date().isoformat()
    input_path = f"/home/iceberg/data/events/dt={day_date}/"

    try:
        df = spark.read.json(input_path)
        df = df.withColumn("event_timestamp", to_timestamp(col("event_timestamp"))) \
               .withColumn("ingested_at", to_timestamp(col("ingested_at")))

        df.select(
            "event_id", "transaction_id", "event_type", "event_timestamp",
            "ingested_at", "merchant_id", "customer_id", "amount",
            "currency", "payment_method", "status"
        ).writeTo("demo.payments.transaction_events").append()
        print(f"Successfully appended events for dt={day_date} to demo.payments.transaction_events")
    except Exception as e:
        print(f"Error processing dt={day_date}: {e}")
