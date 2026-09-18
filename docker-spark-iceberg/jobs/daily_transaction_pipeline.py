import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lit

spark = SparkSession.builder \
    .appName("DailyTransactionPipeline") \
    .getOrCreate()

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

BASE_DATE = datetime(2026, 1, 1)
NUM_DAYS = 30
SCHEMA_EVOLUTION_DAY = 20  # matches generate_events.py

schema_evolved = False  # tracks whether we've already run the ALTER TABLE

for day_index in range(NUM_DAYS):
    day_date = (BASE_DATE + timedelta(days=day_index)).date().isoformat()
    input_path = f"/home/iceberg/data/events/dt={day_date}/"

    # The moment: run this exactly once, right when the new payment rail launches
    if day_index >= SCHEMA_EVOLUTION_DAY and not schema_evolved:
        spark.sql("""
            ALTER TABLE demo.payments.transaction_events
            ADD COLUMNS (bank_reference_number STRING, routing_details STRING)
        """)
        schema_evolved = True
        print(f"Schema evolved at dt={day_date}: added bank_reference_number, routing_details")

    try:
        df = spark.read.json(input_path)
        df = df.withColumn("event_timestamp", to_timestamp(col("event_timestamp"))) \
               .withColumn("ingested_at", to_timestamp(col("ingested_at")))

        base_cols = [
            "event_id", "transaction_id", "event_type", "event_timestamp",
            "ingested_at", "merchant_id", "customer_id", "amount",
            "currency", "payment_method", "status",
        ]

        if day_index >= SCHEMA_EVOLUTION_DAY:
            # These columns only exist in the JSON from day 20 onward.
            # If a given day happens to have zero BANK_TRANSFER_INSTANT
            # events, the columns won't be in df at all, so add them as
            # nulls rather than assuming they're present.
            if "bank_reference_number" not in df.columns:
                df = df.withColumn("bank_reference_number", lit(None).cast("string"))
            if "routing_details" not in df.columns:
                df = df.withColumn("routing_details", lit(None).cast("string"))
            select_cols = base_cols + ["bank_reference_number", "routing_details"]
        else:
            select_cols = base_cols

        df = df.select(*select_cols)
        df.createOrReplaceTempView("day_batch")

        spark.sql("""
            MERGE INTO demo.payments.transaction_events t
            USING day_batch s
            ON t.event_id = s.event_id
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"Successfully merged events for dt={day_date} into demo.payments.transaction_events")
    except Exception as e:
        print(f"Error processing dt={day_date}: {e}")