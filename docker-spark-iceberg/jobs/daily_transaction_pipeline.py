import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, to_timestamp, lit, row_number

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

# fact_transactions gets the two new fields from the start (unlike the
# bronze table) - the schema evolution *story* lives in transaction_events,
# the raw immutable log; fact_transactions is a derived/curated table where
# having the columns from day one is a reasonable simplification.
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.payments.fact_transactions (
        transaction_id STRING,
        merchant_id STRING,
        customer_id STRING,
        amount DOUBLE,
        currency STRING,
        payment_method STRING,
        status STRING,
        last_event_type STRING,
        last_updated_at TIMESTAMP,
        bank_reference_number STRING,
        routing_details STRING
    ) USING iceberg
""")

BASE_DATE = datetime(2026, 1, 1)
NUM_DAYS = 30
SCHEMA_EVOLUTION_DAY = 20  # matches generate_events.py

schema_evolved = False

for day_index in range(NUM_DAYS):
    day_date = (BASE_DATE + timedelta(days=day_index)).date().isoformat()
    input_path = f"/home/iceberg/data/events/dt={day_date}/"

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

        # fact_transactions always has these columns, so make sure every
        # day's df has them too, even before the new rail exists
        if "bank_reference_number" not in df.columns:
            df = df.withColumn("bank_reference_number", lit(None).cast("string"))
        if "routing_details" not in df.columns:
            df = df.withColumn("routing_details", lit(None).cast("string"))

        # ---------- bronze: transaction_events (append-only, idempotent) ----------
        bronze_cols = [
            "event_id", "transaction_id", "event_type", "event_timestamp",
            "ingested_at", "merchant_id", "customer_id", "amount",
            "currency", "payment_method", "status",
        ]
        if day_index >= SCHEMA_EVOLUTION_DAY:
            bronze_cols += ["bank_reference_number", "routing_details"]

        bronze_df = df.select(*bronze_cols)
        bronze_df.createOrReplaceTempView("day_batch")

        spark.sql("""
            MERGE INTO demo.payments.transaction_events t
            USING day_batch s
            ON t.event_id = s.event_id
            WHEN NOT MATCHED THEN INSERT *
        """)

        # ---------- silver: fact_transactions (current state, one row per tx) ----------
        # Keep only the latest event per transaction_id within this day's
        # batch before merging - MERGE INTO errors on duplicate source keys.
        window_spec = Window.partitionBy("transaction_id").orderBy(col("event_timestamp").desc())

        fact_df = (
            df.withColumn("rn", row_number().over(window_spec))
              .filter(col("rn") == 1)
              .selectExpr(
                  "transaction_id", "merchant_id", "customer_id", "amount",
                  "currency", "payment_method", "status",
                  "event_type as last_event_type",
                  "event_timestamp as last_updated_at",
                  "bank_reference_number", "routing_details",
              )
        )
        fact_df.createOrReplaceTempView("day_fact_batch")

        spark.sql("""
            MERGE INTO demo.payments.fact_transactions t
            USING day_fact_batch s
            ON t.transaction_id = s.transaction_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

        print(f"Successfully processed dt={day_date}")
    except Exception as e:
        print(f"Error processing dt={day_date}: {e}")