"""
Transaction Data Generator for Meridian Pay Data Platform.

Generates 30 days of simulated transaction data in JSONL format with:
- ~500 to 1000 transactions per day
- ~10% corrections/refunds referencing transactions from 1-3 days prior
- ~5% processing-lag events (event timestamp earlier than ingestion timestamp)
- Schema evolution on Day 20+: introduction of BANK_TRANSFER_INSTANT with
  `clearing_house_ref` and `instant_settlement_flag` fields.
"""

import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(OUTPUT_DIR, exist_ok=True)

START_DATE = datetime(2026, 8, 1, 0, 0, 0, tzinfo=timezone.utc)
DAYS = 30

BASE_RAILS = ["CREDIT_CARD", "DEBIT_CARD", "ACH", "PAYPAL"]
MERCHANTS = [f"mch_{i:03d}" for i in range(1, 26)]
CURRENCIES = ["USD", "USD", "USD", "EUR", "GBP"]
EVENT_TYPES = ["PAYMENT", "REFUND", "CORRECTION", "CHARGEBACK"]

def generate_dataset():
    random.seed(42)
    history = {}  # day_num -> list of transaction_ids

    total_records = 0
    print(f"Generating 30 days of dataset into {OUTPUT_DIR}...")

    for day in range(1, DAYS + 1):
        day_date = START_DATE + timedelta(days=day - 1)
        daily_tx_count = random.randint(500, 1000)
        daily_records = []
        day_tx_ids = []

        for _ in range(daily_tx_count):
            tx_id = f"tx_{uuid.uuid4().hex[:12]}"
            day_tx_ids.append(tx_id)

            # Determine event time & ingestion time (handling ~5% lag)
            is_lagged = random.random() < 0.05
            if is_lagged and day > 2:
                lag_days = random.randint(1, 2)
                event_time = day_date - timedelta(days=lag_days, seconds=random.randint(0, 86400))
            else:
                event_time = day_date + timedelta(seconds=random.randint(0, 86400))

            ingestion_time = day_date + timedelta(seconds=random.randint(0, 86400))
            if ingestion_time < event_time:
                ingestion_time = event_time + timedelta(seconds=random.randint(5, 300))

            # Determine event type & reference transaction (~10% corrections)
            is_correction = random.random() < 0.10
            ref_tx_id = None
            if is_correction and day > 1:
                # Pick a transaction from 1 to 3 days ago
                lookback_days = random.randint(1, min(3, day - 1))
                target_day = day - lookback_days
                if history.get(target_day):
                    ref_tx_id = random.choice(history[target_day])
                    event_type = random.choice(["REFUND", "CORRECTION", "CHARGEBACK"])
                else:
                    event_type = "PAYMENT"
            else:
                event_type = "PAYMENT"

            # Payment rail selection (Day 20+ includes BANK_TRANSFER_INSTANT)
            if day >= 20 and random.random() < 0.15:
                payment_rail = "BANK_TRANSFER_INSTANT"
            else:
                payment_rail = random.choice(BASE_RAILS)

            # Base Record
            record = {
                "transaction_id": tx_id,
                "event_timestamp": event_time.isoformat(),
                "ingestion_timestamp": ingestion_time.isoformat(),
                "merchant_id": random.choice(MERCHANTS),
                "customer_id": f"cust_{random.randint(100, 999)}",
                "amount": round(random.uniform(5.0, 1250.0), 2),
                "currency": random.choice(CURRENCIES),
                "payment_rail": payment_rail,
                "event_type": event_type,
                "reference_transaction_id": ref_tx_id,
                "status": "SUCCESS" if random.random() > 0.03 else "FAILED"
            }

            # Schema Evolution fields for BANK_TRANSFER_INSTANT on Day 20+
            if payment_rail == "BANK_TRANSFER_INSTANT":
                record["clearing_house_ref"] = f"CH-{random.randint(1000000, 9999999)}"
                record["instant_settlement_flag"] = True

            daily_records.append(record)

        history[day] = day_tx_ids

        # Write to JSONL
        file_path = os.path.join(OUTPUT_DIR, f"transactions_day_{day:02d}.jsonl")
        with open(file_path, "w", encoding="utf-8") as f:
            for rec in daily_records:
                f.write(json.dumps(rec) + "\n")

        total_records += len(daily_records)
        print(f"  Day {day:02d}: Generated {len(daily_records)} records -> {os.path.basename(file_path)}")

    print(f"\nDone! Generated {total_records} total transactions across 30 days.")

if __name__ == "__main__":
    generate_dataset()
