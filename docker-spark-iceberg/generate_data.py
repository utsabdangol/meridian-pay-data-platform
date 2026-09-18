"""
Generates synthetic daily transaction event files for the payments lakehouse
project. Simulates three kinds of traffic:
  - normal same-day events (~85%)
  - corrections referencing a transaction from 1-3 days earlier (~10%)
  - processing-lag events, reported a day or two after they happened (~5%)

From SCHEMA_EVOLUTION_DAY onward, a new payment rail (BANK_TRANSFER_INSTANT)
appears with two extra fields — this is the schema evolution moment for
Iceberg's ALTER TABLE ADD COLUMN to handle.

Output: one JSONL file per day, partitioned by ingestion date, written to
./data/events/dt=YYYY-MM-DD/events_YYYY-MM-DD.jsonl
"""

import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone

from faker import Faker

fake = Faker()
random.seed(42)
Faker.seed(42)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "data", "events")
NUM_DAYS = 30
BASE_DATE = datetime(2026, 1, 1, tzinfo=timezone.utc)
DAILY_TX_MIN, DAILY_TX_MAX = 500, 1000
CORRECTION_RATE = 0.10
LAG_RATE = 0.05
SCHEMA_EVOLUTION_DAY = 20  # day index (0-based) when the new payment rail launches

MERCHANTS = [f"m-{i:04d}" for i in range(1, 201)]
CUSTOMERS = [f"c-{i:05d}" for i in range(1, 5001)]

PAYMENT_METHODS = ["WALLET_BALANCE", "CARD", "BANK_TRANSFER"]
NEW_PAYMENT_METHOD = "BANK_TRANSFER_INSTANT"

STATUS_FOR_EVENT = {
    "INITIATED": "INITIATED",
    "COMPLETED": "COMPLETED",
    "FAILED": "FAILED",
    "REFUNDED": "REFUNDED",
    "CHARGEBACK": "CHARGEBACK",
}


def new_transaction_id():
    return f"t-{uuid.uuid4().hex[:12]}"


def new_event_id():
    return f"e-{uuid.uuid4().hex[:12]}"


def make_event(transaction_id, event_type, event_timestamp, ingested_at, payment_method, day_index):
    event = {
        "event_id": new_event_id(),
        "transaction_id": transaction_id,
        "event_type": event_type,
        "event_timestamp": event_timestamp.isoformat(),
        "ingested_at": ingested_at.isoformat(),
        "merchant_id": random.choice(MERCHANTS),
        "customer_id": random.choice(CUSTOMERS),
        "amount": round(random.uniform(50, 15000), 2),
        "currency": "NPR",
        "payment_method": payment_method,
        "status": STATUS_FOR_EVENT[event_type],
    }
    # Schema evolution: these fields only exist once the new rail launches,
    # and only on events that actually use it.
    if day_index >= SCHEMA_EVOLUTION_DAY and payment_method == NEW_PAYMENT_METHOD:
        event["bank_reference_number"] = fake.bban()
        event["routing_details"] = fake.swift8()
    return event


def write_day(day_index, events):
    day_date = (BASE_DATE + timedelta(days=day_index)).date().isoformat()
    day_dir = os.path.join(OUTPUT_DIR, f"dt={day_date}")
    os.makedirs(day_dir, exist_ok=True)
    path = os.path.join(day_dir, f"events_{day_date}.jsonl")
    with open(path, "w", encoding="utf-8") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")
    print(f"Day {day_index} ({day_date}): wrote {len(events)} events -> {path}")


def main():
    # Pool of recently-completed transactions eligible for a late correction.
    # {transaction_id: day_index_completed}
    recent_transactions = {}

    for day_index in range(NUM_DAYS):
        day_date = BASE_DATE + timedelta(days=day_index)
        events = []
        daily_count = random.randint(DAILY_TX_MIN, DAILY_TX_MAX)

        available_methods = PAYMENT_METHODS + (
            [NEW_PAYMENT_METHOD] if day_index >= SCHEMA_EVOLUTION_DAY else []
        )

        for _ in range(daily_count):
            roll = random.random()

            # --- Correction event: references an existing transaction ---
            if roll < CORRECTION_RATE and recent_transactions:
                candidates = [
                    tx for tx, created_day in recent_transactions.items()
                    if 1 <= day_index - created_day <= 3
                ]
                if candidates:
                    transaction_id = random.choice(candidates)
                    event_type = random.choice(["REFUNDED", "CHARGEBACK", "FAILED"])
                    event_ts = day_date + timedelta(
                        hours=random.randint(0, 23), minutes=random.randint(0, 59)
                    )
                    events.append(make_event(
                        transaction_id, event_type, event_ts, event_ts,
                        random.choice(available_methods), day_index,
                    ))
                    continue  # correction doesn't create a new transaction_id

            # --- New transaction: normal traffic or processing lag ---
            transaction_id = new_transaction_id()
            payment_method = random.choice(available_methods)

            if roll < CORRECTION_RATE + LAG_RATE:
                # Processing lag: happened 1-2 days before it was reported
                event_ts = day_date - timedelta(days=random.randint(1, 2))
                ingested_ts = day_date + timedelta(
                    hours=random.randint(0, 23), minutes=random.randint(0, 59)
                )
            else:
                # Normal same-day traffic
                event_ts = day_date + timedelta(
                    hours=random.randint(0, 23), minutes=random.randint(0, 59)
                )
                ingested_ts = event_ts

            event_type = random.choices(
                ["INITIATED", "COMPLETED", "FAILED"], weights=[0.1, 0.85, 0.05]
            )[0]

            events.append(make_event(
                transaction_id, event_type, event_ts, ingested_ts, payment_method, day_index
            ))

            if event_type == "COMPLETED":
                recent_transactions[transaction_id] = day_index

        write_day(day_index, events)

        # Keep the correction pool bounded to the last 5 days
        recent_transactions = {
            tx: d for tx, d in recent_transactions.items() if day_index - d <= 5
        }


if __name__ == "__main__":
    main()