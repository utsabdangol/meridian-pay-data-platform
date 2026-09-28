# Meridian Pay Data Platform

An open lakehouse data platform built with **Apache Spark**, **Apache
Iceberg**, and **Snowflake** — framed as a simulated client engagement for a
fictional digital payments company. Built to learn the "Spark writes,
Snowflake reads, no vendor lock-in" pattern hands-on, including the real
infrastructure obstacles that came with it.

See [`CLIENT_BRIEF.md`](CLIENT_BRIEF.md) for the full business framing, and
[`decisions.md`](decisions.md) for a detailed log of every engineering
decision and trade-off made — including the ones that didn't work on the
first attempt.

## What this proves

- **Two engines, one copy of data.** Spark writes every row; Snowflake reads
  the exact same physical Parquet/Iceberg files with zero data duplication.
- **Idempotent ingestion.** The daily pipeline can be safely re-run any
  number of times without creating duplicate rows (`MERGE INTO ... WHEN NOT
  MATCHED`).
- **Schema evolution mid-stream.** A new payment rail's fields are added to
  the raw event log partway through the data's timeline via `ALTER TABLE ADD
  COLUMNS`, without breaking queries over historical data that predates it.
- **Auditability of corrections.** A transaction's state before and after a
  correction (refund/chargeback) can be reconstructed — the exact
  compliance/dispute-resolution scenario the client brief was written
  around.

## Architecture

```
Daily transaction event files (JSON)
        │
        ▼
Spark ingest & merge job
        │
        ├──▶ transaction_events (bronze) — append-only audit log
        │
        └──▶ fact_transactions (silver) — current state via MERGE INTO
                     │
                     ▼
        Snowflake Iceberg Table (reads the same B2-hosted files, zero copy)
                     │
                     ├──▶ Merchant settlement report
                     └──▶ Audit / correction reconstruction
```

**Storage:** Backblaze B2 (S3-compatible) rather than AWS S3 — a substitution
driven by a regional payment-access constraint, not a technical requirement.
See `decisions.md` section 6 for the full story of why, and what was tried
first.

## Repo structure

```
CLIENT_BRIEF.md              Business framing this project responds to
decisions.md                 Full engineering decisions log
generate_events.py           Synthetic transaction event data generator
docker-spark-iceberg/        Spark + Iceberg REST catalog + MinIO (dev) stack
  spark/jobs/
    daily_transaction_pipeline.py   Main ingestion job (bronze + silver)
    reregister.sql                 Recovery script if the REST catalog
                                    loses its table registrations
view_snapshot_log.py          Reads an Iceberg table's snapshot history
                               with human-readable timestamps
```

## Running it

1. **Generate synthetic data:**
   ```bash
   python generate_events.py
   ```
2. **Start the Spark + Iceberg environment:**
   ```bash
   cd docker-spark-iceberg
   docker compose up -d --build
   ```
3. **Run the ingestion pipeline:**
   ```bash
   MSYS_NO_PATHCONV=1 docker exec -it spark-iceberg spark-submit //home/iceberg/jobs/daily_transaction_pipeline.py
   ```
4. **Verify locally:**
   ```bash
   docker exec -it spark-iceberg spark-sql -e "SELECT COUNT(*) FROM demo.payments.fact_transactions"
   ```
5. **Point Snowflake at the data** — see `decisions.md` section 8 for the
   exact `CREATE EXTERNAL VOLUME` / `CREATE CATALOG INTEGRATION` /
   `CREATE ICEBERG TABLE` sequence.

## Results

- **Settlement report** — net payout per merchant, correctly excluding
  transactions later refunded or charged back:
  ```sql
  SELECT merchant_id,
         SUM(CASE WHEN status = 'COMPLETED' THEN amount ELSE 0 END) AS completed_amount,
         SUM(CASE WHEN status IN ('REFUNDED','CHARGEBACK') THEN amount ELSE 0 END) AS corrected_amount,
         SUM(CASE WHEN status = 'COMPLETED' THEN amount ELSE 0 END)
           - SUM(CASE WHEN status IN ('REFUNDED','CHARGEBACK') THEN amount ELSE 0 END) AS net_payout
  FROM fact_transactions
  GROUP BY merchant_id
  ORDER BY net_payout DESC;
  ```
- **Correction audit** — a transaction's state reconstructed both before and
  after a refund, proven by re-pointing Snowflake's Iceberg Table at
  successive metadata snapshots. Full walkthrough and the registration-
  boundary limitation this surfaced: `decisions.md` section 9.