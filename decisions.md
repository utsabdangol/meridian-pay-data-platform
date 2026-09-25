# Decisions Log — Meridian Pay Data Platform

This document records the real engineering decisions made building this project,
including the ones that didn't work on the first attempt. See `CLIENT_BRIEF.md`
for the business framing this project responds to.

## 1. Why Spark + Iceberg + Snowflake, not just Snowflake

The brief's core requirement — audit trail, late corrections, schema evolution,
and no single-vendor lock-in — maps directly onto Apache Iceberg's actual
feature set (snapshot history / time travel, `MERGE INTO`, `ALTER TABLE ADD
COLUMN`, open file format). Using Snowflake alone (as in the earlier
[school-inventory-pipeline](../school-inventory-pipeline) project) would have
meant re-learning Snowflake's ELT patterns rather than the open-lakehouse
pattern this project exists to learn.

The architecture deliberately keeps **Spark as the only writer** and
**Snowflake as a read-only consumer of the same physical files**. This is the
one thing that actually proves the "avoid vendor lock-in" requirement — a
Snowflake-managed Iceberg table (Snowflake as the sole writer) would have been
far simpler to build, but would not have demonstrated multi-engine access to
the same data, which was the entire point.

## 2. Bronze/silver table design

Two Iceberg tables instead of one flat table:

- **`transaction_events`** (bronze) — append-only, one row per event ever
  received. This is the audit trail: nothing is ever overwritten, satisfying
  the compliance/dispute-resolution requirement directly.
- **`fact_transactions`** (silver) — one row per transaction, current state
  only, maintained via `MERGE INTO`. This is what a settlement report queries
  against.

This mirrors how real event-sourced financial systems are typically built,
and gives two separate, genuinely different Iceberg mechanisms to build and
explain: an idempotent append, and a proper upsert.

## 3. Idempotent ingestion via `MERGE INTO ... WHEN NOT MATCHED`

The first version of the bronze pipeline used `.writeTo(...).append()`. During
testing, the job was accidentally run twice, and the table's row count came
back roughly double what it should have been — Iceberg (and Parquet) have no
concept of a primary key or uniqueness constraint, so nothing prevented the
duplicate write.

Fixed by switching to:
```sql
MERGE INTO transaction_events t
USING day_batch s
ON t.event_id = s.event_id
WHEN NOT MATCHED THEN INSERT *
```
This makes the job safe to re-run any number of times — verified by
deliberately running it twice and confirming the row count didn't change.

## 4. Window-function dedup before the silver merge

A single transaction can have more than one event on the same day (e.g.
`COMPLETED` then `REFUNDED`). `MERGE INTO` throws a "multiple source rows
matched" error if the source batch has more than one row per merge key, so
each day's batch is deduplicated down to one row per `transaction_id` — the
most recent event, by `event_timestamp` — before the merge:
```python
Window.partitionBy("transaction_id").orderBy(col("event_timestamp").desc())
```

## 5. Schema evolution deferred to day 20, not declared upfront

`transaction_events` is deliberately created *without* the
`bank_reference_number` / `routing_details` columns that a new payment rail
introduces partway through the simulated 30-day period. `ALTER TABLE ADD
COLUMNS` is run once, at the point the new rail "launches" in the data.
Declaring the columns from the start would have stored nulls from day one and
proven nothing — the interesting behavior is that Iceberg handles the
mid-stream addition without breaking any query over the historical data that
predates it.

`fact_transactions`, by contrast, has these columns from creation — a
deliberate simplification, since the schema-evolution story belongs in the
raw event log, not the derived current-state table.

## 6. Storage: the real infrastructure constraint

This is the part of the project that didn't go as planned, and is worth
documenting honestly rather than glossing over.

**MinIO (local, via Docker)** was used for all initial development —
S3-compatible, zero cost, and let the Spark/Iceberg pipeline logic be built
and verified (idempotency, dedup, schema evolution) without any cloud
dependency.

**Getting Snowflake to read that same data required real, internet-reachable
storage** — and this is where the project hit a genuine, non-technical wall:
international card/PayPal access is restricted for Nepali bank accounts,
which blocked signing up for AWS S3 in the conventional way.

Attempts, in order:
1. **Cloudflare Tunnel (Quick Tunnel) to local MinIO** — free, no card
   required, exposes a local service via a temporary public HTTPS URL.
   Technically worked (MinIO became reachable), but Snowflake's
   `CREATE EXTERNAL VOLUME` rejected the `*.trycloudflare.com` domain
   outright (`Endpoint not allowed`) — Snowflake validates S3-compatible
   endpoints against a small list of pre-approved vendor domains for security
   reasons, and shared/dynamic tunnel domains aren't on it.
2. **Backblaze B2** — chosen because it doesn't require a card to create an
   account, unlike AWS, GCP, or Cloudflare R2. Its endpoint wasn't on
   Snowflake's pre-approved list either, but — unlike the tunnel domain —
   `CREATE EXTERNAL VOLUME` accepted it without needing the manual
   Snowflake Support enablement process that was initially assumed necessary.

**Decision: B2 is the storage layer for this POC.** A production deployment
of this system would use AWS S3 or GCS directly; B2 was a pragmatic
substitution driven by a real regional payment-access constraint, not a
technical requirement of the architecture. The External Volume / Iceberg
Table mechanism is identical regardless of which S3-compatible vendor sits
behind it.

## 7. Consequences of moving from local to real cloud storage

Several problems only appeared once the storage layer moved from local MinIO
to real, network-attached storage:

- **The REST catalog's table registry is in-memory, not persistent.**
  (`apache/iceberg-rest-fixture`, used for local development, does not
  survive container restarts.) The underlying Parquet/metadata files in B2
  remained intact across restarts; the catalog's *registration* of those
  tables did not. Tables had to be recreated (`CREATE TABLE IF NOT EXISTS`)
  after each restart during this phase. A production setup would back the
  REST catalog with a persistent store (Postgres-backed catalog, or AWS
  Glue) specifically to avoid this.
- **Iceberg metadata embeds absolute storage paths.** Simply copying the
  already-written MinIO data files to B2 (byte-for-byte) does not produce a
  working table — the metadata/manifest files still reference the original
  `s3://warehouse/...` paths. The correct approach is having Spark write
  directly to the target storage from the start, which is what the final
  pipeline does.
- **Windows line-ending (CRLF) in `entrypoint.sh`** caused the rebuilt Docker
  image's container to crash immediately (`Exited 255`) after editing
  `spark-defaults.conf` in a Windows editor. Fixed by converting the file to
  LF and adding a `.gitattributes` rule (`*.sh text eol=lf`) to prevent
  recurrence.
- **Per-commit network latency compounds significantly at scale.** Each daily
  `MERGE INTO` against B2 involves several sequential round trips (data file,
  manifest, manifest list, new metadata version). Locally against MinIO this
  was instantaneous; against B2 from Nepal, the full 30-day pipeline run took
  several minutes rather than seconds. A production version processing many
  more days, or with tighter latency requirements, would batch commits rather
  than committing once per day.
- **B2's free tier has a daily Class B (download) transaction cap** (2,500
  free transactions, 1GB free download bandwidth per day, per Backblaze's
  published limits). Iterative development and testing exhausted this within
  a single day, temporarily blocking further reads until the daily reset
  (00:00 GMT). Not a data-loss event — purely a rate limit on reads.

## 8. Snowflake-side Iceberg Table configuration

Used the simplest available connection method: a static metadata-file pointer
rather than a full catalog integration syncing with the REST catalog:
```sql
CREATE CATALOG INTEGRATION meridian_object_store_catalog
  CATALOG_SOURCE = OBJECT_STORE
  TABLE_FORMAT = ICEBERG
  ENABLED = TRUE;

CREATE ICEBERG TABLE fact_transactions
  EXTERNAL_VOLUME = 'meridian_b2_volume'
  CATALOG = 'meridian_object_store_catalog'
  METADATA_FILE_PATH = 'payments/fact_transactions/metadata/<latest>.metadata.json';
```
This means Snowflake does **not** automatically pick up new snapshots as the
Spark pipeline continues to run — `METADATA_FILE_PATH` has to be manually
updated (or the table refreshed) to point at each new metadata version. A
production setup would instead use a catalog integration pointed at a
persistent, shared catalog (e.g. AWS Glue) so both engines see updates
automatically, without a manual pointer-update step. This was a deliberate
scope reduction for the POC, not an oversight.

## 9. Known limitations / what a production version would change

- Persistent (not in-memory) REST catalog, or AWS Glue as the shared catalog
- Automatic metadata refresh on the Snowflake side, instead of a static
  metadata-file pointer
- Batched commits instead of one `MERGE INTO` per day, to reduce network
  round trips against remote storage
- Real AWS S3 (or GCS) in place of Backblaze B2, once payment-access
  constraints are resolved
- Data-quality checks (row-count assertions, no-duplicate-key checks) as
  automated tests rather than ad hoc manual verification queries
