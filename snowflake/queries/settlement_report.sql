-- ---------------------------------------------------------------------
-- STEP 1: Merchant Net Payout Aggregation
-- ---------------------------------------------------------------------
-- Calculates gross completed volume, total refunds/chargebacks, and net
-- payout amount per merchant.

SELECT
  merchant_id,
  SUM(CASE WHEN status = 'COMPLETED' THEN amount ELSE 0 END) AS completed_amount,
  SUM(CASE WHEN status IN ('REFUNDED', 'CHARGEBACK') THEN amount ELSE 0 END) AS corrected_amount,
  SUM(CASE WHEN status = 'COMPLETED' THEN amount ELSE 0 END)
    - SUM(CASE WHEN status IN ('REFUNDED', 'CHARGEBACK') THEN amount ELSE 0 END) AS net_payout
FROM MERIDIAN_PAY.PAYMENTS.fact_transactions
GROUP BY merchant_id
ORDER BY net_payout DESC;


-- ---------------------------------------------------------------------
-- STEP 2: Identify Sample Corrected Record
-- ---------------------------------------------------------------------
-- Fetches a single refunded or charged-back transaction to use as a target ID.

SELECT transaction_id, merchant_id, amount, status, last_updated_at
FROM MERIDIAN_PAY.PAYMENTS.fact_transactions
WHERE status IN ('REFUNDED', 'CHARGEBACK')
LIMIT 1;


-- ---------------------------------------------------------------------
-- STEP 3: Current State vs. Historical Time Travel State
-- ---------------------------------------------------------------------
-- Verifies the current post-correction state of a transaction and compares it
-- with how the record looked prior to the correction timestamp.

-- State today (post-correction):
SELECT transaction_id, status, amount, last_updated_at
FROM MERIDIAN_PAY.PAYMENTS.fact_transactions
WHERE transaction_id = 't-00142c483a16';

-- State as it looked before the correction arrived (Historical Snapshot):
SELECT transaction_id, status, amount, last_updated_at
FROM MERIDIAN_PAY.PAYMENTS.fact_transactions
  AT (TIMESTAMP => '2026-09-25 06:49:31'::TIMESTAMP_NTZ)
WHERE transaction_id = 't-00142c483a16';


-- ---------------------------------------------------------------------
-- STEP 4: Helper & Target Lookup Queries
-- ---------------------------------------------------------------------
-- Lookups to grab a completed transaction ID and inspect specific records.

-- Get sample completed transaction ID:
SELECT transaction_id 
FROM MERIDIAN_PAY.PAYMENTS.fact_transactions
WHERE status = 'COMPLETED' 
LIMIT 1;

-- Check status of specific target transaction pre-refresh:
SELECT transaction_id, status, last_updated_at
FROM MERIDIAN_PAY.PAYMENTS.fact_transactions
WHERE transaction_id = 't-00010e956833';


-- ---------------------------------------------------------------------
-- STEP 5: Refresh Iceberg Table Metadata Pointer
-- ---------------------------------------------------------------------
-- Updates Snowflake's unmanaged Iceberg table to reference the latest 
-- metadata JSON snapshot commit created in Backblaze B2 (00031 commit).

CREATE OR REPLACE ICEBERG TABLE MERIDIAN_PAY.PAYMENTS.fact_transactions
  EXTERNAL_VOLUME = 'meridian_b2_volume'
  CATALOG = 'meridian_object_store_catalog'
  METADATA_FILE_PATH = 'payments/fact_transactions/metadata/00031-4e36346d-e635-43f0-9ebc-266613897814.metadata.json';


-- ---------------------------------------------------------------------
-- STEP 6: Post-Refresh Verification
-- ---------------------------------------------------------------------
-- Confirms that Snowflake reads the updated REFUNDED status for the target row.

SELECT transaction_id, status, last_updated_at
FROM MERIDIAN_PAY.PAYMENTS.fact_transactions
WHERE transaction_id = 't-00010e956833';