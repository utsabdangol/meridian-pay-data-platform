-- =====================================================================
-- SNOWFLAKE EXTERNAL VOLUME & CONNECTION SETUP FOR BACKBLAZE B2
-- Project: Meridian Pay Data Platform
-- Target: Snowflake External Volume & Stage setup for Iceberg tables
-- Provider: Backblaze B2 (S3-Compatible Storage)
-- =====================================================================
-- Variables match your .env file:
--   ${bucket}         -> B2 bucket name
--   ${Endpoint}       -> B2 endpoint (e.g. s3.us-east-005.backblazeb2.com)
--   ${keyID}          -> B2 Application Key ID
--   ${applicationKey} -> B2 Application Key
-- =====================================================================
-- Purpose: Set up external volume, catalog integration, and attach 
--          an Apache Iceberg unmanaged table referencing B2 storage.
-- =====================================================================

-- ---------------------------------------------------------------------
-- STEP 1: Database & Schema Setup
-- ---------------------------------------------------------------------
-- Ensure high-privilege role is active and create target database & schema.

USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS MERIDIAN_PAY;
CREATE SCHEMA IF NOT EXISTS MERIDIAN_PAY.PAYMENTS;

USE DATABASE MERIDIAN_PAY;
USE SCHEMA PAYMENTS;


-- ---------------------------------------------------------------------
-- STEP 2: Create External Volume (Backblaze B2 Connection)
-- ---------------------------------------------------------------------
-- Defines the physical connection properties, S3-compatible endpoint, 
-- and credentials for the Backblaze B2 storage bucket.

CREATE OR REPLACE EXTERNAL VOLUME meridian_b2_volume
  STORAGE_LOCATIONS = (
    (
      NAME = 'b2_meridian_pay_lakehouse'
      STORAGE_PROVIDER = 'S3COMPAT'
      STORAGE_BASE_URL = 's3compat://${bucket}/'
      STORAGE_ENDPOINT = '${Endpoint}'
      CREDENTIALS = (
        AWS_KEY_ID = '${keyID}'
        AWS_SECRET_KEY = '${applicationKey}'
      )
    )
  )
  ALLOW_WRITES = FALSE;

-- Verify the external volume configuration properties
DESCRIBE EXTERNAL VOLUME meridian_b2_volume;


-- ---------------------------------------------------------------------
-- STEP 3: Connectivity Verification (Stage & List Test)
-- ---------------------------------------------------------------------
-- Quick sanity check using a standard stage to verify Snowflake has 
-- valid permissions to reach and read objects inside the B2 bucket.

CREATE OR REPLACE STAGE meridian_b2_test_stage
  URL = 's3compat://${bucket}/'
  ENDPOINT = '${Endpoint}'
  CREDENTIALS = (
    AWS_KEY_ID = '${keyID}'
    AWS_SECRET_KEY = '${applicationKey}'
  );

-- Verify Snowflake can list files stored in the bucket:
LIST @meridian_b2_test_stage;


-- ---------------------------------------------------------------------
-- STEP 4: Create Catalog Integration
-- ---------------------------------------------------------------------
-- Enables Snowflake to read Apache Iceberg metadata files directly 
-- from an external object store without an external metastore.

CREATE OR REPLACE CATALOG INTEGRATION meridian_object_store_catalog
  CATALOG_SOURCE = OBJECT_STORE
  TABLE_FORMAT = ICEBERG
  ENABLED = TRUE;


-- ---------------------------------------------------------------------
-- STEP 5: Create Unmanaged Iceberg Table
-- ---------------------------------------------------------------------
-- Mounts the Iceberg table in Snowflake by referencing the latest 
-- metadata JSON file path located in your B2 bucket.

CREATE OR REPLACE ICEBERG TABLE MERIDIAN_PAY.PAYMENTS.fact_transactions
  EXTERNAL_VOLUME = 'meridian_b2_volume'
  CATALOG = 'meridian_object_store_catalog'
  METADATA_FILE_PATH = 'payments/fact_transactions/metadata/00030-42e30d3b-2d36-435c-adb5-0ba419807c64.metadata.json';


-- ---------------------------------------------------------------------
-- STEP 6: Data Verification Query
-- ---------------------------------------------------------------------
-- Query the newly mounted Iceberg table to confirm data readability.

SELECT COUNT(*) AS total_transactions 
FROM MERIDIAN_PAY.PAYMENTS.fact_transactions;