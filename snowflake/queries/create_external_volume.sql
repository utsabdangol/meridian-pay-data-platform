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

-- ---------------------------------------------------------------------
-- 1. Database & Schema Context
-- ---------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;
CREATE DATABASE IF NOT EXISTS MERIDIAN_PAY;
CREATE SCHEMA IF NOT EXISTS MERIDIAN_PAY.PAYMENTS;
USE SCHEMA MERIDIAN_PAY.PAYMENTS;

-- ---------------------------------------------------------------------
-- 2. Create S3-Compatible External Volume for Backblaze B2
-- ---------------------------------------------------------------------
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

-- Describe volume to verify configuration
DESCRIBE EXTERNAL VOLUME meridian_b2_volume;


-- ---------------------------------------------------------------------
-- 3. Connectivity Verification Stage (Fastest way to test B2 auth)
-- ---------------------------------------------------------------------
CREATE OR REPLACE STAGE meridian_b2_test_stage
  URL = 's3compat://${bucket}/'
  ENDPOINT = '${Endpoint}'
  CREDENTIALS = (
    AWS_KEY_ID = '${keyID}'
    AWS_SECRET_KEY = '${applicationKey}'
  );

-- Run this to verify Snowflake can list files in your B2 bucket:
LIST @meridian_b2_test_stage;


-- ---------------------------------------------------------------------
-- 4. Template: Create Iceberg Table from B2 External Volume
-- ---------------------------------------------------------------------
-- CREATE OR REPLACE ICEBERG TABLE fact_transactions
--   EXTERNAL_VOLUME = 'meridian_b2_volume'
--   CATALOG = 'SNOWFLAKE'
--   METADATA_FILE_PATH = 'payments/fact_transactions/metadata/<latest-metadata-file>.metadata.json';
