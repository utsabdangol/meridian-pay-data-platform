# Meridian Pay Transaction Data Platform

A proof-of-concept transaction data platform for handling:

- Daily payment transactions
- Late corrections and refunds
- Historical transaction versions
- Iceberg time travel
- Merchant settlement reporting
- Snowflake analytics

## Current Pipeline

Sample CSV
    ↓
PySpark
    ↓
Apache Iceberg
    ↓
Snowflake
    ↓
Settlement Report

## Setup

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt