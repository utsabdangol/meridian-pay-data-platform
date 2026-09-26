"""
view_snapshot_log.py
---------------------
Fetches fact_transactions' current metadata.json from B2 and prints its
snapshot history with human-readable timestamps, so you can pick one to
use in a Snowflake AT (TIMESTAMP => ...) time-travel query without any
separate epoch-conversion step.

Usage:
  python view_snapshot_log.py
"""

import os
import json
import boto3
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent
load_dotenv(REPO_ROOT / ".env")

# Update this if you create a newer metadata.json later
METADATA_KEY = "payments/fact_transactions/metadata/00030-42e30d3b-2d36-435c-adb5-0ba419807c64.metadata.json"


def main():
    s3 = boto3.client(
        "s3",
        endpoint_url=f"https://{os.environ['Endpoint']}",
        aws_access_key_id=os.environ["keyID"],
        aws_secret_access_key=os.environ["applicationKey"],
        region_name="us-east-1",
    )

    response = s3.get_object(Bucket=os.environ["bucket"], Key=METADATA_KEY)
    metadata = json.loads(response["Body"].read())

    print(f"{'#':>3}  {'Snapshot Timestamp (UTC)':<26}  Snapshot ID")
    print("-" * 60)
    for i, entry in enumerate(metadata["snapshot-log"]):
        ts = datetime.fromtimestamp(entry["timestamp-ms"] / 1000, tz=timezone.utc)
        print(f"{i:>3}  {ts.strftime('%Y-%m-%d %H:%M:%S UTC'):<26}  {entry['snapshot-id']}")

    print(f"\n[INFO] {len(metadata['snapshot-log'])} snapshot(s) total.")
    print("[INFO] Copy a timestamp above directly into:")
    print("       ... AT (TIMESTAMP => '<timestamp>'::TIMESTAMP_NTZ) ...")


if __name__ == "__main__":
    main()