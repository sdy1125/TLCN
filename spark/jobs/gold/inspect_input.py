"""Read-only readiness check for a reproducible published Silver input."""

import json
from pyspark.sql import SparkSession
from gold.source import resolve


def main():
    spark = (SparkSession.builder.appName("gold-inspect-input")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        try:
            batch, snapshots = resolve(spark)
        except Exception as exc:
            print(json.dumps({"status": "BLOCKED", "reason": str(exc)}), flush=True)
            return 2
        print(json.dumps({"status": "READY", "silver_batch_id": batch["batch_id"],
                          "policy_version": batch["policy_version"],
                          "snapshots": snapshots}, sort_keys=True), flush=True)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
