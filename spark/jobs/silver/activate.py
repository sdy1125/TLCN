"""Activate a validated Silver batch with an auditable consumer pointer."""

import argparse
import json
from datetime import datetime

from pyspark.sql import SparkSession, functions as F

from silver.domains import DOMAIN
from silver.policy import current_policy_version


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-id", required=True)
    args = parser.parse_args()
    spark = (SparkSession.builder.appName("silver-activate")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        table = "lakehouse.silver_meta.publish_batch"
        rows = spark.table(table).filter(F.col("batch_id") == args.batch_id).collect()
        if len(rows) != 1 or rows[0]["status"] not in {
                "READY_WITH_EXCEPTIONS", "ACTIVE_WITH_EXCEPTIONS"}:
            raise ValueError("Batch has not passed the publish gate")
        batch = rows[0]
        if batch["policy_version"] != current_policy_version(spark):
            raise ValueError("Canonical policy changed after batch preparation")
        dq = spark.table("lakehouse.silver_meta.dq_result").filter(
            F.col("run_id") == batch["dq_run_id"])
        if dq.count() != 54 or dq.filter("severity='BLOCK' AND status='FAIL'").count():
            raise ValueError("Acceptance evidence is incomplete or failing")
        warnings = {(row[0], row[1]) for row in dq.filter("status='WARN'")
                    .select("rule_code", "failed_count").collect()}
        if warnings != {("OPEN_PRICE_CONFLICTS", 6),
                        ("PENDING_OFFICIAL_DISTRICT_MAPPING", 2142)}:
            raise ValueError("Business exceptions changed after batch preparation")
        snapshots = json.loads(batch["domain_snapshots_json"])
        if set(snapshots) != set(DOMAIN):
            raise ValueError("Batch does not cover every domain")
        for domain in DOMAIN:
            name = f"lakehouse.silver_core.{domain}"
            current = spark.sql(
                f"SELECT snapshot_id FROM {name}.history "
                "ORDER BY made_current_at DESC LIMIT 1").first()[0]
            if current != snapshots[domain]["snapshot_id"]:
                raise ValueError(f"Snapshot changed after review: {domain}")
            if spark.table(name).count() != snapshots[domain]["rows"]:
                raise ValueError(f"Row count changed after review: {domain}")
        if batch["status"] == "ACTIVE_WITH_EXCEPTIONS":
            print(json.dumps({"batch_id": args.batch_id, "status": batch["status"]}))
            return
        escaped = args.batch_id.replace("'", "''")
        spark.sql(f"UPDATE {table} SET status='RETIRED' "
                  f"WHERE status LIKE 'ACTIVE%' AND batch_id <> '{escaped}'")
        spark.sql(f"UPDATE {table} SET status='ACTIVE_WITH_EXCEPTIONS', "
                  f"activated_at=TIMESTAMP '{datetime.utcnow().isoformat(sep=' ')}' "
                  f"WHERE batch_id='{escaped}' AND status='READY_WITH_EXCEPTIONS'")
        print(json.dumps({"batch_id": args.batch_id,
                          "status": "ACTIVE_WITH_EXCEPTIONS",
                          "policy_version": batch["policy_version"]}), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
