"""Record an immutable reviewable V2 batch; never switch consumers itself."""

import argparse
import json
from datetime import datetime

from pyspark.sql import SparkSession, functions as F

from silver_v2.domains import DOMAIN
from silver_v2.foundation import _upsert, create_tables


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--dq-run-id", required=True)
    args = parser.parse_args()
    spark = (SparkSession.builder.appName("silver-v2-publish-review")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        create_tables(spark)
        dq = spark.table("lakehouse.silver_meta.dq_result").filter(
            F.col("run_id") == args.dq_run_id
        )
        failures = dq.filter("severity='BLOCK' AND status='FAIL'").count()
        warnings = dq.filter("status='WARN'").count()
        if dq.count() < 36 or failures:
            raise ValueError("Acceptance checks incomplete or failing")
        snapshots = {}
        for domain in DOMAIN:
            table = f"lakehouse.silver_core.{domain}"
            history = spark.sql(
                f"SELECT snapshot_id FROM {table}.history ORDER BY made_current_at DESC LIMIT 1"
            ).first()
            if history is None:
                raise ValueError("Missing snapshot: " + domain)
            snapshots[domain] = dict(snapshot_id=history[0], rows=spark.table(table).count())
        status = "BLOCKED_BUSINESS_REVIEW" if warnings else "READY_FOR_REVIEW"
        row = dict(batch_id=args.batch_id, namespace="silver_core",
                   policy_version="source-local-v3", status=status,
                   domain_snapshots_json=json.dumps(snapshots, sort_keys=True),
                   dq_run_id=args.dq_run_id, created_at=datetime.utcnow(),
                   activation_reason="Gold Dimension contract and source-policy review pending")
        _upsert(spark, "lakehouse.silver_meta.publish_batch", [row], ["batch_id"])
        print(json.dumps({"batch_id": args.batch_id, "status": status,
                          "block_failures": failures, "warnings": warnings,
                          "snapshots": snapshots}), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
