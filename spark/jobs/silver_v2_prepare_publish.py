"""Record an immutable reviewable V2 batch; never switch consumers itself."""

import argparse
import json
from datetime import datetime

from pyspark.sql import SparkSession, functions as F

from silver_v2.domains import DOMAIN
from silver_v2.foundation import _upsert, create_tables
from silver_v2.policy import current_policy_version


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
        policy_version = current_policy_version(spark)
        dq = spark.table("lakehouse.silver_meta.dq_result").filter(
            F.col("run_id") == args.dq_run_id
        )
        failures = dq.filter("severity='BLOCK' AND status='FAIL'").count()
        warning_rows = dq.filter("status='WARN'")\
            .select("rule_code", "failed_count").collect()
        warnings = len(warning_rows)
        # Three metadata, five per-domain, and eleven cross-domain checks.
        if dq.count() < 54 or failures:
            raise ValueError("Acceptance checks incomplete or failing")
        expected_exceptions = {
            ("OPEN_PRICE_CONFLICTS", 6),
            ("PENDING_OFFICIAL_DISTRICT_MAPPING", 2142),
        }
        actual_exceptions = {(row[0], row[1]) for row in warning_rows}
        snapshots = {}
        for domain in DOMAIN:
            table = f"lakehouse.silver_core.{domain}"
            history = spark.sql(
                f"SELECT snapshot_id FROM {table}.history ORDER BY made_current_at DESC LIMIT 1"
            ).first()
            if history is None:
                raise ValueError("Missing snapshot: " + domain)
            snapshots[domain] = dict(snapshot_id=history[0], rows=spark.table(table).count())
        status = ("READY_WITH_EXCEPTIONS" if actual_exceptions == expected_exceptions
                  else "READY_FOR_REVIEW" if not warnings else "BLOCKED_BUSINESS_REVIEW")
        row = dict(batch_id=args.batch_id, namespace="silver_core",
                   policy_version=policy_version, status=status,
                   domain_snapshots_json=json.dumps(snapshots, sort_keys=True),
                   dq_run_id=args.dq_run_id, created_at=datetime.utcnow(),
                   activation_reason=(
                       "Price conflicts excluded; source-scoped district keys pending official mapping; "
                       "Comtrade canonical inactive; NASA solar canonical unit unverified"))
        _upsert(spark, "lakehouse.silver_meta.publish_batch", [row], ["batch_id"])
        print(json.dumps({"batch_id": args.batch_id, "policy_version": policy_version,
                          "status": status,
                          "block_failures": failures, "warnings": warnings,
                          "snapshots": snapshots}), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
