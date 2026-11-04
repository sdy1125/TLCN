"""Create an immutable, quality-gated Silver snapshot candidate."""

import argparse
import json
from datetime import datetime

from pyspark.sql import SparkSession, functions as F

from silver.domains import DOMAIN
from silver.dq_contract import ACCEPTED_WARNING_RULES, evidence
from silver.foundation import _upsert, create_tables
from silver.policy import current_policy_version
from silver.publication import (
    BATCH, capture, load_batch, make_manifest, validate_batch_id,
)
from silver.readers import release_manifest


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--dq-run-id", required=True)
    args = parser.parse_args()
    validate_batch_id(args.batch_id)
    spark = (SparkSession.builder.appName("silver-publish-review")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        create_tables(spark)
        policy_version = current_policy_version(spark)
        dq_rows = spark.table("lakehouse.silver_meta.dq_result").filter(
            F.col("run_id") == args.dq_run_id).collect()
        if not dq_rows or any(row["policy_version"] != policy_version
                              for row in dq_rows):
            raise ValueError("Silver DQ run does not match current policy")
        warnings = evidence(dq_rows)
        existing = spark.table(BATCH).filter(
            F.col("batch_id") == args.batch_id).collect()
        if existing:
            batch, old_manifest = load_batch(spark, args.batch_id)
            if (batch["policy_version"] != policy_version or
                    batch["dq_run_id"] != args.dq_run_id or
                    old_manifest["input_snapshots"] != capture(spark)):
                raise ValueError("Silver batch ID already pins different inputs")
            print(json.dumps({"batch_id": args.batch_id, "status": batch["status"],
                              "manifest_sha256": old_manifest["manifest_sha256"]}),
                  flush=True)
            return
        now = datetime.utcnow()
        exception_rows = [dict(
            batch_id=args.batch_id, rule_code=rule, affected_count=count,
            reason=ACCEPTED_WARNING_RULES[rule],
            approval_status="ACCEPTED_BY_POLICY",
            approved_by="silver_exception_policy_v1", approved_at=now,
        ) for rule, count in sorted(warnings.items())]
        _upsert(spark, "lakehouse.silver_meta.release_exception",
                exception_rows, ["batch_id", "rule_code"])
        snapshots = capture(spark)
        rows = {domain: spark.table(f"lakehouse.silver_core.{domain}").count()
                for domain in DOMAIN}
        bronze = release_manifest()
        manifest = make_manifest(args.batch_id, policy_version, args.dq_run_id,
                                 snapshots, rows, warnings, bronze)
        core = {domain: dict(
            snapshot_id=snapshots[f"lakehouse.silver_core.{domain}"],
            rows=rows[domain]) for domain in DOMAIN}
        status = "READY_WITH_EXCEPTIONS" if warnings else "READY"
        row = dict(batch_id=args.batch_id, namespace="silver_core",
                   policy_version=policy_version, status=status,
                   domain_snapshots_json=json.dumps(core, sort_keys=True),
                   dq_run_id=args.dq_run_id, created_at=now,
                   activation_reason="Published Silver snapshot candidate",
                   release_manifest_json=json.dumps(manifest, sort_keys=True))
        _upsert(spark, BATCH, [row], ["batch_id"])
        print(json.dumps({"batch_id": args.batch_id, "policy_version": policy_version,
                          "status": status, "warnings": warnings,
                          "manifest_sha256": manifest["manifest_sha256"],
                          "snapshots": snapshots}), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
