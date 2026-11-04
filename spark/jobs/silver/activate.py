"""Activate or restore a Silver release through one atomic pointer row."""

import argparse
import json

from pyspark.sql import SparkSession, functions as F

from silver.dq_contract import evidence
from silver.foundation import create_tables
from silver.policy import current_policy_version
from silver.publication import (
    BATCH, capture, current_pointer, flip,
    load_batch, validate_batch_id,
)


def _validate_candidate(spark, batch_id):
    batch, manifest = load_batch(spark, batch_id)
    if batch["status"] not in {"READY", "READY_WITH_EXCEPTIONS"}:
        raise ValueError("Silver candidate has not passed prepare")
    if batch["policy_version"] != current_policy_version(spark):
        raise ValueError("Silver policy changed after candidate preparation")
    rows = spark.table("lakehouse.silver_meta.dq_result").filter(
        F.col("run_id") == batch["dq_run_id"]).collect()
    if any(row["policy_version"] != batch["policy_version"] for row in rows):
        raise ValueError("Silver DQ policy version drift")
    warnings = evidence(rows)
    if manifest["warnings"] != warnings:
        raise ValueError("Silver exceptions changed after preparation")
    approvals = spark.table("lakehouse.silver_meta.release_exception").filter(
        F.col("batch_id") == batch_id).collect()
    approved = {row["rule_code"]: row["affected_count"] for row in approvals
                if row["approval_status"] == "ACCEPTED_BY_POLICY" and
                row["approved_by"] and row["approved_at"]}
    if len(approvals) != len(approved) or approved != warnings:
        raise ValueError("Silver batch exceptions are not approved")
    current = capture(spark)
    if current != manifest["input_snapshots"]:
        raise ValueError("Silver input snapshots changed after preparation")
    for domain, expected in manifest["domain_rows"].items():
        if spark.table(f"lakehouse.silver_core.{domain}").count() != expected:
            raise ValueError(f"Silver row count changed: {domain}")
    return batch, manifest


def _validate_legacy_rollback(spark, batch_id):
    rows = spark.table(BATCH).filter(F.col("batch_id") == batch_id).collect()
    if len(rows) != 1 or rows[0]["status"] not in {
            "ACTIVE", "ACTIVE_WITH_EXCEPTIONS", "RETIRED",
            "RETIRED_WITH_EXCEPTIONS"}:
        raise ValueError("Rollback target was not a published Silver release")
    batch = rows[0].asDict()
    if batch.get("release_manifest_json"):
        _, manifest = load_batch(spark, batch_id)
        snapshots = manifest["input_snapshots"]
    else:
        snapshots = {
            f"lakehouse.silver_core.{domain}": int(info["snapshot_id"])
            for domain, info in json.loads(batch["domain_snapshots_json"]).items()
        }
    for table, snapshot_id in snapshots.items():
        if snapshot_id is None:
            continue
        history = {int(row[0]) for row in spark.sql(
            f"SELECT snapshot_id FROM {table}.history").collect()}
        if int(snapshot_id) not in history:
            raise ValueError(f"Rollback Silver snapshot missing: {table}")
    return batch


def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--batch-id")
    group.add_argument("--rollback-to")
    args = parser.parse_args()
    target = validate_batch_id(args.batch_id or args.rollback_to)
    spark = (SparkSession.builder.appName("silver-activate")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        create_tables(spark)
        if args.rollback_to:
            batch = _validate_legacy_rollback(spark, target)
            warnings = json.loads(batch["release_manifest_json"])["warnings"] if (
                batch.get("release_manifest_json")) else (
                    {"LEGACY_ACCEPTED_EXCEPTIONS": 1} if
                    batch["status"].endswith("WITH_EXCEPTIONS") else {})
        else:
            batch, manifest = _validate_candidate(spark, target)
            warnings = manifest["warnings"]
        pointer = current_pointer(spark)
        if pointer["active_batch_id"] == target:
            print(json.dumps({"batch_id": target, "status": "ALREADY_ACTIVE"}),
                  flush=True)
            return
        flip(spark, pointer, target)
        status = "ACTIVE_WITH_EXCEPTIONS" if warnings else "ACTIVE"
        spark.sql(
            f"UPDATE {BATCH} SET status='{status}', "
            "activated_at=current_timestamp() "
            f"WHERE batch_id='{target}'")
        old = validate_batch_id(pointer["active_batch_id"])
        retired = ("RETIRED_WITH_EXCEPTIONS" if
                   spark.table(BATCH).filter(F.col("batch_id") == old).first()["status"]
                   .endswith("WITH_EXCEPTIONS") else "RETIRED")
        spark.sql(f"UPDATE {BATCH} SET status='{retired}' "
                  f"WHERE batch_id='{old}'")
        print(json.dumps({"batch_id": target, "status": status,
                          "pointer_generation": pointer["generation"] + 1}),
              flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
