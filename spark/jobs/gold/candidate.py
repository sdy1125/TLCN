"""Build and validate a private Gold candidate from one immutable manifest."""

import argparse
import json

from pyspark.sql import SparkSession

from gold.build_dimensions import build as build_dimensions
from gold.build_facts import build as build_facts
from gold.candidate import (
    create_manifest, ensure_candidate, load_manifest, candidate_namespace,
)
from gold.quality import audit
from silver.foundation import _upsert


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest-id", required=True)
    parser.add_argument("--stage", required=True, choices=(
        "inspect", "foundation", "dimensions", "facts", "validate"))
    args = parser.parse_args()
    spark = (SparkSession.builder.appName("gold-candidate-" + args.stage)
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        if args.stage == "inspect":
            context = create_manifest(spark, args.manifest_id)
            result = {"status": "PINNED", "manifest_id": args.manifest_id,
                      "silver_batch_id": context[0]["batch_id"],
                      "snapshots": context[1]}
        else:
            context = load_manifest(spark, args.manifest_id)
            registry = "lakehouse.gold_meta.release_registry"
            if spark.catalog.tableExists(registry) and spark.table(registry).filter(
                    f"gold_batch_id='{args.manifest_id}' AND status='PUBLISHED'").limit(1).count():
                raise RuntimeError("Published Gold candidate is immutable")
            namespace = ensure_candidate(spark, args.manifest_id)
            if args.stage == "foundation":
                result = {"status": "READY", "candidate_namespace": namespace}
            elif args.stage == "dimensions":
                result = build_dimensions(spark, context, namespace)
            elif args.stage == "facts":
                result = build_facts(spark, context, namespace)
            else:
                _, _, _, counts, snapshots, checks = audit(
                    spark, context, namespace)
                failures = [item for item in checks if item["status"] == "FAIL"]
                _upsert(spark, f"lakehouse.{namespace}.dq_result", checks,
                        ["gold_batch_id", "table_name", "rule_code"])
                result = {"status": "BLOCKED" if failures else "VALIDATED",
                          "checks": len(checks), "failures": [
                              (item["table_name"], item["rule_code"],
                               item["failed_count"]) for item in failures],
                          "row_counts": counts, "snapshots": snapshots}
        print(json.dumps(result, default=str, sort_keys=True), flush=True)
        return 1 if result.get("status") == "BLOCKED" else 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
