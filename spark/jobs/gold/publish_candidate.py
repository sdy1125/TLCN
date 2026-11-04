"""Validate, register and activate a private Gold release."""

import argparse
import json

from pyspark.sql import SparkSession

from gold.publication import publish_candidate, rollback


def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--manifest-id")
    group.add_argument("--rollback-to")
    args = parser.parse_args()
    spark = (SparkSession.builder.appName("gold-publish-candidate")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        result = (publish_candidate(spark, args.manifest_id)
                  if args.manifest_id else rollback(spark, args.rollback_to))
        print(json.dumps(result, default=str, sort_keys=True), flush=True)
        return 1 if result["status"] == "BLOCKED" else 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
