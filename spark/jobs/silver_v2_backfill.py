"""Backfill one or all Bronze releases into isolated Silver V2 Iceberg tables."""

import argparse
import json
from pyspark.sql import SparkSession
from silver.registry import select_datasets
from silver_v2.foundation import create_tables, seed_foundation
from silver_v2.pipeline import run_dataset


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-id", action="append")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--result-file")
    args = parser.parse_args()
    spark = (SparkSession.builder.appName("silver-v2-backfill")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        create_tables(spark)
        seed_foundation(spark)
        failed = False
        results = []
        for dataset in select_datasets(args.dataset_id):
            try:
                result = run_dataset(spark, dataset, args.run_id)
                results.append(result)
                print(json.dumps(result), flush=True)
            except Exception as exc:
                import traceback
                traceback.print_exc()
                result = {"dataset_id": dataset, "status": "FAILED", "error": str(exc)[:500]}
                results.append(result)
                print(json.dumps(result), flush=True)
                failed = True
        if args.result_file:
            from pathlib import Path
            Path(args.result_file).write_text(json.dumps(results, indent=2))
        return 1 if failed else 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
