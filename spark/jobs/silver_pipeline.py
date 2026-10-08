"""Production entry point; never reads data/raw."""

import argparse
import json
import sys
import fcntl
from pathlib import Path
from pyspark.sql import SparkSession
from silver.pipeline import run_dataset
from silver.registry import select_datasets
from silver.writer import identifier


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-id", action="append")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--namespace", default="lakehouse.silver")
    parser.add_argument("--result-file")
    args = parser.parse_args()
    identifier(args.namespace)
    datasets = select_datasets(args.dataset_id)
    # Serialize all entry-point runs, including CLI and Airflow, in this container.
    lock = open("/tmp/silver-publish.lock", "w")
    fcntl.flock(lock, fcntl.LOCK_EX)
    spark = (
        SparkSession.builder.appName("silver-transformation")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    results = []
    try:
        for dataset in datasets:
            try:
                results.append(run_dataset(spark, dataset, args.run_id, args.namespace))
            except Exception as exc:
                import traceback

                traceback.print_exc()
                results.append(
                    {
                        "dataset_id": dataset,
                        "status": "FAILED",
                        "error_type": type(exc).__name__,
                    }
                )
        from silver.reconciliation import run as reconcile

        reconcile(spark, args.namespace, args.run_id)
        if args.result_file:
            Path(args.result_file).write_text(json.dumps(results, indent=2))
        print(json.dumps(results, indent=2))
        return 1 if any(r["status"] == "FAILED" for r in results) else 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
