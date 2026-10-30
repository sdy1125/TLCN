"""Create and guard Gold Iceberg schemas; never load unpublished Silver data."""

import json
from pyspark.sql import SparkSession
from gold.contracts import TABLES, ensure_storage


def main():
    spark = (SparkSession.builder.appName("gold-foundation")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        ensure_storage(spark)
        print(json.dumps({"status": "CREATED", "tables": {
            namespace: list(tables) for namespace, tables in TABLES.items()
        }}), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
