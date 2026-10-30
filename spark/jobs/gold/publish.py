"""Validate and activate a snapshot-pinned Gold publication."""

import json

from pyspark.sql import SparkSession
from gold.quality import publish


def main():
    spark = (SparkSession.builder.appName("gold-publication")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        result = publish(spark)
        print(json.dumps(result, default=str), flush=True)
        return 0 if result['status'] in {'ACTIVE_WITH_EXCEPTIONS', 'ALREADY_PUBLISHED'} else 1
    finally:
        spark.stop()


if __name__ == '__main__':
    raise SystemExit(main())
