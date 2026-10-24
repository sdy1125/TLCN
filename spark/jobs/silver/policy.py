"""Seed reviewed source-local policy rows for all observed dataset/metric grains."""

import json
from pyspark.sql import SparkSession
from silver.policy import seed


if __name__ == "__main__":
    spark = (SparkSession.builder.appName("silver-policy")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        print(json.dumps(seed(spark)), flush=True)
    finally:
        spark.stop()
