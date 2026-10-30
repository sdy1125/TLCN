"""Build eight canonical Gold Facts and observation-level lineage."""

import json

from pyspark.sql import SparkSession
from gold.build_facts import build


def main():
    spark = (SparkSession.builder.appName("gold-facts")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        print(json.dumps(build(spark)), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
