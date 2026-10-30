"""Load Gold dimensions and classification bridge from published Silver refs."""

import json

from pyspark.sql import SparkSession
from gold.build_dimensions import build


def main():
    spark = (SparkSession.builder.appName("gold-dimensions")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        print(json.dumps(build(spark)), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
