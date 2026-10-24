"""Create and seed metadata/reference namespaces from immutable Bronze metadata."""

import json
from pyspark.sql import SparkSession
from silver.foundation import create_tables, seed_foundation


def main():
    spark = (SparkSession.builder.appName("silver-foundation")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        create_tables(spark)
        print(json.dumps(seed_foundation(spark)), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
