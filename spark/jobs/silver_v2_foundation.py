"""Create and seed V2 metadata/reference namespaces from immutable Bronze metadata."""

import json
from pyspark.sql import SparkSession
from silver_v2.foundation import create_tables, seed_foundation


def main():
    spark = (SparkSession.builder.appName("silver-v2-foundation")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        create_tables(spark)
        print(json.dumps(seed_foundation(spark)), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
