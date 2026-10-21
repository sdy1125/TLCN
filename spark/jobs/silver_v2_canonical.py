"""Audit deterministic source-local canonical decisions across eight domains."""

import json
import argparse
from pyspark.sql import SparkSession
from silver_v2.domains import DOMAIN
from silver_v2.canonical import decide_domain


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--domain", action="append")
    parser.add_argument("--policy-version", default="source-local-v1")
    args = parser.parse_args()
    spark = (SparkSession.builder.appName("silver-v2-canonical")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        for domain in (args.domain or DOMAIN):
            print(json.dumps(decide_domain(spark, domain, args.policy_version)), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
