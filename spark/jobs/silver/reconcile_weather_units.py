"""Idempotently apply the verified NASA POWER unit to existing Silver rows."""

import json

from pyspark.sql import SparkSession


def main():
    spark = (SparkSession.builder.appName("silver-reconcile-weather-units")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    counts = {}
    try:
        for layer in ("silver_stage", "silver_core"):
            table = f"lakehouse.{layer}.weather_daily"
            mismatch = (
                "dataset_id='nasa_power_weather_daily_63' AND ("
                "NOT (solar_native_unit <=> 'MJ/m^2/day') OR "
                "NOT (solar_radiation_mj_m2_day <=> solar_native_value))"
            )
            before = spark.sql(f"SELECT count(*) FROM {table} WHERE {mismatch}").first()[0]
            if before:
                spark.sql(
                    f"UPDATE {table} SET solar_native_unit='MJ/m^2/day', "
                    "solar_radiation_mj_m2_day=solar_native_value "
                    f"WHERE {mismatch}")
            after = spark.sql(f"SELECT count(*) FROM {table} WHERE {mismatch}").first()[0]
            if after:
                raise ValueError(f"Solar reconciliation incomplete in {table}: {after}")
            counts[layer] = int(before)
        print(json.dumps(counts, sort_keys=True), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
