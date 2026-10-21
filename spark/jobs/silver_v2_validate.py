"""Reproducible acceptance gates over the eight Silver V2 Iceberg domains."""

import argparse
import json
from datetime import datetime

from pyspark.sql import SparkSession, functions as F

from silver_v2.domains import DOMAIN
from silver_v2.foundation import _upsert
from silver_v2.identity import digest


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default="silver-v2-acceptance")
    args = parser.parse_args()
    spark = (SparkSession.builder.appName("silver-v2-acceptance")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    checks = []

    def record(object_name, rule, checked, failed, severity="BLOCK", threshold=0):
        checks.append(dict(
            dq_result_id=digest(args.run_id, object_name, rule), run_id=args.run_id,
            object_name=object_name, rule_code=rule, severity=severity,
            checked_count=int(checked), failed_count=int(failed), threshold=float(threshold),
            status="FAIL" if severity == "BLOCK" and failed > threshold else
                   "WARN" if failed else "PASS",
            checked_at=datetime.utcnow(),
        ))

    try:
        sets = spark.table("lakehouse.silver_meta.observation_set")
        registry = spark.table("lakehouse.silver_meta.dataset_registry")
        record("silver_meta.observation_set", "PUBLISHED_SET_COUNT", 26,
               abs(26 - sets.filter("status='PUBLISHED'").count()))
        record("silver_meta.observation_set", "SOURCE_RECORD_COUNT_PRESENT", 26,
               sets.filter("source_record_count IS NULL").count())
        record("silver_meta.dataset_registry", "REGISTRY_COUNT", 26,
               abs(26 - registry.count()))
        observed_set_ids = []
        for domain in DOMAIN:
            table = f"lakehouse.silver_core.{domain}"
            frame = spark.table(table)
            total = frame.count()
            record(table, "NONEMPTY", 1, int(total == 0))
            duplicate = frame.groupBy("observation_id").count().filter("count > 1").count()
            record(table, "ATOMIC_ID_UNIQUE", total, duplicate)
            missing_lineage = frame.filter(
                F.col("observation_id").isNull() |
                F.col("observation_set_id").isNull() |
                F.col("source_row_locator").isNull() |
                F.col("source_record_hash").isNull() |
                F.col("bronze_path").isNull()
            ).count()
            record(table, "LINEAGE_COMPLETE", total, missing_lineage)
            observed_set_ids.append(frame.select("observation_set_id").distinct())
        ids = observed_set_ids[0]
        for frame in observed_set_ids[1:]:
            ids = ids.unionByName(frame)
        ids = ids.distinct()
        missing_set = ids.join(sets.select("observation_set_id"),
                               "observation_set_id", "left_anti").count()
        record("silver_core.*", "OBSERVATION_SET_FK", ids.count(), missing_set)

        price = spark.table("lakehouse.silver_core.price_observation")
        bad_xor = price.filter(
            (F.col("value_status") != "MISSING") &
            (F.col("price_amount_native").isNull() == F.col("price_index_value").isNull())
        ).count()
        record("silver_core.price_observation", "AMOUNT_INDEX_XOR", price.count(), bad_xor)
        stock = spark.table("lakehouse.silver_core.coffee_stock_observation")
        bad_stock = stock.filter(
            F.col("ending_stock_tonnes") !=
            F.col("ending_stock_thousand_bags") * F.col("bag_weight_kg")
        ).count()
        record("silver_core.coffee_stock_observation", "BAG_CONVERSION", stock.count(), bad_stock)
        weather = spark.table("lakehouse.silver_core.weather_daily")
        names = ["temperature_avg_c", "temperature_max_c", "temperature_min_c",
                 "relative_humidity_pct", "wind_speed_m_s", "precipitation_mm",
                 "solar_native_value"]
        valid_count = sum(F.when(F.col(name).isNotNull(), 1).otherwise(0) for name in names)
        bad_weather = weather.filter(F.col("valid_measure_count") != valid_count).count()
        record("silver_core.weather_daily", "VALID_MEASURE_COUNT", weather.count(), bad_weather)
        sentinel = weather.filter(" OR ".join(f"{name}=-999" for name in names)).count()
        record("silver_core.weather_daily", "NO_SENTINEL_IN_MEASURES", weather.count(), sentinel)
        macro = spark.table("lakehouse.silver_core.macro_indicator_observation")
        missing_wdi = macro.filter("value_status='MISSING'").count()
        record("silver_core.macro_indicator_observation", "SOURCE_NULLS_RETAINED", 138,
               abs(138 - missing_wdi))
        decisions = spark.table("lakehouse.silver_meta.canonical_decision").filter(
            "policy_version='source-local-v3'")
        multiple_winners = decisions.filter("decision_status='WINNER'")\
            .groupBy("business_process", "canonical_business_key")\
            .count().filter("count>1").count()
        record("silver_meta.canonical_decision", "ONE_WINNER_MAX", decisions.count(),
               multiple_winners)
        conflicts = spark.table("lakehouse.silver_meta.canonical_conflict").filter(
            "resolution_status='OPEN' AND business_process='price_observation' "
            "AND policy_version='source-local-v3'").count()
        record("silver_meta.canonical_conflict", "OPEN_PRICE_CONFLICTS", 6, conflicts,
               severity="WARN")
        district = spark.table("lakehouse.silver_core.provincial_agri_observation")\
            .filter("geography_level='DISTRICT' AND geography_code IS NULL").count()
        record("silver_core.provincial_agri_observation", "UNMAPPED_DISTRICTS", district,
               district, severity="WARN")
        _upsert(spark, "lakehouse.silver_meta.dq_result", checks, ["dq_result_id"])
        print(json.dumps({"checks": checks, "block_failures":
                          sum(x["status"] == "FAIL" for x in checks)},
                         default=str), flush=True)
        return 1 if any(x["status"] == "FAIL" for x in checks) else 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
