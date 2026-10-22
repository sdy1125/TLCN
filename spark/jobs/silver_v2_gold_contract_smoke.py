"""Read-only smoke checks for the eight Gold Fact inputs and shared references."""

import json

from pyspark.sql import SparkSession, functions as F

from silver_v2.domains import DOMAIN
from silver_v2.policy import current_policy_version


def main():
    spark = SparkSession.builder.appName("silver-v2-gold-contract-smoke").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    checks = []

    def check(name, failed):
        checks.append({"check": name, "failed": int(failed)})

    try:
        frames = {name: spark.table(f"lakehouse.silver_core.{name}") for name in DOMAIN}
        policy_version = current_policy_version(spark)
        decisions = spark.table("lakehouse.silver_meta.canonical_decision")\
            .filter(F.col("policy_version") == policy_version)

        def winners(name):
            ids = decisions.filter((F.col("business_process") == name) &
                                   (F.col("decision_status") == "WINNER"))\
                .select(F.col("candidate_observation_id").alias("observation_id"))
            return frames[name].join(ids, "observation_id", "inner")

        periods = spark.table("lakehouse.silver_ref.period")\
            .select("period_type", "period_code").distinct()
        for name, frame in frames.items():
            check(f"{name}.period_ref", frame.select("period_type", "period_code")
                  .join(periods, ["period_type", "period_code"], "left_anti").count())
        production = winners("production_observation")
        check("F01.production_keys", production.filter(
            "m49_code IS NULL OR faostat_item_code IS NULL OR calendar_year IS NULL "
            "OR element_code IS NULL OR quality_flag_source IS NULL").count())
        provincial = frames["provincial_agri_observation"]
        geography = spark.table("lakehouse.silver_ref.geography")\
            .select("geography_code", "geography_version").distinct()
        check("F02.geography_ref", provincial.select(
            "geography_code", "geography_version").join(
                geography, ["geography_code", "geography_version"], "left_anti").count())
        check("F02.scope", provincial.filter(
            "geography_level NOT IN ('PROVINCE','DISTRICT') OR "
            "coverage_code IS NULL OR record_status_code IS NULL").count())
        price = frames["price_observation"]
        series = spark.table("lakehouse.silver_ref.price_series")\
            .select("series_code").distinct()
        check("F03.price_series_ref", price.select("series_code")\
              .join(series, "series_code", "left_anti").count())
        check("F03.amount_index_xor", price.filter(
            "value_status <> 'MISSING' AND "
            "((price_amount_native IS NULL AND price_index_value IS NULL) OR "
            "(price_amount_native IS NOT NULL AND price_index_value IS NOT NULL))").count())
        trade_total = winners("trade_total_observation")
        check("F04.trade_total_keys", trade_total.filter(
            "reporter_m49_code IS NULL OR product_code IS NULL OR "
            "trade_flow_code IS NULL OR classification_version IS NULL").count())
        trade_partner = frames["trade_partner_observation"]
        check("F05.trade_partner_keys", trade_partner.filter(
            "reporter_m49_code IS NULL OR partner_m49_code IS NULL OR "
            "product_code IS NULL OR classification_version IS NULL OR "
            "trade_flow_code IS NULL").count())
        weather = frames["weather_daily"]
        locations = spark.table("lakehouse.silver_ref.weather_location")\
            .select("weather_location_code").distinct()
        check("F06.weather_location_ref", weather.select("weather_location_code")\
              .join(locations, "weather_location_code", "left_anti").count())
        check("F06.daily_grain", weather.filter(
            "period_type <> 'DAY' OR observation_date IS NULL").count())
        stock = frames["coffee_stock_observation"]
        check("F07.stock_scenario_and_weight", stock.filter(
            "scenario_code NOT IN ('ACTUAL','FORECAST') OR "
            "bag_weight_kg <> 60 OR period_type <> 'CROP_YEAR'").count())
        macro = frames["macro_indicator_observation"]
        indicators = spark.table("lakehouse.silver_ref.macro_indicator")\
            .select("indicator_code").distinct()
        check("F08.indicator_ref", macro.select("indicator_code")\
              .join(indicators, "indicator_code", "left_anti").count())
        check("F08.missing_retained", abs(138 - macro.filter(
            "value_status='MISSING' AND indicator_value_native IS NULL").count()))
        check("canonical.one_winner", decisions.filter("decision_status='WINNER'")
              .groupBy("business_process", "canonical_business_key")
              .count().filter("count>1").count())
        failures = sum(item["failed"] for item in checks)
        print(json.dumps({"status": "PASS" if failures == 0 else "FAIL",
                          "policy_version": policy_version, "checks": checks}), flush=True)
        return 1 if failures else 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
