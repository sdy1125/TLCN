"""Source-local canonical audit with deterministic duplicate and conflict rules."""

from datetime import datetime

from pyspark.sql import Window, functions as F

from .domains import DOMAIN


GRAIN = {
    "production_observation": ["m49_code", "faostat_item_code", "calendar_year", "element_code", "metric_code"],
    "provincial_agri_observation": ["geography_code", "geography_version", "geography_level", "province_code", "district_code", "commodity_code", "calendar_year", "metric_code", "coverage_code", "coverage_end_date", "record_status_code"],
    "price_observation": ["series_code", "commodity_code", "country_m49_code", "geography_code", "period_type", "period_code", "metric_code"],
    "trade_total_observation": ["reporter_m49_code", "classification_system", "classification_version", "product_code", "trade_flow_code", "period_type", "period_code", "metric_code", "statistical_scope_code"],
    "trade_partner_observation": ["reporter_m49_code", "partner_m49_code", "partner2_code", "customs_code", "mode_of_transport_code", "classification_system", "classification_version", "product_code", "trade_flow_code", "period_type", "period_code", "metric_code", "statistical_scope_code"],
    "weather_daily": ["weather_location_code", "observation_date"],
    "coffee_stock_observation": ["country_m49_code", "commodity_code", "crop_year_code", "scenario_code"],
    "macro_indicator_observation": ["country_m49_code", "calendar_year", "indicator_code"],
}


def _value(df, domain):
    if domain == "weather_daily":
        fields = ["temperature_avg_c", "temperature_max_c", "temperature_min_c",
                  "relative_humidity_pct", "wind_speed_m_s", "precipitation_mm",
                  "solar_native_value"]
        return F.to_json(F.struct(*[F.col(name) for name in fields]), {"ignoreNullFields": "false"})
    if domain == "price_observation":
        return F.coalesce(F.col("price_index_value"), F.col("price_amount_native")).cast("string")
    return F.coalesce(F.col("value_canonical"), F.col("value_native")).cast("string")


def decide_domain(spark, domain, policy_version="source-local-v1"):
    if domain not in DOMAIN:
        raise ValueError(domain)
    source = spark.table(f"lakehouse.silver_core.{domain}")
    if policy_version == "source-local-v3":
        policies = spark.table("lakehouse.silver_meta.source_selection_policy").filter(
            (F.col("policy_version") == policy_version) &
            (F.col("business_process") == domain)
        ).select(F.col("candidate_source_code").alias("dataset_id"),
                 "metric_code", "period_type", "is_active")
        policy_count = policies.count()
        if policy_count == 0:
            raise ValueError(f"Missing selection policies for {domain}")
        source = source.join(F.broadcast(policies),
                             ["dataset_id", "metric_code", "period_type"], "left")
    keys = ["dataset_id", "observation_set_id"] + GRAIN[domain]
    key = F.sha2(F.to_json(F.struct(*[F.col(name) for name in keys]),
                            {"ignoreNullFields": "false"}), 256)
    df = source.withColumn("canonical_business_key", key).withColumn(
        "_comparison_value", F.coalesce(_value(source, domain), F.lit("<NULL>"))
    )
    # The source-specific policy never compares or adds different datasets.
    # Unmapped district geography and unresolved Comtrade valuation stay out.
    required = [name for name in GRAIN[domain]
                if name not in {"district_code", "coverage_end_date", "country_m49_code",
                "geography_code", "partner2_code", "customs_code",
                "mode_of_transport_code"}]
    eligible = F.lit(True)
    for name in required:
        eligible = eligible & F.col(name).isNotNull()
    if domain == "weather_daily":
        eligible = eligible & F.col("quality_status").isin("VALID", "WARN")
    else:
        eligible = eligible & (F.col("quality_status") == "VALID")
    if policy_version == "source-local-v3":
        eligible = eligible & (F.col("is_active") == True)
    df = df.withColumn("_eligible", eligible)
    stats = df.filter(F.col("_eligible")).groupBy("canonical_business_key").agg(
        F.countDistinct("_comparison_value").alias("_distinct_values"),
        F.count("*").alias("_eligible_count"),
        F.to_json(F.sort_array(F.collect_set("_comparison_value"))).alias("_conflicting_values_json"),
        F.to_json(F.sort_array(F.collect_set("observation_id"))).alias("_candidate_ids_json"),
    )
    ranked = df.join(stats, "canonical_business_key", "left").withColumn(
        "_rank", F.row_number().over(Window.partitionBy("canonical_business_key")
                                     .orderBy(F.col("_eligible").desc(), "observation_id", "source_row_locator"))
    )
    decision = F.when(~F.col("_eligible"), F.lit("EXCLUDED"))\
        .when(F.col("_distinct_values") > 1, F.lit("EXCLUDED"))\
        .when(F.col("_rank") == 1, F.lit("WINNER"))\
        .otherwise(F.lit("LOSER"))
    now = datetime.utcnow()
    records = ranked.select(
        F.lit(domain).alias("business_process"), F.col("metric_code"),
        "canonical_business_key", F.col("observation_id").alias("candidate_observation_id"),
        "observation_set_id", F.lit("SOURCE_RELEASE_ONLY").alias("policy_code"),
        F.lit(policy_version).alias("policy_version"), F.lit(1).alias("selection_rank"),
        decision.alias("decision_status"),
        F.when(~F.col("_eligible"), "not_eligible")
         .when(F.col("_distinct_values") > 1, "unresolved_same_grain_conflict")
         .when(F.col("_rank") == 1, "unique_or_identical_source_value")
         .otherwise("identical_duplicate").alias("decision_reason"),
        F.lit(now).alias("decided_at"), F.lit(None).cast("long").alias("published_snapshot_id"),
    )
    collisions = ranked.filter(F.col("_eligible") & (F.col("_distinct_values") > 1))\
        .select("canonical_business_key", "_candidate_ids_json", "_conflicting_values_json")\
        .dropDuplicates(["canonical_business_key"])
    conflicts = collisions.select(
        F.sha2(F.concat_ws("|", F.lit(domain), F.lit(policy_version), "canonical_business_key"), 256).alias("conflict_id"),
        F.lit(domain).alias("business_process"), F.lit(None).cast("string").alias("metric_code"),
        "canonical_business_key", F.col("_candidate_ids_json").alias("candidate_ids_json"),
        F.col("_conflicting_values_json").alias("conflicting_values_json"),
        F.lit("CONFLICTING_VALUES").alias("conflict_reason"),
        F.lit("BLOCK").alias("conflict_action"), F.lit("OPEN").alias("resolution_status"),
        F.lit(None).cast("string").alias("resolved_by"),
        F.lit(None).cast("timestamp").alias("resolved_at"),
        F.lit(policy_version).alias("policy_version"),
    )
    existing = spark.table("lakehouse.silver_meta.canonical_decision").filter(
        (F.col("business_process") == domain) & (F.col("policy_version") == policy_version)
    ).count()
    if existing:
        if existing != records.count():
            raise ValueError("Existing canonical audit count differs; use a new policy_version")
        return {"domain": domain, "status": "SKIPPED", "decisions": existing}
    # Both tables are append-only for a policy version. Readers use the policy
    # version pointer; a failed run never activates a new version.
    conflicts.writeTo("lakehouse.silver_meta.canonical_conflict").append()
    records.writeTo("lakehouse.silver_meta.canonical_decision").append()
    return {"domain": domain, "decisions": records.count(),
            "conflicts": conflicts.count(),
            "winners": records.filter("decision_status='WINNER'").count()}
