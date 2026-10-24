"""Persistent Silver metadata and reference contracts in Iceberg."""

from datetime import datetime, timezone

from .common import GEO, RULES
from .readers import resolve
from .registry import SOURCES
from .identity import observation_set_id
from .storage import WAREHOUSE_SILVER, ensure_namespace, ensure_table_location


TABLES = {
    "silver_meta": {
        "dataset_registry": "dataset_id string, provider string, dataset_code string, dataset_name string, source_uri string, license_info string, owner string, expected_frequency string, source_schema_version string, is_active boolean, created_at timestamp, updated_at timestamp",
        "observation_set": "observation_set_id string, dataset_id string, source_file_name string, source_uri string, file_sha256 string, file_size_bytes bigint, source_record_count bigint, published_at timestamp, retrieved_at timestamp, first_seen_at timestamp, source_version string, schema_version string, ingestion_run_id string, status string, supersedes_observation_set_id string, created_at timestamp, updated_at timestamp",
        "ingestion_run": "ingestion_run_id string, dag_id string, task_id string, started_at timestamp, finished_at timestamp, code_commit string, environment string, run_status string, rows_read bigint, rows_valid bigint, rows_quarantined bigint, rows_published bigint, error_summary string",
        "source_selection_policy": "policy_code string, policy_version string, business_process string, metric_code string, grain_scope string, period_type string, geography_level string, coverage_code string, candidate_source_code string, source_priority int, allowed_record_status string, allowed_quality_flags string, effective_from date, effective_to date, tie_break_rule string, conflict_action string, is_active boolean, approved_by string, approved_at timestamp",
        "canonical_decision": "business_process string, metric_code string, canonical_business_key string, candidate_observation_id string, observation_set_id string, policy_code string, policy_version string, selection_rank int, decision_status string, decision_reason string, decided_at timestamp, published_snapshot_id bigint",
        "canonical_conflict": "conflict_id string, business_process string, metric_code string, canonical_business_key string, candidate_ids_json string, conflicting_values_json string, conflict_reason string, conflict_action string, resolution_status string, resolved_by string, resolved_at timestamp, policy_version string",
        "dq_result": "dq_result_id string, run_id string, object_name string, rule_code string, severity string, checked_count bigint, failed_count bigint, threshold double, status string, sample_ref string, checked_at timestamp",
        "quarantine_record": "quarantine_id string, run_id string, dataset_id string, observation_set_id string, source_row_locator string, raw_payload_json string, error_code string, error_detail string, severity string, quarantine_at timestamp, resolution_status string, resolution_note string",
        "publish_batch": "batch_id string, namespace string, policy_version string, status string, domain_snapshots_json string, dq_run_id string, created_at timestamp, activated_at timestamp, activation_reason string",
    },
    "silver_ref": {
        "country": "m49_code string, iso2_code string, iso3_code string, country_name string, country_type string, is_aggregate boolean, valid_from date, valid_to date, source_code string",
        "commodity": "commodity_code string, commodity_name string, faostat_item_code string, commodity_group string, is_active boolean",
        "geography": "geography_code string, geography_version string, geography_name string, geography_level string, province_code string, district_code string, parent_code string, valid_from date, valid_to date",
        "geography_alias": "source_dataset string, raw_name string, raw_code string, normalized_name string, geography_code string, confidence double, approval_status string, valid_from date, valid_to date",
        "weather_location": "weather_location_code string, latitude double, longitude double, provider string, geography_code string, valid_from date, valid_to date, mapping_method string",
        "trade_product": "classification_system string, classification_version string, product_code string, product_name string, aggregation_level int, is_leaf boolean",
        "trade_product_commodity_bridge": "classification_system string, classification_version string, product_code string, commodity_code string, mapping_type string, allocation_weight decimal(28,8), mapping_status string, mapping_rule string, valid_from date, valid_to date",
        "unit_conversion": "conversion_code string, from_unit string, to_unit string, context string, multiplier decimal(28,8), formula string, precision_rule string, valid_from date, valid_to date, approved_by string",
        "metric_definition": "metric_code string, business_process string, native_unit string, canonical_unit string, value_type string, aggregation_rule string, min_value decimal(28,8), max_value decimal(28,8), nullable_policy string",
        "price_series": "series_code string, dataset_id string, commodity_code string, price_type string, market_scope string, native_currency string, native_unit string, canonical_unit string, frequency string, index_base string, valuation_basis string, valid_from date, valid_to date",
        "macro_indicator": "indicator_code string, indicator_name string, unit_code string, scale_factor decimal(28,8), aggregation_rule string, value_type string, valid_from date, valid_to date",
        "period": "period_type string, period_code string, start_date date, end_date date, calendar_year int, calendar_month int, crop_year_label string",
    },
}


def create_tables(spark):
    for namespace, tables in TABLES.items():
        ensure_namespace(spark, namespace)
        for table, fields in tables.items():
            spark.sql(
                f"CREATE TABLE IF NOT EXISTS lakehouse.{namespace}.{table} ({fields}) "
                f"USING iceberg LOCATION '{WAREHOUSE_SILVER}/{namespace}/{table}' "
                "TBLPROPERTIES ('format-version'='2')"
            )
            ensure_table_location(spark, namespace, table)
    conflict = "lakehouse.silver_meta.canonical_conflict"
    if "policy_version" not in spark.table(conflict).columns:
        spark.sql(f"ALTER TABLE {conflict} ADD COLUMN policy_version string")


def _upsert(spark, table, rows, keys):
    if not rows:
        return
    columns = [field.name for field in spark.table(table).schema]
    data = spark.createDataFrame(
        [tuple(row.get(name) for name in columns) for row in rows],
        spark.table(table).schema,
    )
    data.createOrReplaceTempView("_silver_seed")
    condition = " AND ".join(f"t.{key} <=> s.{key}" for key in keys)
    changed = " OR ".join(f"NOT (t.{name} <=> s.{name})"
                          for name in columns if name not in keys)
    matched = f"WHEN MATCHED AND ({changed}) THEN UPDATE SET * " if changed else ""
    spark.sql(
        f"MERGE INTO {table} t USING _silver_seed s ON {condition} "
        f"{matched}WHEN NOT MATCHED THEN INSERT *"
    )


def _insert_missing(spark, table, rows, keys):
    if not rows:
        return
    table_schema = spark.table(table).schema
    columns = [field.name for field in table_schema]
    spark.createDataFrame(
        [tuple(row.get(name) for name in columns) for row in rows], table_schema
    ).createOrReplaceTempView("_silver_new_sets")
    condition = " AND ".join(f"t.{key} <=> s.{key}" for key in keys)
    spark.sql(f"MERGE INTO {table} t USING _silver_new_sets s ON {condition} "
              "WHEN NOT MATCHED THEN INSERT *")


def seed_foundation(spark):
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    registry, sets = [], []
    for dataset, spec in SOURCES.items():
        m = resolve(dataset)
        release = m.get("source_modified_time") or m["ingestion_id"]
        set_id = observation_set_id(dataset, release, m["checksum_sha256"])
        retrieved = datetime.fromisoformat(m["ingested_at"]).replace(tzinfo=None)
        registry.append(dict(
            dataset_id=dataset, provider=m["source_system"].upper(),
            dataset_code=dataset, dataset_name=dataset.replace("_", " "),
            source_uri=m.get("source_file_path") or m["bronze_path"],
            expected_frequency=spec["frequency"].upper(),
            source_schema_version=m.get("schema_hash") or "UNKNOWN",
            is_active=True, created_at=retrieved, updated_at=now,
        ))
        sets.append(dict(
            observation_set_id=set_id, dataset_id=dataset,
            source_file_name=m["source_file_name"], source_uri=m["bronze_path"],
            file_sha256=m["checksum_sha256"], file_size_bytes=m["file_size_bytes"],
            source_record_count=m.get("row_count"), retrieved_at=retrieved,
            first_seen_at=retrieved, source_version=release,
            schema_version=m.get("schema_hash"), ingestion_run_id=m["ingestion_id"],
            status="DISCOVERED", created_at=now, updated_at=now,
        ))
    _upsert(spark, "lakehouse.silver_meta.dataset_registry", registry, ["dataset_id"])
    _insert_missing(spark, "lakehouse.silver_meta.observation_set", sets, ["observation_set_id"])
    commodities = [
        dict(commodity_code=code.upper(), commodity_name=code.title(),
             faostat_item_code=item, commodity_group="AGRICULTURE", is_active=True)
        for code, item in [("coffee", "656"), ("rubber", "836"), ("pepper", "687")]
    ]
    _upsert(spark, "lakehouse.silver_ref.commodity", commodities, ["commodity_code"])
    geography = [dict(geography_code=code, geography_version=GEO["version"],
                      geography_name=name, geography_level="PROVINCE", province_code=code)
                 for name, code in GEO["provinces"].items()]
    _upsert(spark, "lakehouse.silver_ref.geography", geography,
            ["geography_code", "geography_version"])
    aliases = [dict(source_dataset="*", raw_name=raw, normalized_name=normalized,
                    geography_code=GEO["provinces"][normalized], confidence=1.0,
                    approval_status="REVIEWED")
               for raw, normalized in GEO["aliases"].items()]
    _upsert(spark, "lakehouse.silver_ref.geography_alias", aliases,
            ["source_dataset", "raw_name"])
    locations = [dict(weather_location_code=point, latitude=float(v["latitude"]),
                      longitude=float(v["longitude"]), provider="NASA_POWER",
                      geography_code=v["province_code_63"].zfill(2),
                      mapping_method="SOURCE_POINT_TO_PROVINCE")
                 for point, v in GEO["weather_points"].items()]
    _upsert(spark, "lakehouse.silver_ref.weather_location", locations,
            ["weather_location_code"])
    units = []
    from decimal import Decimal
    for raw, (canonical, factor) in RULES["units"].items():
        if raw in {"LCU/tonne", "SLC/tonne"}:
            continue
        units.append(dict(conversion_code=raw, from_unit=raw, to_unit=canonical,
                          context="SOURCE_DECLARED", multiplier=Decimal(factor),
                          formula="value * multiplier", precision_rule="DECIMAL_28_8",
                          approved_by="SOURCE_UNIT"))
    _upsert(spark, "lakehouse.silver_ref.unit_conversion", units,
            ["conversion_code"])
    macros = [dict(indicator_code=code, indicator_name=code, unit_code=unit,
                   scale_factor=Decimal(1), aggregation_rule="NO_SUM",
                   value_type="PERCENT" if unit == "%" else "NUMBER")
              for code, (unit, _) in RULES["wdi"].items()]
    _upsert(spark, "lakehouse.silver_ref.macro_indicator", macros,
            ["indicator_code"])
    return {"datasets": len(registry), "observation_sets": len(sets),
            "geographies": len(geography), "weather_locations": len(locations)}
