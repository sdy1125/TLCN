"""Publish isolated Gold candidates through one atomic release pointer."""

import json
import re
import time
from datetime import datetime

import requests
from pyspark.sql import functions as F

from .candidate import candidate_namespace, load_manifest, validate_id
from .contracts import BRIDGE, DIMENSIONS, FACTS, ROOT
from .quality import audit
from .scope import CONTRACT, SCOPE_VERSION, classify
from .source import current_snapshot, pinned, winners
from silver.foundation import _upsert


POINTER = "lakehouse.gold_meta.publication_pointer"
RELEASE = "lakehouse.gold_meta.release_registry"
EXCEPTION = "lakehouse.gold_meta.release_exception"
TRINO = "http://trino:8080/v1/statement"
TABLES = {**DIMENSIONS, **BRIDGE, **FACTS}


def _ensure_tables(spark):
    specs = {
        "publication_pointer": (
            "pointer_id string, active_gold_batch_id string, generation bigint, "
            "updated_at timestamp"),
        "release_registry": (
            "gold_batch_id string, manifest_id string, source_namespace string, "
            "scope_version string, input_snapshots_json string, "
            "output_snapshots_json string, row_counts_json string, "
            "status string, published_at timestamp"),
        "release_exception": (
            "gold_batch_id string, exception_code string, business_process string, "
            "affected_count bigint, reason string, evidence_ref string, "
            "approval_status string, approved_by string, approved_at timestamp"),
    }
    for name, fields in specs.items():
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS lakehouse.gold_meta.{name} ({fields}) "
            f"USING iceberg LOCATION '{ROOT}/gold_meta/{name}' "
            "TBLPROPERTIES ('format-version'='2')"
        )


def _trino_sql(statement):
    headers = {
        "X-Trino-User": "gold-publisher",
        "X-Trino-Catalog": "iceberg",
        "X-Trino-Schema": "gold_semantic",
    }
    response = requests.post(TRINO, data=statement.encode(), headers=headers,
                             timeout=30)
    response.raise_for_status()
    payload = response.json()
    for _ in range(1000):
        if payload.get("error"):
            raise RuntimeError("Trino semantic view failed: " +
                               payload["error"]["message"])
        next_uri = payload.get("nextUri")
        if not next_uri:
            return payload
        time.sleep(0.1)
        response = requests.get(next_uri, headers=headers, timeout=30)
        response.raise_for_status()
        payload = response.json()
    raise RuntimeError("Trino semantic view statement did not finish")


def _legacy_release(spark):
    rows = spark.table("lakehouse.gold_meta.publish_batch").filter(
        F.col("status").isin("ACTIVE", "ACTIVE_WITH_EXCEPTIONS")
    ).select("gold_batch_id").collect()
    if len(rows) != 1:
        raise RuntimeError("Expected one legacy active Gold release for migration")
    batch_id = rows[0][0]
    if not re.fullmatch(r"[a-zA-Z0-9_-]+", batch_id):
        raise RuntimeError("Unsafe legacy release ID")
    return batch_id


def _initialize_pointer(spark):
    rows = spark.table(POINTER).filter("pointer_id='gold'").collect()
    if len(rows) > 1:
        raise RuntimeError("Multiple Gold publication pointers")
    if rows:
        legacy_rows = spark.table(RELEASE).filter(
            "source_namespace='LEGACY' AND output_snapshots_json IS NULL").collect()
        for legacy_row in legacy_rows:
            legacy_snapshots = {}
            for table in TABLES:
                kind = ('gold_dim' if table in DIMENSIONS else
                        'gold_bridge' if table in BRIDGE else 'gold_fact')
                name = f'lakehouse.{kind}.{table}'
                legacy_snapshots[name] = current_snapshot(spark, name)
            updated = legacy_row.asDict()
            updated['output_snapshots_json'] = json.dumps(legacy_snapshots,
                                                         sort_keys=True)
            _upsert(spark, RELEASE, [updated], ['gold_batch_id'])
        return rows[0].asDict()
    legacy = _legacy_release(spark)
    legacy_snapshots = {}
    for table in TABLES:
        kind = ('gold_dim' if table in DIMENSIONS else
                'gold_bridge' if table in BRIDGE else 'gold_fact')
        name = f'lakehouse.{kind}.{table}'
        legacy_snapshots[name] = current_snapshot(spark, name)
    _upsert(spark, RELEASE, [dict(
        gold_batch_id=legacy, manifest_id=legacy,
        source_namespace="LEGACY", scope_version="legacy",
        output_snapshots_json=json.dumps(legacy_snapshots, sort_keys=True),
        status="PUBLISHED", published_at=datetime.utcnow())],
        ["gold_batch_id"])
    _upsert(spark, POINTER, [dict(
        pointer_id="gold", active_gold_batch_id=legacy, generation=0,
        updated_at=datetime.utcnow())], ["pointer_id"])
    rows = spark.table(POINTER).filter("pointer_id='gold'").collect()
    if len(rows) != 1:
        raise RuntimeError("Could not initialize Gold publication pointer")
    return rows[0].asDict()


def _source_namespace(release, table):
    if release["source_namespace"] != "LEGACY":
        return release["source_namespace"]
    if table in DIMENSIONS:
        return "gold_dim"
    if table in BRIDGE:
        return "gold_bridge"
    return "gold_fact"


def _refresh_semantic_views(spark):
    releases = spark.table(RELEASE).filter(
        "status IN ('PUBLISHED','PREPARED')").orderBy("gold_batch_id").collect()
    if not releases:
        raise RuntimeError("No Gold release registered")
    _trino_sql("CREATE SCHEMA IF NOT EXISTS iceberg.gold_semantic")
    for table in TABLES:
        branches = []
        for row in releases:
            release = row.asDict()
            batch_id = release["gold_batch_id"]
            if not re.fullmatch(r"[a-zA-Z0-9_-]+", batch_id):
                raise RuntimeError("Unsafe Gold release ID")
            namespace = _source_namespace(release, table)
            if not re.fullmatch(r"[a-zA-Z0-9_]+", namespace):
                raise RuntimeError("Unsafe Gold release namespace")
            snapshot_name = f'lakehouse.{namespace}.{table}'
            snapshots = json.loads(release['output_snapshots_json'] or '{}')
            snapshot_id = snapshots.get(snapshot_name)
            if not isinstance(snapshot_id, int) or snapshot_id <= 0:
                raise RuntimeError(f'Missing pinned release snapshot: {snapshot_name}')
            branches.append(
                f"SELECT t.* FROM iceberg.{namespace}.{table} "
                f"FOR VERSION AS OF {snapshot_id} t "
                "JOIN iceberg.gold_meta.publication_pointer p "
                f"ON p.pointer_id='gold' AND p.active_gold_batch_id='{batch_id}'")
        sql = (f"CREATE OR REPLACE VIEW iceberg.gold_semantic.{table} AS " +
               " UNION ALL ".join(branches))
        _trino_sql(sql)
        _trino_sql(f"SELECT * FROM iceberg.gold_semantic.{table} LIMIT 0")
    keys = ("period_key, geography_key, commodity_key, coverage_code, "
            "record_status_code")
    measures = ("area_planted_ha", "area_harvested_ha",
                "production_tonnes", "yield_kg_ha")
    expressions = [
        f"CASE WHEN count(DISTINCT {measure})=1 THEN max({measure}) "
        f"ELSE NULL END AS {measure}, "
        f"count(DISTINCT {measure})>1 AS {measure}_conflict"
        for measure in measures
    ]
    canonical = (
        "CREATE OR REPLACE VIEW "
        "iceberg.gold_semantic.fact_production_provincial_canonical AS "
        f"SELECT {keys}, count(*) AS source_row_count, " +
        ", ".join(expressions) +
        " FROM iceberg.gold_semantic.fact_production_provincial "
        f"GROUP BY {keys}"
    )
    _trino_sql(canonical)
    _trino_sql("SELECT * FROM iceberg.gold_semantic."
               "fact_production_provincial_canonical LIMIT 0")
    canonical_views = {
        "production_national_canonical": "fact_production_national",
        "production_provincial_canonical":
            "fact_production_provincial_canonical",
        "price_canonical": "fact_price",
        "trade_total_canonical": "fact_trade_total",
        "trade_partner_canonical": "fact_trade_partner",
        "weather_daily_canonical": "fact_weather_daily",
        "coffee_stock_actual": "fact_coffee_stock",
        "macro_indicator_canonical": "fact_macro_indicator",
    }
    for name, source_table in canonical_views.items():
        condition = " WHERE scenario_code='ACTUAL'" if name == (
            "coffee_stock_actual") else ""
        _trino_sql(
            f"CREATE OR REPLACE VIEW iceberg.gold_semantic.{name} AS "
            f"SELECT * FROM iceberg.gold_semantic.{source_table}{condition}")
        _trino_sql(f"SELECT * FROM iceberg.gold_semantic.{name} LIMIT 0")
    kpis = {
        "kpi_production_tonnes_annual": (
            "SELECT p.calendar_year, c.commodity_code, n.m49_code AS country_m49_code, "
            "SUM(f.production_tonnes) AS production_tonnes "
            "FROM iceberg.gold_semantic.fact_production_national f "
            "JOIN iceberg.gold_semantic.dim_period p ON f.period_key=p.period_key "
            "JOIN iceberg.gold_semantic.dim_commodity c ON f.commodity_key=c.commodity_key "
            "JOIN iceberg.gold_semantic.dim_country n ON f.country_key=n.country_key "
            "WHERE p.period_type='YEAR' AND f.production_tonnes IS NOT NULL "
            "GROUP BY 1,2,3"),
        "kpi_export_value_usd_source_total": (
            "SELECT p.calendar_year, c.commodity_code, n.m49_code AS reporter_m49_code, "
            "SUM(f.trade_value_usd) AS trade_value_usd "
            "FROM iceberg.gold_semantic.fact_trade_total f "
            "JOIN iceberg.gold_semantic.dim_period p ON f.period_key=p.period_key "
            "JOIN iceberg.gold_semantic.dim_commodity c ON f.commodity_key=c.commodity_key "
            "JOIN iceberg.gold_semantic.dim_country n ON f.reporter_country_key=n.country_key "
            "WHERE p.period_type='YEAR' AND f.trade_flow_code='EXPORT' "
            "AND f.trade_value_usd IS NOT NULL "
            "GROUP BY 1,2,3"),
        "kpi_coffee_stock_actual_tonnes": (
            "SELECT p.crop_year_label, n.m49_code AS country_m49_code, "
            "SUM(f.ending_stock_tonnes) AS ending_stock_tonnes "
            "FROM iceberg.gold_semantic.fact_coffee_stock f "
            "JOIN iceberg.gold_semantic.dim_period p ON f.period_key=p.period_key "
            "JOIN iceberg.gold_semantic.dim_country n ON f.country_key=n.country_key "
            "WHERE p.period_type='CROP_YEAR' AND f.scenario_code='ACTUAL' "
            "AND f.ending_stock_tonnes IS NOT NULL GROUP BY 1,2"),
        "kpi_price_native_by_series": (
            "SELECT p.period_type, p.period_code, c.commodity_code, "
            "s.series_code, s.native_currency_code, s.native_unit_code, "
            "AVG(f.price_amount_native) AS avg_price_native "
            "FROM iceberg.gold_semantic.fact_price f "
            "JOIN iceberg.gold_semantic.dim_period p ON f.period_key=p.period_key "
            "JOIN iceberg.gold_semantic.dim_commodity c ON f.commodity_key=c.commodity_key "
            "JOIN iceberg.gold_semantic.dim_price_series s ON f.price_series_key=s.price_series_key "
            "WHERE f.price_amount_native IS NOT NULL AND f.price_index_value IS NULL "
            "GROUP BY 1,2,3,4,5,6"),
        "kpi_provincial_coffee_production_source_year": (
            "SELECT p.calendar_year, g.geography_code, g.geography_version, "
            "c.commodity_code, f.production_tonnes "
            "FROM iceberg.gold_semantic.fact_production_provincial_canonical f "
            "JOIN iceberg.gold_semantic.dim_period p ON f.period_key=p.period_key "
            "JOIN iceberg.gold_semantic.dim_geography g ON f.geography_key=g.geography_key "
            "JOIN iceberg.gold_semantic.dim_commodity c ON f.commodity_key=c.commodity_key "
            "WHERE p.period_type='YEAR' AND g.geography_level='PROVINCE' "
            "AND c.commodity_code='COFFEE' "
            "AND f.coverage_code='ALL_PRODUCERS' "
            "AND f.record_status_code='FINAL' "
            "AND f.production_tonnes IS NOT NULL"),
    }
    for name, query in kpis.items():
        _trino_sql(f"CREATE OR REPLACE VIEW iceberg.gold_semantic.{name} AS {query}")
        _trino_sql(f"SELECT * FROM iceberg.gold_semantic.{name} LIMIT 0")


def _exceptions(spark, context, namespace):
    batch, snapshots = context
    result = []
    approved_by = CONTRACT["approved_by"]
    now = datetime.utcnow()

    def add(code, domain, count, reason, evidence):
        if count:
            result.append(dict(
                gold_batch_id=batch["gold_batch_id"], exception_code=code,
                business_process=domain, affected_count=int(count),
                reason=reason, evidence_ref=evidence,
                approval_status="ACCEPTED_WITH_LIMITATION",
                approved_by=approved_by, approved_at=now))

    for domain in CONTRACT["domains"]:
        frame = classify(domain, winners(
            spark, domain, batch["policy_version"], snapshots))
        groups = frame.filter("_scope_status='EXCLUDED_APPROVED'").groupBy(
            "_scope_reason").count().collect()
        for row in groups:
            add(row[0], domain, row[1], row[0],
                "spark/gold/scope_contract.json")
    conflicts = pinned(
        spark, "lakehouse.silver_meta.canonical_conflict", snapshots
    ).filter((F.col("policy_version") == batch["policy_version"]) &
             (F.col("resolution_status") == "OPEN")).count()
    add("OPEN_CANONICAL_CONFLICT", "price_observation", conflicts,
        "Source values conflict; no canonical winner",
        "silver_meta.canonical_conflict")
    aliases = pinned(
        spark, "lakehouse.silver_ref.geography_alias", snapshots
    ).filter("approval_status='PENDING'").count()
    add("PENDING_GEOGRAPHY_ALIAS", "provincial_agri_observation", aliases,
        "Official district identity not verified", "silver_ref.geography_alias")
    districts = pinned(
        spark, "lakehouse.silver_core.provincial_agri_observation", snapshots
    ).filter("geography_level='DISTRICT'").count()
    add("DISTRICT_SOURCE_IDENTITY_ONLY", "provincial_agri_observation",
        districts, "District rows retain source identity pending official crosswalk",
        "silver_core.provincial_agri_observation")
    provincial = spark.table(
        f"lakehouse.{namespace}.fact_production_provincial")
    provincial_grain = [
        "period_key", "geography_key", "commodity_key", "coverage_code",
        "record_status_code"]
    for measure in ("area_planted_ha", "area_harvested_ha",
                    "production_tonnes", "yield_kg_ha"):
        conflicts = provincial.groupBy(*provincial_grain).agg(
            F.countDistinct(measure).alias("variants")
        ).filter("variants > 1").count()
        add("PROVINCIAL_" + measure.upper() + "_CONFLICT",
            "provincial_agri_observation", conflicts,
            "Multiple source values at one provincial business grain; semantic "
            "canonical view returns NULL for this measure",
            "gold_semantic.fact_production_provincial_canonical")
    bridge = pinned(
        spark, "lakehouse.silver_ref.trade_product_commodity_bridge", snapshots
    ).filter("mapping_status='PENDING'").count()
    add("PENDING_PRODUCT_BRIDGE", "trade_partner_observation", bridge,
        "Commodity allocation not verified",
        "silver_ref.trade_product_commodity_bridge")
    weather = pinned(
        spark, "lakehouse.silver_core.weather_daily", snapshots
    ).filter("solar_radiation_mj_m2_day IS NULL").count()
    add("SOLAR_UNIT_UNVERIFIED", "weather_daily", weather,
        "NASA request metadata does not establish canonical solar unit",
        "silver_core.weather_daily")
    comtrade = pinned(
        spark, "lakehouse.silver_core.trade_partner_observation", snapshots
    ).filter("dataset_id LIKE 'un_comtrade_%'").count()
    add("COMTRADE_CANONICAL_INACTIVE", "trade_partner_observation", comtrade,
        "HS scope and valuation rules not approved",
        "silver_meta.source_selection_policy")
    return result


def _flip_pointer(spark, expected, target):
    if not re.fullmatch(r"[a-zA-Z0-9_-]+", target):
        raise ValueError("Unsafe release ID")
    generation = int(expected["generation"])
    spark.sql(
        f"UPDATE {POINTER} SET active_gold_batch_id='{target}', "
        f"generation={generation + 1}, updated_at=current_timestamp() "
        f"WHERE pointer_id='gold' AND generation={generation} "
        f"AND active_gold_batch_id='{expected['active_gold_batch_id']}'"
    )
    rows = spark.table(POINTER).filter("pointer_id='gold'").collect()
    if len(rows) != 1 or rows[0]["active_gold_batch_id"] != target or (
            rows[0]["generation"] != generation + 1):
        raise RuntimeError("Gold publication pointer changed concurrently")


def publish_candidate(spark, manifest_id):
    validate_id(manifest_id)
    _ensure_tables(spark)
    pointer = _initialize_pointer(spark)
    if pointer["active_gold_batch_id"] == manifest_id:
        spark.sql(
            f"UPDATE {RELEASE} SET status='PUBLISHED', "
            "published_at=current_timestamp() "
            f"WHERE gold_batch_id='{manifest_id}' AND status='PREPARED'")
        return {"status": "ALREADY_PUBLISHED", "gold_batch_id": manifest_id}
    context = load_manifest(spark, manifest_id)
    namespace = candidate_namespace(manifest_id)
    _, _, inputs, counts, snapshots, checks = audit(spark, context, namespace)
    failures = [row for row in checks if row["status"] == "FAIL"]
    _upsert(spark, f"lakehouse.{namespace}.dq_result", checks,
            ["gold_batch_id", "table_name", "rule_code"])
    if failures:
        return {"status": "BLOCKED", "gold_batch_id": manifest_id,
                "failed_checks": [(x["table_name"], x["rule_code"],
                                   x["failed_count"]) for x in failures]}
    exceptions = _exceptions(spark, context, namespace)
    _upsert(spark, EXCEPTION, exceptions,
            ["gold_batch_id", "exception_code", "business_process"])
    _upsert(spark, RELEASE, [dict(
        gold_batch_id=manifest_id, manifest_id=manifest_id,
        source_namespace=namespace, scope_version=SCOPE_VERSION,
        input_snapshots_json=json.dumps(inputs, sort_keys=True),
        output_snapshots_json=json.dumps(snapshots, sort_keys=True),
        row_counts_json=json.dumps(counts, sort_keys=True),
        status="PREPARED", published_at=None)],
        ["gold_batch_id"])
    _refresh_semantic_views(spark)
    _flip_pointer(spark, pointer, manifest_id)
    spark.sql(
        f"UPDATE {RELEASE} SET status='PUBLISHED', "
        "published_at=current_timestamp() "
        f"WHERE gold_batch_id='{manifest_id}' AND status='PREPARED'")
    return {"status": "ACTIVE_WITH_EXCEPTIONS" if exceptions else "ACTIVE",
            "gold_batch_id": manifest_id, "checks": len(checks),
            "exception_count": len(exceptions)}


def rollback(spark, target):
    _ensure_tables(spark)
    rows = spark.table(RELEASE).filter(
        (F.col("gold_batch_id") == target) &
        (F.col("status") == "PUBLISHED")).collect()
    if len(rows) != 1:
        raise ValueError("Rollback target is not a published Gold release")
    pointer = _initialize_pointer(spark)
    if pointer["active_gold_batch_id"] == target:
        return {"status": "ALREADY_ACTIVE", "gold_batch_id": target}
    _refresh_semantic_views(spark)
    _flip_pointer(spark, pointer, target)
    return {"status": "ROLLED_BACK", "gold_batch_id": target}
