"""Versioned source-local policy for a coherent set of Bronze releases."""

import hashlib
import json
from pathlib import Path

from .domains import DOMAIN
from .foundation import _upsert


POLICY_FAMILY = "source-local-v7"
SOURCE_CODE_FILES = (
    "silver/canonical.py", "silver/common.py", "silver/domains.py",
    "silver/foundation.py", "silver/identity.py", "silver/pipeline.py",
    "silver/policy.py", "silver/readers.py",
    "silver/parsers/faostat.py", "silver/parsers/provincial.py",
    "silver/config/geography.json", "silver/config/rules.json",
    "silver/config/sources.json", "jobs/silver/refs.py",
    "jobs/silver/reconcile_weather_units.py",
    "silver/dq_contract.py", "jobs/silver/validate.py",
    "jobs/silver/gold_contract_smoke.py",
    "jobs/silver/prepare_publish.py", "jobs/silver/activate.py",
)
REFERENCE_TABLES = (
    "country", "commodity", "geography", "geography_alias", "weather_location",
    "price_series", "trade_product", "trade_product_commodity_bridge",
    "macro_indicator", "period", "unit_conversion", "metric_definition",
)


def current_policy_version(spark):
    from .readers import release_manifest

    sets = spark.table("lakehouse.silver_meta.observation_set").filter(
        "status='PUBLISHED'").select("observation_set_id").collect()
    if not sets:
        raise ValueError("No published observation sets")
    digest = hashlib.sha256()
    root = Path(__file__).parents[1]
    for name in SOURCE_CODE_FILES:
        digest.update(name.encode())
        digest.update((root / name).read_bytes())
    snapshots = {}
    for namespace, names in (("silver_core", DOMAIN),
                             ("silver_ref", REFERENCE_TABLES)):
        for name in names:
            table = f"lakehouse.{namespace}.{name}"
            history = spark.sql(
                f"SELECT snapshot_id FROM {table}.history "
                "ORDER BY made_current_at DESC LIMIT 1").first()
            snapshots[table] = int(history[0]) if history else None
    release = release_manifest()
    payload = {
        "observation_sets": sorted(row[0] for row in sets),
        "snapshots": snapshots,
        "bronze_fingerprint": release["fingerprint"] if release else None,
    }
    digest.update(json.dumps(payload, sort_keys=True).encode())
    return POLICY_FAMILY + "-" + digest.hexdigest()[:20]


def seed(spark):
    policy_version = current_policy_version(spark)
    rows = []
    for domain in DOMAIN:
        table = spark.table(f"lakehouse.silver_core.{domain}")
        for item in table.select("dataset_id", "metric_code", "period_type").distinct().collect():
            dataset, metric, period_type = item
            rows.append(dict(
                policy_code="LATEST_VALID_RELEASE", policy_version=policy_version,
                business_process=domain, metric_code=metric,
                grain_scope="ONE_DATASET_ACROSS_RELEASES", period_type=period_type,
                geography_level="SOURCE_NATIVE", coverage_code="SOURCE_NATIVE",
                candidate_source_code=dataset, source_priority=1,
                allowed_record_status="FINAL,PRELIMINARY,FORECAST",
                allowed_quality_flags=("VALID,WARN_COMTRADE"
                                       if dataset.startswith("un_comtrade_")
                                       else "VALID,WARN_WEATHER"),
                tie_break_rule="LOWEST_OBSERVATION_ID_IF_SAME_VALUE",
                conflict_action="BLOCK", is_active=True,
            ))
    _upsert(spark, "lakehouse.silver_meta.source_selection_policy", rows,
            ["policy_code", "policy_version", "business_process", "metric_code",
             "period_type", "candidate_source_code"])
    return {"policy_version": policy_version, "policies": len(rows),
            "active": sum(row["is_active"] for row in rows)}
