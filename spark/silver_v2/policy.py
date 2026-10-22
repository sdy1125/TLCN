"""Versioned source-local policy for a coherent set of Bronze releases."""

import hashlib

from .domains import DOMAIN
from .foundation import _upsert


POLICY_FAMILY = "source-local-v5"


def current_policy_version(spark):
    sets = spark.table("lakehouse.silver_meta.observation_set").filter(
        "status='PUBLISHED'").select("observation_set_id").collect()
    if not sets:
        raise ValueError("No published observation sets")
    payload = "|".join(sorted(row[0] for row in sets))
    return POLICY_FAMILY + "-" + hashlib.sha256(payload.encode()).hexdigest()[:16]


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
                allowed_quality_flags="VALID,WARN_WEATHER",
                tie_break_rule="LOWEST_OBSERVATION_ID_IF_SAME_VALUE",
                conflict_action="BLOCK", is_active=not dataset.startswith("un_comtrade_"),
            ))
    _upsert(spark, "lakehouse.silver_meta.source_selection_policy", rows,
            ["policy_code", "policy_version", "business_process", "metric_code",
             "period_type", "candidate_source_code"])
    return {"policy_version": policy_version, "policies": len(rows),
            "active": sum(row["is_active"] for row in rows)}
