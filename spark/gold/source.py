"""Resolve a published Silver release and pin every input table snapshot."""

import json

from pyspark.sql import functions as F

from silver.domains import DOMAIN


REFERENCE_TABLES = (
    "country", "commodity", "geography", "geography_alias", "weather_location", "price_series",
    "trade_product", "trade_product_commodity_bridge", "macro_indicator", "period",
)
META_TABLES = ("canonical_decision", "canonical_conflict",
               "source_selection_policy", "observation_set")


def current_snapshot(spark, name):
    row = spark.sql(f"SELECT snapshot_id FROM {name}.history "
                    "ORDER BY made_current_at DESC LIMIT 1").first()
    if row is None:
        raise RuntimeError(f"No snapshot: {name}")
    return int(row[0])


def snapshot_at_activation(spark, name, activated_at):
    """Pin the last snapshot no later than Silver activation."""
    rows = spark.sql(f"SELECT snapshot_id, made_current_at FROM {name}.history "
                     "ORDER BY made_current_at DESC").collect()
    eligible = [int(row[0]) for row in rows if row[1] <= activated_at]
    if not eligible:
        raise RuntimeError(f"No {name} snapshot at Silver activation")
    return eligible[0]


def resolve(spark):
    """Fail closed if the published Silver snapshot set cannot be reproduced."""
    rows = (spark.table("lakehouse.silver_meta.publish_batch")
            .filter(F.col("status").isin("ACTIVE", "ACTIVE_WITH_EXCEPTIONS"))
            .orderBy(F.col("activated_at").desc()).limit(2).collect())
    if len(rows) != 1:
        raise RuntimeError(f"Expected one active Silver batch, found {len(rows)}")
    batch = rows[0].asDict()
    if batch["activated_at"] is None:
        raise RuntimeError("Active Silver batch has no activation timestamp")
    published = json.loads(batch["domain_snapshots_json"])
    if set(published) != set(DOMAIN):
        raise RuntimeError("Silver publish manifest does not cover all eight domains")
    snapshots = {}
    for domain in DOMAIN:
        table = f"lakehouse.silver_core.{domain}"
        snapshot = int(published[domain]["snapshot_id"])
        history = {int(r[0]) for r in spark.sql(
            f"SELECT snapshot_id FROM {table}.history").collect()}
        if snapshot not in history:
            raise RuntimeError(f"Published snapshot missing: {table} {snapshot}")
        snapshots[table] = snapshot
    for name in REFERENCE_TABLES:
        table = f"lakehouse.silver_ref.{name}"
        snapshots[table] = snapshot_at_activation(spark, table, batch["activated_at"])
    for name in META_TABLES:
        table = f"lakehouse.silver_meta.{name}"
        snapshots[table] = snapshot_at_activation(spark, table, batch["activated_at"])
    return batch, snapshots


def pinned(spark, table, snapshots):
    return spark.read.format("iceberg").option(
        "snapshot-id", str(snapshots[table])).load(table)


def winners(spark, domain, policy_version, snapshots):
    source = pinned(spark, f"lakehouse.silver_core.{domain}", snapshots)
    decision = pinned(spark, "lakehouse.silver_meta.canonical_decision", snapshots)
    ids = (decision.filter((F.col("business_process") == domain) &
                           (F.col("policy_version") == policy_version) &
                           (F.col("decision_status") == "WINNER"))
           .select(F.col("candidate_observation_id").alias("observation_id")))
    if ids.groupBy("observation_id").count().filter("count > 1").limit(1).count():
        raise RuntimeError(f"Duplicate canonical decisions for {domain}")
    return source.join(ids, "observation_id", "inner")
