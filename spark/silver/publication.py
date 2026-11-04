"""Pinned Silver release evidence and one-row publication pointer."""

import hashlib
import json
import re
from datetime import datetime

from pyspark.sql import functions as F

from .domains import DOMAIN
from .foundation import _upsert
from .policy import REFERENCE_TABLES


POINTER = "lakehouse.silver_meta.publication_pointer"
BATCH = "lakehouse.silver_meta.publish_batch"
META_INPUTS = (
    "observation_set", "source_selection_policy", "canonical_decision",
    "canonical_conflict", "dq_result", "release_exception",
)


def validate_batch_id(batch_id):
    if not re.fullmatch(r"silver-[a-z0-9][a-z0-9_-]{2,95}", batch_id):
        raise ValueError("Invalid Silver batch ID")
    return batch_id


def snapshot(spark, table, required=True):
    row = spark.sql(f"SELECT snapshot_id FROM {table}.history "
                    "ORDER BY made_current_at DESC LIMIT 1").first()
    if row is None:
        if required:
            raise ValueError(f"Missing Iceberg snapshot: {table}")
        return None
    return int(row[0])


def capture(spark):
    snapshots = {}
    for namespace, names in (("silver_core", DOMAIN),
                             ("silver_ref", REFERENCE_TABLES),
                             ("silver_meta", META_INPUTS)):
        for name in names:
            table = f"lakehouse.{namespace}.{name}"
            snapshots[table] = snapshot(
                spark, table, required=name != "release_exception")
    return snapshots


def manifest_hash(payload):
    return hashlib.sha256(json.dumps(
        payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def make_manifest(batch_id, policy_version, dq_run_id, snapshots,
                  domain_rows, warnings, bronze_release):
    payload = {
        "batch_id": batch_id, "policy_version": policy_version,
        "dq_run_id": dq_run_id, "input_snapshots": snapshots,
        "domain_rows": domain_rows, "warnings": warnings,
        "bronze_release_id": (bronze_release or {}).get("release_id"),
        "bronze_fingerprint": (bronze_release or {}).get("fingerprint"),
    }
    return {**payload, "manifest_sha256": manifest_hash(payload)}


def verify_manifest(manifest):
    payload = {key: value for key, value in manifest.items()
               if key != "manifest_sha256"}
    if manifest_hash(payload) != manifest.get("manifest_sha256"):
        raise ValueError("Silver release manifest checksum mismatch")
    if set(manifest["input_snapshots"]) != {
            *(f"lakehouse.silver_core.{name}" for name in DOMAIN),
            *(f"lakehouse.silver_ref.{name}" for name in REFERENCE_TABLES),
            *(f"lakehouse.silver_meta.{name}" for name in META_INPUTS)}:
        raise ValueError("Silver release manifest omits input tables")
    return manifest


def load_batch(spark, batch_id):
    validate_batch_id(batch_id)
    rows = spark.table(BATCH).filter(F.col("batch_id") == batch_id).collect()
    if len(rows) != 1:
        raise ValueError("Expected one Silver batch")
    batch = rows[0].asDict()
    if not batch.get("release_manifest_json"):
        raise ValueError("Silver batch has no complete release manifest")
    return batch, verify_manifest(json.loads(batch["release_manifest_json"]))


def current_pointer(spark):
    rows = spark.table(POINTER).filter("pointer_id='silver'").collect()
    if len(rows) > 1:
        raise RuntimeError("Multiple Silver publication pointers")
    if rows:
        return rows[0].asDict()
    legacy = spark.table(BATCH).filter(
        F.col("status").isin("ACTIVE", "ACTIVE_WITH_EXCEPTIONS")
    ).select("batch_id").collect()
    if len(legacy) != 1:
        raise RuntimeError("Cannot initialize Silver pointer without one active batch")
    _upsert(spark, POINTER, [dict(
        pointer_id="silver", active_batch_id=legacy[0][0], generation=0,
        updated_at=datetime.utcnow())], ["pointer_id"])
    return spark.table(POINTER).filter("pointer_id='silver'").first().asDict()


def flip(spark, current, target):
    validate_batch_id(target)
    generation = int(current["generation"])
    old = validate_batch_id(current["active_batch_id"])
    spark.sql(
        f"UPDATE {POINTER} SET active_batch_id='{target}', "
        f"generation={generation + 1}, updated_at=current_timestamp() "
        f"WHERE pointer_id='silver' AND generation={generation} "
        f"AND active_batch_id='{old}'")
    rows = spark.table(POINTER).filter("pointer_id='silver'").collect()
    if (len(rows) != 1 or rows[0]["active_batch_id"] != target or
            rows[0]["generation"] != generation + 1):
        raise RuntimeError("Silver pointer changed concurrently")
    return rows[0].asDict()
