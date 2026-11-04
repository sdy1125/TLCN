"""Immutable Gold run manifests and isolated per-batch candidate storage."""

import hashlib
import json
import re
from datetime import datetime

from .contracts import BRIDGE, DIMENSIONS, FACTS, META, ROOT, VERSION
from .scope import SCOPE_VERSION, validate_contract


MANIFEST_TABLE = "lakehouse.gold_meta.run_manifest"
MANIFEST_SCHEMA = (
    "manifest_id string, silver_batch_id string, policy_version string, "
    "transform_version string, scope_version string, input_snapshots_json string, "
    "manifest_sha256 string, created_at timestamp"
)
CANDIDATE_META = {
    "fact_lineage": META["fact_lineage"],
    "dq_result": META["dq_result"],
}


def _hash(payload):
    return hashlib.sha256(json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode()).hexdigest()


def validate_id(manifest_id):
    if not re.fullmatch(r"gold-[a-z0-9][a-z0-9_-]{2,95}", manifest_id):
        raise ValueError("Invalid Gold manifest ID")
    return manifest_id


def candidate_namespace(manifest_id):
    validate_id(manifest_id)
    return "gold_candidate_" + hashlib.sha256(manifest_id.encode()).hexdigest()[:20]


def _ensure_manifest_table(spark):
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {MANIFEST_TABLE} ({MANIFEST_SCHEMA}) "
        f"USING iceberg LOCATION '{ROOT}/gold_meta/run_manifest' "
        "TBLPROPERTIES ('format-version'='2')"
    )


def create_manifest(spark, manifest_id):
    """Capture Silver's ACTIVE pointer once. Retries reuse identical evidence."""
    from pyspark.sql import functions as F

    from .source import resolve
    validate_id(manifest_id)
    validate_contract()
    _ensure_manifest_table(spark)
    old = spark.table(MANIFEST_TABLE).filter(F.col("manifest_id") == manifest_id).collect()
    if old:
        return load_manifest(spark, manifest_id)
    silver_batch, snapshots = resolve(spark)
    payload = {
        "manifest_id": manifest_id,
        "silver_batch_id": silver_batch["batch_id"],
        "policy_version": silver_batch["policy_version"],
        "transform_version": VERSION,
        "scope_version": SCOPE_VERSION,
        "input_snapshots_json": json.dumps(snapshots, sort_keys=True),
    }
    row = {**payload, "manifest_sha256": _hash(payload),
           "created_at": datetime.utcnow()}
    frame = spark.createDataFrame([row], spark.table(MANIFEST_TABLE).schema)
    frame.writeTo(MANIFEST_TABLE).append()
    return load_manifest(spark, manifest_id)


def load_manifest(spark, manifest_id):
    from pyspark.sql import functions as F

    validate_id(manifest_id)
    rows = spark.table(MANIFEST_TABLE).filter(
        F.col("manifest_id") == manifest_id).collect()
    if len(rows) != 1:
        raise RuntimeError("Expected exactly one immutable Gold run manifest")
    row = rows[0].asDict()
    payload = {name: row[name] for name in (
        "manifest_id", "silver_batch_id", "policy_version", "transform_version",
        "scope_version", "input_snapshots_json")}
    if _hash(payload) != row["manifest_sha256"]:
        raise RuntimeError("Gold run manifest checksum mismatch")
    if row["scope_version"] != SCOPE_VERSION or row["transform_version"] != VERSION:
        raise RuntimeError("Gold code or scope changed after run manifest creation")
    snapshots = json.loads(row["input_snapshots_json"])
    batch = {"batch_id": row["silver_batch_id"],
             "policy_version": row["policy_version"],
             "gold_batch_id": manifest_id}
    return batch, snapshots


def ensure_candidate(spark, manifest_id):
    """Create tables only under this batch's private Iceberg namespace."""
    namespace = candidate_namespace(manifest_id)
    location = f"{ROOT}/candidates/{manifest_id}"
    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS lakehouse.{namespace} "
        f"LOCATION '{location}'"
    )
    details = spark.sql(
        f"DESCRIBE NAMESPACE EXTENDED lakehouse.{namespace}").collect()
    actual = [str(row[1]).rstrip("/") for row in details
              if str(row[0]).strip().lower() == "location"]
    if actual != [location]:
        raise RuntimeError("Gold candidate namespace location mismatch")
    for name, fields in {**DIMENSIONS, **BRIDGE, **FACTS, **CANDIDATE_META}.items():
        target = f"lakehouse.{namespace}.{name}"
        path = f"{location}/{name}"
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {target} ({fields}) USING iceberg "
            f"LOCATION '{path}' TBLPROPERTIES ('format-version'='2')"
        )
        details = spark.sql(f"DESCRIBE TABLE EXTENDED {target}").collect()
        actual = [str(row[1]).rstrip("/") for row in details
                  if str(row[0]).strip().lower() == "location"]
        if actual != [path]:
            raise RuntimeError(f"Gold candidate table location mismatch: {name}")
    return namespace
