"""Skip only the latest successful dataset state with a still-active snapshot."""

from pyspark.sql import functions as F
from .common import stable_id


def state_key(dataset, checksum, version):
    return stable_id(dataset, [checksum, version])


def can_skip(spark, namespace, metadata, version, target):
    states = (
        spark.table(f"{namespace}.processing_state")
        .filter(
            (F.col("dataset_id") == metadata["dataset_id"])
            & (F.col("status") == "SUCCESS")
        )
        .orderBy(F.col("checked_at").desc())
        .take(1)
    )
    if not states:
        return False
    state = states[0]
    if state.state_key != state_key(
        metadata["dataset_id"], metadata["checksum_sha256"], version
    ):
        return False
    active = (
        spark.table(f"{namespace}.{target}.history")
        .filter(
            (F.col("snapshot_id") == state.snapshot_id) & F.col("is_current_ancestor")
        )
        .count()
    )
    if not active:
        return False
    scope = spark.table(f"{namespace}.{target}").filter(
        F.col("dataset_id") == metadata["dataset_id"]
    )
    invalid = (
        scope.filter(
            (F.col("bronze_checksum_sha256") != metadata["checksum_sha256"])
            | (F.col("transform_version") != version)
        )
        .limit(1)
        .count()
    )
    return not invalid and scope.count() == state.output_rows
