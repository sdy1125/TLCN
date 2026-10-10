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


def recoverable_snapshot(spark, namespace, metadata, version, target, frame, rows):
    """Return the current snapshot when a prior MERGE is already fully visible.

    This repairs the narrow crash window between an atomic domain-table MERGE and
    the processing-state SUCCESS write. It deliberately requires an exact key set,
    checksum, transform version and row count before reusing the published scope.
    """

    if rows <= 0:
        return None
    scope = spark.table(f"{namespace}.{target}").filter(
        F.col("dataset_id") == metadata["dataset_id"]
    )
    if scope.count() != rows:
        return None
    if (
        scope.filter(
            (F.col("bronze_checksum_sha256") != metadata["checksum_sha256"])
            | (F.col("transform_version") != version)
        )
        .limit(1)
        .count()
    ):
        return None
    current_keys = scope.select("source_record_id")
    expected_keys = frame.select("source_record_id")
    if (
        current_keys.groupBy("source_record_id")
        .count()
        .filter("count != 1")
        .limit(1)
        .count()
        or expected_keys.join(current_keys, "source_record_id", "left_anti")
        .limit(1)
        .count()
        or current_keys.join(expected_keys, "source_record_id", "left_anti")
        .limit(1)
        .count()
    ):
        return None
    return spark.sql(
        f"SELECT snapshot_id FROM {namespace}.{target}.history "
        "ORDER BY made_current_at DESC LIMIT 1"
    ).first()[0]
