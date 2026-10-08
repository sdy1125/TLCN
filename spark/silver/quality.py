"""Output audit and deterministic within-source deduplication."""

from pyspark.sql import functions as F, Window
from .common import RULES
from .contracts import REQUIRED


def _resolve_configured_conflicts(good, conflict, dataset):
    """Resolve only reviewed source-specific conflicts and retain losing rows."""

    policy = RULES.get("conflict_resolution", {}).get(dataset)
    empty_keys = conflict.limit(0)
    empty_losers = good.limit(0).select(
        "source_record_id", "_payload_hash", "raw_payload_json"
    )
    if not policy:
        return empty_keys, empty_losers, good.limit(0), conflict

    if policy["strategy"] != "maximum_value_within_tolerance":
        raise ValueError(f"Unsupported conflict resolution strategy for {dataset}")

    conflicting = good.join(conflict, "source_record_id", "inner")
    resolved_keys = (
        conflicting.groupBy("source_record_id")
        .agg(
            F.countDistinct("_payload_hash").alias("variants"),
            F.min(
                (
                    (F.col("measure_code") == policy["measure_code"])
                    & (F.col("unit_standard") == policy["unit_standard"])
                ).cast("int")
            ).alias("all_variants_eligible"),
            (F.max("value_standard") - F.min("value_standard")).alias("value_delta"),
        )
        .filter(
            (F.col("variants") > 1)
            & (F.col("all_variants_eligible") == 1)
            & (F.col("value_delta") <= F.lit(policy["max_absolute_delta"]))
        )
        .select("source_record_id")
    )
    ranked = conflicting.join(resolved_keys, "source_record_id", "inner").withColumn(
        "_conflict_rank",
        F.row_number().over(
            Window.partitionBy("source_record_id").orderBy(
                F.col("value_standard").desc_nulls_last(),
                F.col("_payload_hash"),
                F.sha2("raw_payload_json", 256),
            )
        ),
    )
    winners = (
        ranked.filter("_conflict_rank = 1")
        .drop("_conflict_rank")
        .withColumn("_dedupe_reason", F.lit(policy["winner_reason"]))
        .withColumn(
            "quality_flag",
            F.concat_ws(
                ",",
                F.col("quality_flag"),
                F.lit("resolved_source_conflict:" + policy["version"]),
            ),
        )
    )
    winner_hashes = winners.select("source_record_id", "_payload_hash")
    losers = (
        ranked.join(winner_hashes, ["source_record_id", "_payload_hash"], "left_anti")
        .select("source_record_id", "_payload_hash", "raw_payload_json")
        .dropDuplicates()
    )
    unresolved = conflict.join(resolved_keys, "source_record_id", "left_anti")
    return resolved_keys, losers, winners, unresolved


def audit(transformed, target, dataset=None):
    invalid = transformed.filter(F.col("error_code").isNotNull())
    clean = transformed.filter(F.col("error_code").isNull())
    good = clean.select("record.*")
    # Original payload may contain sibling measures from an unpivoted row.
    # Compare this observation's fields; retain raw payload for replay.
    comparable = [
        c
        for c in good.columns
        if c
        not in {"processed_at", "airflow_run_id", "dedupe_reason", "raw_payload_json"}
    ]
    good = good.withColumn(
        "_payload_hash",
        F.sha2(F.to_json(F.struct(*[F.col(c) for c in comparable])), 256),
    )
    conflict = (
        good.groupBy("source_record_id")
        .agg(F.countDistinct("_payload_hash").alias("variants"))
        .filter("variants > 1")
        .select("source_record_id")
    )
    conflict_count = conflict.count()
    resolved_keys, resolved_losers, resolved_winners, unresolved = (
        _resolve_configured_conflicts(good, conflict, dataset)
    )
    resolved_count = resolved_keys.count()
    unresolved_count = unresolved.count()
    unresolved_rows = (
        clean.join(
            unresolved,
            F.col("record.source_record_id") == F.col("source_record_id"),
            "left_semi",
        )
        .withColumn("error_code", F.lit("CONFLICTING_DUPLICATE"))
        .withColumn(
            "error_message", F.lit("Same business key has conflicting observations")
        )
    )
    clean_hashed = clean.withColumn(
        "source_record_id", F.col("record.source_record_id")
    ).withColumn(
        "_payload_hash",
        F.sha2(
            F.to_json(
                F.struct(*[F.col(f"record.{c}").alias(c) for c in comparable])
            ),
            256,
        ),
    )
    resolved_loser_rows = (
        clean_hashed.join(
            resolved_losers,
            ["source_record_id", "_payload_hash", "raw_payload_json"],
            "inner",
        )
        .drop("_payload_hash", "source_record_id")
        .withColumn("error_code", F.lit("RESOLVED_CONFLICTING_DUPLICATE"))
        .withColumn(
            "error_message",
            F.lit("Configured deterministic winner retained; this source row lost"),
        )
    )
    invalid = invalid.unionByName(unresolved_rows).unionByName(resolved_loser_rows)
    good = good.join(conflict, "source_record_id", "left_anti")
    good = good.withColumn("_dedupe_reason", F.lit(RULES["duplicate_policy"])).unionByName(
        resolved_winners, allowMissingColumns=True
    )
    ranked = good.withColumn(
        "_rank",
        F.row_number().over(
            Window.partitionBy("source_record_id").orderBy(
                "_payload_hash", F.sha2("raw_payload_json", 256)
            )
        ),
    )
    valid = (
        ranked.filter("_rank = 1")
        .drop("_rank", "_payload_hash")
        .withColumn("dedupe_reason", F.col("_dedupe_reason"))
        .drop("_dedupe_reason")
    )
    candidates = transformed.count()
    rejected = invalid.count()
    warning_rejected = invalid.filter(
        F.col("error_code") == "RESOLVED_CONFLICTING_DUPLICATE"
    ).count()
    failed_rejected = rejected - warning_rejected
    output = valid.count()
    duplicates = good.count() - output
    nulls = valid.filter(F.col("observation_status") == "missing").count()
    null_key = valid.filter(
        F.greatest(*[F.col(c).isNull().cast("int") for c in REQUIRED]) == 1
    ).count()
    metrics = {
        "candidate_rows": candidates,
        "output_rows": output,
        "quarantined_rows": rejected,
        "duplicates_removed": duplicates,
        "explicit_missing_rows": nulls,
        "conflicting_keys": conflict_count,
        "resolved_conflicting_keys": resolved_count,
        "unresolved_conflicting_keys": unresolved_count,
        "warning_quarantined_rows": warning_rejected,
        "failed_quarantined_rows": failed_rejected,
        "null_required_rows": null_key,
    }
    # Per-observation conversion is checked before aggregation to prevent
    # opposite-signed mistakes cancelling out in a sum.
    conversion_bad = 0
    if target != "weather_daily":
        conversion_bad = valid.filter(
            F.col("value_standard").isNotNull()
            & (
                F.abs(
                    F.col("value_standard")
                    - F.col("raw_value").cast("decimal(28,8)")
                    * F.col("conversion_factor")
                )
                > F.lit(0.000001)
            )
        ).count()
    metrics["conversion_mismatch_rows"] = conversion_bad
    metrics["accounting_ok"] = candidates == output + rejected + duplicates
    metrics["quarantine_ratio"] = failed_rejected / max(1, candidates)
    metrics["null_ratio"] = nulls / max(1, output)
    passed = (
        metrics["accounting_ok"]
        and not unresolved_count
        and not null_key
        and not conversion_bad
        and metrics["quarantine_ratio"] <= RULES["quarantine_ratio"]
    )
    return valid, invalid, metrics, passed
