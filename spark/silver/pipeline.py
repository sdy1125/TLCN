"""Audit -> transform -> audit -> publish, with independently recoverable state."""

import json
import hashlib
from datetime import datetime
from pyspark.sql import functions as F, types as T
from pyspark import StorageLevel
from . import readers, writer, state
from .contracts import schema
from .common import RULES
from .quality import audit
from .registry import SOURCES, transform_version
from .transforms import rows
from .transforms.weather_daily import transform as weather


def run_dataset(spark, dataset, run_id, namespace="lakehouse.silver", metadata=None):
    spec = dict(SOURCES[dataset])
    target = spec["target"]
    version = transform_version()
    now = datetime.utcnow()
    writer.initialize(spark, namespace)
    try:
        m = metadata or readers.resolve(dataset)
    except Exception as exc:
        dq = (
            run_id,
            dataset,
            "unresolved",
            target,
            "bronze_discovery",
            "dataset",
            "FAIL",
            "FAIL",
            None,
            "valid latest_success",
            None,
            json.dumps({"error_type": type(exc).__name__}),
            now,
        )
        writer.upsert_control(
            spark,
            namespace,
            "dq_results",
            spark.createDataFrame([dq], writer.CONTROL["dq_results"]),
            ["run_id", "dataset_id", "check_name"],
        )
        raise
    if state.can_skip(spark, namespace, m, version, target):
        return {"dataset_id": dataset, "status": "SKIPPED", "target": target}
    metrics = {}
    snapshot = None
    status = "FAILED"
    transformed = None
    valid = None
    try:
        with readers.download(m) as path:
            frame, counts = readers.read(spark, path, spec)
            metrics.update(counts)
            if counts["input_rows"] == 0:
                raise ValueError("Empty input dataset")
            metrics["schema_fingerprint"] = hashlib.sha256(
                json.dumps(spec["headers"]).encode()
            ).hexdigest()
            metrics["metadata_frequency_mismatch"] = spec["metadata_warning"]
            if (
                m.get("row_count") is not None
                and m["row_count"] != counts["input_rows"]
            ):
                raise ValueError(
                    "Input row count differs from Bronze technical metadata"
                )
            if target == "weather_daily":
                summary = frame.agg(
                    *[
                        F.sum(
                            F.when(F.col(c).cast("double") == -999, 1).otherwise(0)
                        ).alias(c)
                        for c in RULES["weather"]
                    ]
                ).first()
                metrics["sentinel_counts"] = summary.asDict()
            if target == "weather_daily":
                transformed = weather(frame, m, version, run_id, now)
            else:
                envelope = T.StructType(
                    [
                        T.StructField("record", schema(target)),
                        T.StructField("error_code", T.StringType()),
                        T.StructField("error_message", T.StringType()),
                        T.StructField("raw_payload_json", T.StringType()),
                        T.StructField("raw_id", T.StringType()),
                    ]
                )
                transformed = spark.createDataFrame(
                    frame.rdd.mapPartitions(
                        lambda it: rows(it, spec, m, version, run_id, now)
                    ),
                    envelope,
                )
            transformed = transformed.persist(StorageLevel.DISK_ONLY)
            valid, invalid, observed, passed = audit(transformed, target, dataset)
            metrics.update(observed)
            valid = valid.persist(StorageLevel.DISK_ONLY)
            factor = (
                4
                if dataset == "world_bank_pink_sheet"
                else (
                    2
                    if dataset
                    in {
                        "un_comtrade_annual",
                        "un_comtrade_monthly",
                        "provincial_coffee_average_price",
                        "provincial_coffee_export_volume_value",
                    }
                    else 1
                )
            )
            source_errors = transformed.filter(F.col("error_code").isNotNull()).count()
            source_rows = counts["input_rows"] - counts["excluded_rows"]
            expected_candidates = (source_rows - source_errors) * factor + source_errors
            metrics["source_accounting_ok"] = (
                expected_candidates == metrics["candidate_rows"]
            )
            metrics["expansion_factor"] = factor
            bounds = valid.agg(
                F.min("period_start").alias("min"), F.max("period_start").alias("max")
            ).first()
            metrics["period_min"] = str(bounds["min"])
            metrics["period_max"] = str(bounds["max"])
            passed = passed and metrics["source_accounting_ok"]
            quarantined = invalid.select(
                F.lit(dataset).alias("dataset_id"),
                F.lit(m["ingestion_id"]).alias("bronze_ingestion_id"),
                F.lit(m["checksum_sha256"]).alias("bronze_checksum_sha256"),
                F.col("raw_id").alias("source_record_id"),
                F.lit(target).alias("target_contract"),
                "error_code",
                "error_message",
                F.when(
                    F.col("error_code") == "RESOLVED_CONFLICTING_DUPLICATE",
                    F.lit("WARNING"),
                )
                .otherwise(F.lit("FAIL"))
                .alias("error_severity"),
                "raw_payload_json",
                F.lit(now).alias("detected_at"),
                F.lit(run_id).alias("airflow_run_id"),
                F.lit(version).alias("transform_version"),
            ).dropDuplicates(
                [
                    "dataset_id",
                    "bronze_ingestion_id",
                    "source_record_id",
                    "error_code",
                    "transform_version",
                ]
            )
            writer.upsert_control(
                spark,
                namespace,
                "quarantine_records",
                quarantined,
                [
                    "dataset_id",
                    "bronze_ingestion_id",
                    "source_record_id",
                    "error_code",
                    "transform_version",
                ],
            )
            if not passed:
                raise ValueError("Output contract audit failed; see dq_results")
            snapshot = writer.publish(spark, namespace, target, dataset, valid)
            # Read-after-write before writing SUCCESS state.
            actual = (
                spark.table(f"{namespace}.{target}")
                .filter(F.col("dataset_id") == dataset)
                .count()
            )
            if actual != metrics["output_rows"]:
                raise RuntimeError("Read-after-write row count mismatch")
            status = "SUCCESS"
    except Exception as exc:
        metrics["error_type"] = type(exc).__name__
        # Avoid retaining infrastructure exception strings that may contain credentials.
        raise
    finally:
        if transformed is not None:
            transformed.unpersist()
        if valid is not None:
            valid.unpersist()
        warning = (
            spec["metadata_warning"]
            or metrics.get("duplicates_removed", 0) > 0
            or metrics.get("resolved_conflicting_keys", 0) > 0
            or metrics.get("null_ratio", 0) > RULES["null_warning_ratio"]
        )
        details = json.dumps(metrics, sort_keys=True)
        dq = (
            run_id,
            dataset,
            m["ingestion_id"],
            target,
            "audit_transform_publish",
            "dataset",
            "FAIL" if status == "FAILED" else "WARNING" if warning else "INFO",
            "FAIL" if status == "FAILED" else "WARNING" if warning else "PASS",
            str(metrics.get("quarantine_ratio", "")),
            str(RULES["quarantine_ratio"]),
            float(RULES["quarantine_ratio"]),
            details,
            now,
        )
        writer.upsert_control(
            spark,
            namespace,
            "dq_results",
            spark.createDataFrame([dq], writer.CONTROL["dq_results"]),
            ["run_id", "dataset_id", "check_name"],
        )
        record = (
            state.state_key(dataset, m["checksum_sha256"], version),
            dataset,
            m["checksum_sha256"],
            m["ingestion_id"],
            version,
            run_id,
            status,
            target,
            snapshot,
            metrics.get("output_rows", 0),
            details,
            now,
        )
        writer.upsert_control(
            spark,
            namespace,
            "processing_state",
            spark.createDataFrame([record], writer.CONTROL["processing_state"]),
            ["state_key", "run_id"],
        )
    return {
        "dataset_id": dataset,
        "status": (
            "QUARANTINED"
            if metrics.get("quarantined_rows")
            else "WARNING" if warning else status
        ),
        "target": target,
        **metrics,
    }
