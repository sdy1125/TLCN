"""Explicit schemas and additive-only compatibility checks."""

from .registry import CONTRACTS, load

COMMON = {
    **dict.fromkeys(
        [
            "dataset_id",
            "source_system",
            "source_file",
            "bronze_path",
            "bronze_ingestion_id",
            "bronze_checksum_sha256",
            "source_record_id",
            "airflow_run_id",
            "transform_version",
        ],
        "string",
    ),
    "processed_at": "timestamp",
    "period_start": "date",
    "period_end": "date",
    "year": "int",
    "month": "int",
    **dict.fromkeys(
        [
            "frequency",
            "season_label",
            "observation_status",
            "measure_code",
            "raw_value",
            "unit_raw",
            "unit_standard",
            "quality_flag",
            "raw_payload_json",
            "dedupe_reason",
        ],
        "string",
    ),
    "value_standard": "decimal(28,8)",
    "conversion_factor": "decimal(28,8)",
    "unit_rule": "string",
    "source_priority": "int",
    "is_preferred": "boolean",
}
REQUIRED = {
    "dataset_id",
    "source_system",
    "source_file",
    "bronze_path",
    "bronze_ingestion_id",
    "bronze_checksum_sha256",
    "source_record_id",
    "processed_at",
    "transform_version",
    "period_start",
    "year",
    "frequency",
    "observation_status",
    "measure_code",
}
OPTIONAL_KEYS = {"district_name", "commodity_code", "geo_code", "geo_type"}


def fields(target):
    result = dict(COMMON)
    result.update(
        dict.fromkeys(CONTRACTS[target]["keys"] + CONTRACTS[target]["fields"], "string")
    )
    result.update(COMMON)
    if target == "weather_daily":
        result.update(
            dict.fromkeys(
                ["latitude", "longitude"]
                + [v[0] for v in load("rules")["weather"].values()],
                "double",
            )
        )
    if target == "trade_partner":
        result.update(
            dict.fromkeys(
                [
                    "is_estimated",
                    "isNetWgtEstimated",
                    "isReported",
                    "isAggregate",
                    "isQtyEstimated",
                    "isAltQtyEstimated",
                    "isGrossWgtEstimated",
                ],
                "boolean",
            )
        )
        result.update(
            dict.fromkeys(
                ["value_source_field", "classification_code", "transport_code"],
                "string",
            )
        )
    if target == "market_indicator":
        result.update({"geo_kind": "string", "indicator_name_raw": "string"})
    return result


def schema(target):
    from pyspark.sql import types as T

    types = {
        "string": T.StringType(),
        "date": T.DateType(),
        "timestamp": T.TimestampType(),
        "int": T.IntegerType(),
        "double": T.DoubleType(),
        "boolean": T.BooleanType(),
        "decimal(28,8)": T.DecimalType(28, 8),
    }
    return T.StructType(
        [
            T.StructField(k, types[v], k not in REQUIRED)
            for k, v in fields(target).items()
        ]
    )


def compatibility(existing, incoming):
    """Reject removals, type changes and tightening; return new optional fields."""
    old = {f.name: f for f in existing}
    new = {f.name: f for f in incoming}
    for name, f in old.items():
        if (
            name not in new
            or f.dataType != new[name].dataType
            or (f.nullable and not new[name].nullable)
        ):
            raise ValueError("Incompatible schema migration: " + name)
    added = [f for name, f in new.items() if name not in old]
    if any(not f.nullable for f in added):
        raise ValueError("New fields must be optional")
    return added
