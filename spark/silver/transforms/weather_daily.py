"""Native Spark expressions for all NASA rows; no Python UDF or driver collect."""

from pyspark.sql import functions as F
from ..common import RULES, GEO
from ..contracts import schema


def transform(df, metadata, version, run_id, processed_at):
    payload = F.to_json(F.struct(*[F.col(c) for c in df.columns]))
    raw = df.withColumn("raw_payload_json", payload).withColumn(
        "raw_id", F.sha2(payload, 256)
    )
    y = F.col("YEAR").cast("int")
    doy = F.col("DOY").cast("int")
    start = F.date_add(F.make_date(y, F.lit(1), F.lit(1)), doy - 1)
    invalid = (
        y.isNull()
        | doy.isNull()
        | (doy < 1)
        | (F.year(start) != y)
        | (~F.col("YEAR").rlike(r"^\d{4}$"))
        | (~F.col("DOY").rlike(r"^\d{1,3}$"))
    )
    error = F.when(invalid, F.lit("INVALID_PERIOD"))
    metrics = {}
    missing = []
    for source, (name, low, high) in RULES["weather"].items():
        n = F.col(source).cast("double")
        absent = (
            F.col(source).isNull()
            | F.trim(F.col(source)).isin(RULES["missing"])
            | (n == -999)
        )
        metrics[name] = F.when(~absent, n)
        missing.append(F.when(absent, F.lit(source)))
        bad = (~absent) & (n.isNull() | F.isnan(n) | (n < low) | (n > high))
        error = F.coalesce(error, F.when(bad, F.lit("IMPOSSIBLE_VALUE")))
    points = df.sparkSession.createDataFrame(
        [
            (
                k,
                v["province_code_63"].zfill(2),
                float(v["latitude"]),
                float(v["longitude"]),
            )
            for k, v in GEO["weather_points"].items()
        ],
        "point string, code string, lat double, lon double",
    )
    raw = raw.join(
        F.broadcast(points), F.col("weather_point") == F.col("point"), "left"
    )
    geo_bad = (
        F.col("point").isNull()
        | (F.lpad(F.col("province_code_63"), 2, "0") != F.col("code"))
        | F.col("latitude").cast("double").isNull()
        | F.col("longitude").cast("double").isNull()
        | (F.abs(F.col("latitude").cast("double") - F.col("lat")) > 0.00001)
        | (F.abs(F.col("longitude").cast("double") - F.col("lon")) > 0.00001)
    )
    error = F.coalesce(error, F.when(geo_bad, F.lit("UNKNOWN_GEOGRAPHY")))
    error = F.coalesce(
        error,
        F.when(
            metrics["temperature_min_c"] > metrics["temperature_max_c"],
            F.lit("IMPOSSIBLE_VALUE"),
        ),
    )
    values = {
        **metrics,
        "period_start": start,
        "period_end": start,
        "year": y,
        "month": F.month(start),
        "frequency": F.lit("daily"),
        "observation_status": F.when(
            F.size(F.filter(F.array(*missing), lambda x: x.isNotNull()))
            == len(metrics),
            "missing",
        ).otherwise("official"),
        "quality_flag": F.concat_ws(",", *missing),
        "measure_code": F.lit("weather_daily"),
        "raw_payload_json": F.col("raw_payload_json"),
        "weather_point": F.col("weather_point"),
        "province_code_63": F.lpad(F.col("province_code_63"), 2, "0"),
        "province_name": F.col("province_name"),
        "latitude": F.col("latitude").cast("double"),
        "longitude": F.col("longitude").cast("double"),
        "source_record_id": F.sha2(
            F.to_json(
                F.struct(
                    F.lit(metadata["dataset_id"]).alias("dataset_id"),
                    F.col("weather_point"),
                    start.alias("date"),
                )
            ),
            256,
        ),
    }
    lineage = {
        "dataset_id": metadata["dataset_id"],
        "source_system": metadata["source_system"],
        "source_file": metadata["source_file_name"],
        "bronze_path": metadata["bronze_path"],
        "bronze_ingestion_id": metadata["ingestion_id"],
        "bronze_checksum_sha256": metadata["checksum_sha256"],
        "transform_version": version,
        "airflow_run_id": run_id,
        "processed_at": processed_at,
    }
    values.update({k: F.lit(v) for k, v in lineage.items()})
    record = F.struct(
        *[
            (values.get(f.name, F.lit(None))).cast(f.dataType).alias(f.name)
            for f in schema("weather_daily")
        ]
    )
    return raw.select(
        record.alias("record"),
        error.alias("error_code"),
        F.when(
            error.isNotNull(), F.lit("Invalid NASA period, metric or geography")
        ).alias("error_message"),
        "raw_payload_json",
        "raw_id",
    )
