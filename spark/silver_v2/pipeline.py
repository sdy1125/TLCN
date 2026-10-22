"""Source-isolated Bronze backfill to staged and validated Iceberg observations."""

import json
from datetime import datetime, timezone

from pyspark import StorageLevel
from pyspark.sql import functions as F, types as T

from . import readers
from .common import RowError
from .registry import SOURCES
from .domains import DOMAIN, schema, target, transform
from .foundation import TABLES
from .identity import digest, observation_set_id


def _create_core(spark):
    for namespace in ("silver_stage", "silver_core"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lakehouse.{namespace}")
        for name in DOMAIN:
            columns = ", ".join(
                f"`{field.name}` {field.dataType.simpleString()}" for field in schema(name)
            )
            spark.sql(
                f"CREATE TABLE IF NOT EXISTS lakehouse.{namespace}.{name} ({columns}) "
                "USING iceberg TBLPROPERTIES ('format-version'='2')"
            )


def _map_partition(rows, spec, metadata, run_id, now, columns):
    for source in rows:
        raw = {k: "" if v is None else str(v) for k, v in source.asDict().items()}
        try:
            mapped = transform(raw, spec, metadata, run_id, now)
            if not mapped:
                yield (None, None, None, None, "EXCLUDED", raw["_source_row_locator"])
            for row in mapped:
                yield (tuple(row.get(k) for k in columns), None, None, None,
                       "VALID", raw["_source_row_locator"])
        except (RowError, ValueError, KeyError, OverflowError) as exc:
            code = exc.code if isinstance(exc, RowError) else type(exc).__name__.upper()
            yield (None, code, str(exc)[:500], raw["_raw_payload_json"],
                   "QUARANTINE", raw["_source_row_locator"])


def _upsert_records(spark, table, rows, keys):
    if not rows:
        return
    table_schema = spark.table(table).schema
    cols = [f.name for f in table_schema]
    frame = spark.createDataFrame(
        [tuple(row.get(name) for name in cols) for row in rows], table_schema
    )
    frame.createOrReplaceTempView("_silver_v2_control")
    on = " AND ".join(f"t.{key} <=> s.{key}" for key in keys)
    spark.sql(f"MERGE INTO {table} t USING _silver_v2_control s ON {on} "
              "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")


def run_dataset(spark, dataset, run_id):
    spec = dict(SOURCES[dataset])
    table_name = target(dataset)
    metadata = readers.resolve(dataset)
    release = metadata.get("source_modified_time") or metadata["ingestion_id"]
    set_id = observation_set_id(dataset, release, metadata["checksum_sha256"])
    table_schema = schema(table_name)
    columns = [f.name for f in table_schema]
    core = f"lakehouse.silver_core.{table_name}"
    stage = f"lakehouse.silver_stage.{table_name}"
    _create_core(spark)
    existing = spark.table(core).filter(F.col("observation_set_id") == set_id).count()
    if existing:
        staged_existing = spark.table(stage).filter(
            F.col("observation_set_id") == set_id).count()
        if staged_existing != existing:
            raise ValueError("Published release and staged release row counts differ")
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        spark.sql(f"UPDATE lakehouse.silver_meta.observation_set SET status='PUBLISHED', "
                  f"updated_at=current_timestamp() WHERE observation_set_id='{set_id}'")
        _upsert_records(spark, "lakehouse.silver_meta.ingestion_run", [dict(
            ingestion_run_id=run_id + ":" + dataset, dag_id="silver_v2_backfill",
            task_id=dataset, started_at=now, finished_at=now,
            environment="docker-compose", run_status="SKIPPED",
            rows_read=0, rows_valid=existing, rows_quarantined=0,
            rows_published=existing)], ["ingestion_run_id"])
        return {"dataset_id": dataset, "status": "SKIPPED", "rows": existing,
                "observation_set_id": set_id}
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    _upsert_records(spark, "lakehouse.silver_meta.ingestion_run", [dict(
        ingestion_run_id=run_id + ":" + dataset, dag_id="silver_v2_backfill",
        task_id=dataset, started_at=now, environment="docker-compose",
        run_status="RUNNING")], ["ingestion_run_id"])
    transformed = None
    try:
        with readers.download(metadata) as path:
            raw, counts = readers.read_with_lineage(spark, path, spec)
            expected = metadata.get("row_count")
            if expected is not None and counts["input_rows"] != expected:
                raise ValueError("Bronze source record count mismatch")
            spark.sql("UPDATE lakehouse.silver_meta.observation_set "
                      f"SET source_record_count={counts['input_rows']}, "
                      f"updated_at=current_timestamp() WHERE observation_set_id='{set_id}'")
            envelope = T.StructType([
                T.StructField("record", table_schema),
                T.StructField("error_code", T.StringType()),
                T.StructField("error_detail", T.StringType()),
                T.StructField("raw_payload_json", T.StringType()),
                T.StructField("row_state", T.StringType()),
                T.StructField("source_row_locator", T.StringType()),
            ])
            transformed = spark.createDataFrame(
                raw.rdd.mapPartitions(lambda rows: _map_partition(
                    rows, spec, metadata, run_id, now, columns)), envelope
            ).persist(StorageLevel.DISK_ONLY)
            summary = {row["row_state"]: row["count"] for row in
                       transformed.groupBy("row_state").count().collect()}
            valid_count = summary.get("VALID", 0)
            bad_count = summary.get("QUARANTINE", 0)
            excluded_count = summary.get("EXCLUDED", 0)
            if valid_count == 0 and bad_count == 0:
                raise ValueError("No source observations")
            valid = transformed.filter(F.col("row_state") == "VALID").select("record.*")
            duplicate_ids = valid.groupBy("observation_id").count().filter("count > 1").count()
            if duplicate_ids:
                raise ValueError(f"Duplicate atomic observation IDs: {duplicate_ids}")
            if valid_count:
                stage_existing = spark.table(stage).filter(
                    F.col("observation_set_id") == set_id).count()
                if stage_existing == 0:
                    valid.writeTo(stage).append()
                elif stage_existing != valid_count:
                    raise ValueError("Existing staged release has different row count")
            if bad_count:
                invalid = transformed.filter(F.col("row_state") == "QUARANTINE").select(
                    F.sha2(F.concat_ws("|", F.lit(set_id), "source_row_locator", "error_code"), 256).alias("quarantine_id"),
                    F.lit(run_id).alias("run_id"), F.lit(dataset).alias("dataset_id"),
                    F.lit(set_id).alias("observation_set_id"), "source_row_locator",
                    "raw_payload_json", "error_code", "error_detail",
                    F.lit("ROW_FAIL").alias("severity"), F.lit(now).alias("quarantine_at"),
                    F.lit("OPEN").alias("resolution_status"), F.lit(None).cast("string").alias("resolution_note"),
                )
                invalid.createOrReplaceTempView("_silver_v2_quarantine")
                spark.sql("MERGE INTO lakehouse.silver_meta.quarantine_record t "
                          "USING _silver_v2_quarantine s ON t.quarantine_id=s.quarantine_id "
                          "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
                _upsert_records(spark, "lakehouse.silver_meta.dq_result", [dict(
                    dq_result_id=digest(run_id, dataset, "SOURCE_ROW_QUARANTINE"),
                    run_id=run_id, object_name=stage,
                    rule_code="SOURCE_ROW_QUARANTINE", severity="BLOCK",
                    checked_count=counts["input_rows"], failed_count=bad_count,
                    threshold=0.0, status="FAIL",
                    sample_ref=f"lakehouse.silver_meta.quarantine_record:{set_id}",
                    checked_at=now)], ["dq_result_id"])
                raise ValueError(f"Source rows quarantined; publication blocked: {bad_count}")
            staged = spark.table(stage).filter(F.col("observation_set_id") == set_id)
            staged.writeTo(core).append()
            published = spark.table(core).filter(F.col("observation_set_id") == set_id).count()
            if published != valid_count:
                raise ValueError("Read-after-write count mismatch")
            spark.sql(f"UPDATE lakehouse.silver_meta.observation_set SET status='PUBLISHED', "
                      f"updated_at=current_timestamp() WHERE observation_set_id='{set_id}'")
            result = dict(dataset_id=dataset, status="SUCCESS", input_rows=counts["input_rows"],
                          excluded_rows=excluded_count, valid_rows=valid_count,
                          quarantined_rows=bad_count, published_rows=published,
                          observation_set_id=set_id)
            _upsert_records(spark, "lakehouse.silver_meta.dq_result", [dict(
                dq_result_id=digest(run_id, dataset, "ROW_ACCOUNTING"), run_id=run_id,
                object_name=core, rule_code="ROW_ACCOUNTING", severity="BLOCK",
                checked_count=counts["input_rows"], failed_count=0, threshold=0.0,
                status="PASS", sample_ref=json.dumps(result), checked_at=now)],
                ["dq_result_id"])
            _upsert_records(spark, "lakehouse.silver_meta.ingestion_run", [dict(
                ingestion_run_id=run_id + ":" + dataset, dag_id="silver_v2_backfill",
                task_id=dataset, started_at=now, finished_at=datetime.utcnow(),
                environment="docker-compose", run_status="SUCCESS",
                rows_read=counts["input_rows"], rows_valid=valid_count,
                rows_quarantined=bad_count, rows_published=published)],
                ["ingestion_run_id"])
            return result
    except Exception as exc:
        _upsert_records(spark, "lakehouse.silver_meta.ingestion_run", [dict(
            ingestion_run_id=run_id + ":" + dataset, dag_id="silver_v2_backfill",
            task_id=dataset, started_at=now, finished_at=datetime.utcnow(),
            environment="docker-compose", run_status="FAILED", error_summary=str(exc)[:500])],
            ["ingestion_run_id"])
        raise
    finally:
        if transformed is not None:
            transformed.unpersist()
