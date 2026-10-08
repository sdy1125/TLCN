"""Iceberg publication: one atomic MERGE replaces only the selected dataset.

Absent keys are represented as tombstones in the same MERGE transaction.
Historical source contents remain available through Iceberg snapshots + Bronze.
"""

from pyspark.sql import functions as F
from .contracts import schema, compatibility
from .registry import CONTRACTS

CONTROL = {
    "processing_state": "state_key string, dataset_id string, bronze_checksum_sha256 string, bronze_ingestion_id string, transform_version string, run_id string, status string, target_contract string, snapshot_id long, output_rows long, details_json string, checked_at timestamp",
    "dq_results": "run_id string, dataset_id string, bronze_ingestion_id string, target_contract string, check_name string, check_scope string, severity string, status string, observed_value string, expected_value string, threshold double, details_json string, checked_at timestamp",
    "quarantine_records": "dataset_id string, bronze_ingestion_id string, bronze_checksum_sha256 string, source_record_id string, target_contract string, error_code string, error_message string, error_severity string, raw_payload_json string, detected_at timestamp, airflow_run_id string, transform_version string",
}


def identifier(namespace):
    import re

    if not re.fullmatch(r"[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*", namespace):
        raise ValueError("Invalid catalog.namespace")
    return namespace


def initialize(spark, namespace):
    identifier(namespace)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    for name in CONTRACTS:
        if not spark.catalog.tableExists(f"{namespace}.{name}"):
            # SQL keeps required fields explicit, rather than inferring empty DF nullability.
            columns = ", ".join(
                f"`{f.name}` {f.dataType.simpleString()}"
                + ("" if f.nullable else " NOT NULL")
                for f in schema(name)
            )
            spark.sql(
                f"CREATE TABLE IF NOT EXISTS {namespace}.{name} ({columns}) USING iceberg PARTITIONED BY (years(period_start)) TBLPROPERTIES ('format-version'='2','write.distribution-mode'='hash')"
            )
    for name, ddl in CONTROL.items():
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {namespace}.{name} ({ddl}) USING iceberg TBLPROPERTIES ('format-version'='2')"
        )


def align(spark, table, incoming):
    existing = spark.table(table).schema
    # Spark query nullability is often relaxed. Compare reviewed contract rather
    # than deriving a required-field migration from a query plan.
    added = compatibility(existing, incoming)
    for f in added:
        spark.sql(
            f"ALTER TABLE {table} ADD COLUMN `{f.name}` {f.dataType.simpleString()}"
        )


def publish(spark, namespace, target, dataset, frame):
    table = f"{namespace}.{target}"
    align(spark, table, schema(target))
    columns = frame.columns
    absent = (
        spark.table(table)
        .filter(F.col("dataset_id") == dataset)
        .join(frame.select("source_record_id"), "source_record_id", "left_anti")
        .select(*columns)
        .withColumn("_delete", F.lit(True))
    )
    updates = frame.withColumn("_delete", F.lit(False)).unionByName(absent)
    updates.createOrReplaceTempView("_silver_updates")
    assignments = ", ".join(f"t.`{c}`=s.`{c}`" for c in columns)
    names = ", ".join(f"`{c}`" for c in columns)
    values = ", ".join(f"s.`{c}`" for c in columns)
    spark.sql(
        f"""MERGE INTO {table} t USING _silver_updates s ON t.dataset_id=s.dataset_id AND t.source_record_id=s.source_record_id
        WHEN MATCHED AND s._delete THEN DELETE
        WHEN MATCHED THEN UPDATE SET {assignments}
        WHEN NOT MATCHED AND NOT s._delete THEN INSERT ({names}) VALUES ({values})"""
    )
    return spark.sql(
        f"SELECT snapshot_id FROM {table}.history ORDER BY made_current_at DESC LIMIT 1"
    ).first()[0]


def upsert_control(spark, namespace, name, frame, keys):
    frame.createOrReplaceTempView("_silver_control")
    condition = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)
    spark.sql(
        f"MERGE INTO {namespace}.{name} t USING _silver_control s ON {condition} WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
    )
