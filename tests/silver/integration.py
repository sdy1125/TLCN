"""Real Spark/MinIO/Iceberg integration with isolated fixture bucket/namespace.

spark-submit --master local[2] /tests/silver/integration.py
No existing Bronze object is written. Fixtures and tables are retained for audit.
"""

import csv
import hashlib
import io
import json
import uuid
from datetime import datetime
from pathlib import Path
from pyspark.sql import SparkSession, functions as F, types as T
from silver.readers import client
from silver.registry import SOURCES
from silver.pipeline import run_dataset
from silver.writer import compatibility, align

spark = SparkSession.builder.appName("silver-integration").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
namespace = "lakehouse.silver_test_" + uuid.uuid4().hex[:10]
connection = client()
bucket = "silver-test-fixtures"
if not connection.bucket_exists(bucket):
    connection.make_bucket(bucket)


def fixture(dataset, value="2", xlsx=False):
    spec = SOURCES[dataset]
    if xlsx:
        from openpyxl import Workbook

        wb = Workbook()
        ws = wb.active
        ws.append(spec["headers"])
        ws.append(["Sơ bộ năm 2024", "Đắk Nông", float(value), "Nghìn ha"])
        buffer = io.BytesIO()
        wb.save(buffer)
        data = buffer.getvalue()
    else:
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=spec["headers"])
        writer.writeheader()
        writer.writerow(
            {
                "Area Code (M49)": "704",
                "Area": "Viet Nam",
                "Element Code": "5510",
                "Element": "Production",
                "Item Code (FAO)": {"coffee":"656", "rubber":"836", "pepper":"687"}[spec["commodity"]],
                "Year": "2024",
                "Unit": "t",
                "Value": value,
                "Flag": "A",
            }
        )
        data = buffer.getvalue().encode()
    checksum = hashlib.sha256(data).hexdigest()
    key = f"{namespace}/{dataset}/{checksum}." + spec["format"]
    connection.put_object(bucket, key, io.BytesIO(data), len(data))
    return dict(
        dataset_id=dataset,
        source_system=spec["source_system"],
        source_file_name=dataset + "." + spec["format"],
        bronze_path=f"s3://{bucket}/{key}",
        ingestion_id=checksum[:32],
        checksum_sha256=checksum,
        file_size_bytes=len(data),
        status="SUCCESS",
    )


def snapshot(target):
    return spark.sql(
        f"SELECT snapshot_id FROM {namespace}.{target}.history ORDER BY made_current_at DESC LIMIT 1"
    ).first()[0]


try:
    a = "faostat_production_coffee"
    b = "provincial_coffee_area"
    m = fixture(a)
    result = run_dataset(spark, a, "fixture-first", namespace, m)
    assert result["output_rows"] == 1
    before = snapshot("production_national")
    assert run_dataset(spark, a, "fixture-rerun", namespace, m)["status"] == "SKIPPED"
    assert snapshot("production_national") == before
    x = fixture(b, xlsx=True)
    assert run_dataset(spark, b, "fixture-xlsx", namespace, x)["output_rows"] == 1
    row = spark.table(namespace + ".production_provincial").first()
    assert row.value_standard == 2000 and row.observation_status == "provisional"
    x_snapshot = snapshot("production_provincial")
    other = "faostat_production_rubber"
    other_meta = fixture(other, "7")
    run_dataset(spark, other, "fixture-neighbour", namespace, other_meta)
    m2 = fixture(a, "3")
    run_dataset(spark, a, "fixture-changed", namespace, m2)
    assert spark.table(namespace + ".production_national").filter(F.col("dataset_id") == a).first().value_standard == 3
    assert snapshot("production_provincial") == x_snapshot
    assert spark.table(namespace + ".production_national").filter(F.col("dataset_id") == other).first().value_standard == 7
    # Reverting to a previous checksum must re-publish, not incorrectly skip an old SUCCESS.
    assert run_dataset(spark, a, "fixture-revert", namespace, m)["status"] != "SKIPPED"
    assert spark.table(namespace + ".production_national").filter(F.col("dataset_id") == a).first().value_standard == 2
    existing = spark.table(namespace + ".production_national").schema
    added = T.StructType(
        existing.fields + [T.StructField("optional_note", T.StringType(), True)]
    )
    align(spark, namespace + ".production_national", added)
    assert "optional_note" in spark.table(namespace + ".production_national").columns
    bad = T.StructType([f for f in existing if f.name != "value_standard"])
    try:
        compatibility(existing, bad)
    except ValueError:
        pass
    else:
        raise AssertionError("Removal accepted")
    bad = T.StructType(
        [
            (
                T.StructField(f.name, T.IntegerType(), True)
                if f.name == "value_standard"
                else f
            )
            for f in existing
        ]
    )
    try:
        compatibility(existing, bad)
    except ValueError:
        pass
    else:
        raise AssertionError("Narrowing accepted")
    print(
        "INTEGRATION_PASS "
        + json.dumps(
            {
                "namespace": namespace,
                "checks": [
                    "CSV_Bronze_reader",
                    "XLSX_Bronze_reader",
                    "conversion",
                    "provisional",
                    "same_version_no_snapshot",
                    "changed_version",
                    "scope_preserved",
                    "same_table_other_dataset_preserved",
                    "checksum_revert",
                    "additive_evolution",
                    "incompatible_evolution",
                ],
            }
        )
    )
finally:
    spark.stop()
