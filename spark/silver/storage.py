"""Keep every Silver Iceberg namespace under the Silver warehouse prefix."""

WAREHOUSE_SILVER = "s3://warehouse/silver"


def ensure_namespace(spark, namespace):
    if namespace not in {"silver_core", "silver_stage", "silver_meta", "silver_ref"}:
        raise ValueError(f"Unknown Silver namespace: {namespace}")
    expected = f"{WAREHOUSE_SILVER}/{namespace}"
    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS lakehouse.{namespace} "
        f"LOCATION '{expected}'"
    )
    details = spark.sql(
        f"DESCRIBE NAMESPACE EXTENDED lakehouse.{namespace}"
    ).collect()
    locations = [
        str(row[1]).rstrip("/") for row in details
        if str(row[0]).strip().lower() == "location"
    ]
    if locations != [expected]:
        raise RuntimeError(
            f"lakehouse.{namespace} location is {locations or 'unknown'}, "
            f"expected {expected}. Migrate the existing catalog before writing Silver."
        )


def ensure_table_location(spark, namespace, table):
    expected = f"{WAREHOUSE_SILVER}/{namespace}/{table}"
    details = spark.sql(
        f"DESCRIBE TABLE EXTENDED lakehouse.{namespace}.{table}"
    ).collect()
    locations = [
        str(row[1]).rstrip("/") for row in details
        if str(row[0]).strip().lower() == "location"
    ]
    if locations != [expected]:
        raise RuntimeError(
            f"lakehouse.{namespace}.{table} location is {locations or 'unknown'}, "
            f"expected {expected}. Migrate the existing table before writing Silver."
        )
