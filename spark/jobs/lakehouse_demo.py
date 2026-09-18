"""Create small Bronze, Silver and Gold Iceberg tables for an end-to-end smoke test."""

from pyspark.sql import SparkSession, functions as F, types as T


spark = SparkSession.builder.appName("tlcn-lakehouse-demo").getOrCreate()

for namespace in ("bronze", "silver", "gold"):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lakehouse.{namespace}")

schema = T.StructType(
    [
        T.StructField("period", T.StringType(), False),
        T.StructField("commodity", T.StringType(), False),
        T.StructField("country", T.StringType(), False),
        T.StructField("export_quantity_tons", T.DoubleType(), False),
        T.StructField("export_value_usd", T.DoubleType(), False),
        T.StructField("source", T.StringType(), False),
    ]
)

rows = [
    ("2021-12-01", "coffee", "Vietnam", 1500.0, 3300000.0, "demo"),
    ("2022-12-01", "coffee", "Vietnam", 1620.0, 4050000.0, "demo"),
    ("2023-12-01", "coffee", "Vietnam", 1710.0, 4788000.0, "demo"),
    ("2024-12-01", "coffee", "Vietnam", 1810.0, 5611000.0, "demo"),
    ("2021-12-01", "pepper", "Vietnam", 650.0, 2080000.0, "demo"),
    ("2022-12-01", "pepper", "Vietnam", 680.0, 2312000.0, "demo"),
    ("2023-12-01", "pepper", "Vietnam", 720.0, 2664000.0, "demo"),
    ("2024-12-01", "pepper", "Vietnam", 760.0, 3116000.0, "demo"),
    ("2021-12-01", "rubber", "Vietnam", 2100.0, 3570000.0, "demo"),
    ("2022-12-01", "rubber", "Vietnam", 2220.0, 3885000.0, "demo"),
    ("2023-12-01", "rubber", "Vietnam", 2350.0, 4230000.0, "demo"),
    ("2024-12-01", "rubber", "Vietnam", 2470.0, 4693000.0, "demo"),
]

bronze = spark.createDataFrame(rows, schema=schema)
bronze.writeTo("lakehouse.bronze.agri_trade_raw").using("iceberg").createOrReplace()

silver = (
    bronze.withColumn("period", F.to_date("period"))
    .withColumn("commodity", F.upper(F.trim("commodity")))
    .withColumn("country", F.initcap(F.trim("country")))
    .withColumn("year", F.year("period"))
    .withColumn(
        "export_unit_value_usd_per_ton",
        F.round(F.col("export_value_usd") / F.col("export_quantity_tons"), 2),
    )
    .dropDuplicates(["period", "commodity", "country", "source"])
)
silver.writeTo("lakehouse.silver.agri_trade").using("iceberg").createOrReplace()

gold = silver.groupBy("year", "commodity").agg(
    F.round(F.sum("export_quantity_tons"), 2).alias("export_quantity_tons"),
    F.round(F.sum("export_value_usd"), 2).alias("export_value_usd"),
    F.round(
        F.sum("export_value_usd") / F.sum("export_quantity_tons"), 2
    ).alias("export_unit_value_usd_per_ton"),
)
gold.writeTo("lakehouse.gold.commodity_year_summary").using("iceberg").createOrReplace()

print("Created lakehouse.bronze.agri_trade_raw")
print("Created lakehouse.silver.agri_trade")
print("Created lakehouse.gold.commodity_year_summary")
gold.orderBy("year", "commodity").show(truncate=False)

spark.stop()

