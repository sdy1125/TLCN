"""Read verified Bronze releases and exercise parsers without writing data."""

import json
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F

from silver import readers
from silver.domains import schema, target, transform
from silver.registry import SOURCES


SAMPLES = (
    "faostat_production_coffee",
    "provincial_agriculture_three_commodities",
    "faostat_producer_prices_annual_coffee",
    "world_bank_pink_sheet",
    "provincial_coffee_average_price",
    "faostat_tcl_trade_coffee",
    "faostat_detailed_trade_matrix_coffee",
    "un_comtrade_annual",
    "nasa_power_weather_daily_63",
    "provincial_coffee_world_ending_stocks",
    "world_bank_wdi",
)


def main():
    spark = SparkSession.builder.appName("silver-bronze-parser-smoke").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    results = []
    try:
        for dataset in SAMPLES:
            spec = dict(SOURCES[dataset])
            metadata = readers.resolve(dataset)
            with readers.download(metadata) as path:
                raw, _ = readers.read_with_lineage(spark, path, spec)
                if dataset == "world_bank_pink_sheet":
                    raw = raw.filter((F.col("source_sheet") == "Monthly Prices") &
                                     F.col("column_0").rlike(r"^\d{4}M\d{2}$"))
                candidates = raw.limit(10).collect()
                observations = []
                for candidate in candidates:
                    row = {key: "" if value is None else str(value)
                           for key, value in candidate.asDict().items()}
                    observations.extend(transform(row, spec, metadata, "smoke-silver",
                                                  datetime.now(timezone.utc).replace(tzinfo=None)))
                    if observations:
                        break
                if not observations:
                    raise ValueError(f"No observation parsed for {dataset}")
                table = target(dataset)
                columns = [field.name for field in schema(table)]
                frame = spark.createDataFrame(
                    [tuple(row.get(name) for name in columns) for row in observations],
                    schema(table))
                if frame.filter("observation_id IS NULL OR source_row_locator IS NULL "
                                "OR source_record_hash IS NULL").count():
                    raise ValueError(f"Missing lineage for {dataset}")
                results.append({"dataset_id": dataset, "domain": table,
                                "parsed": frame.count()})
        print(json.dumps({"status": "PASS", "samples": results}), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
