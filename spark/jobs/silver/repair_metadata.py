"""Fill missing XLSX source counts from verified immutable Bronze workbooks."""

import json
from openpyxl import load_workbook
from pyspark.sql import SparkSession

from silver.readers import download, resolve
from silver.registry import SOURCES
from silver.identity import observation_set_id


def main():
    spark = (SparkSession.builder.appName("silver-repair-metadata")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    repaired = {}
    try:
        for dataset, spec in SOURCES.items():
            if spec["format"] != "xlsx":
                continue
            m = resolve(dataset)
            release = m.get("source_modified_time") or m["ingestion_id"]
            set_id = observation_set_id(dataset, release, m["checksum_sha256"])
            with download(m) as path:
                workbook = load_workbook(path, read_only=True, data_only=True)
                try:
                    if [sheet.title for sheet in workbook.worksheets] != ["data"]:
                        raise ValueError(f"Unreviewed workbook layout: {dataset}")
                    count = sum(1 for _ in workbook.active.values) - 1
                finally:
                    workbook.close()
            spark.sql("UPDATE lakehouse.silver_meta.observation_set "
                      f"SET source_record_count={count}, updated_at=current_timestamp() "
                      f"WHERE observation_set_id='{set_id}'")
            repaired[dataset] = count
        print(json.dumps(repaired), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
