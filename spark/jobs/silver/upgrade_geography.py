"""Idempotently add source-scoped district identities to existing releases."""

import json

from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("silver-upgrade-geography").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    results = {}
    try:
        for namespace in ("silver_stage", "silver_core"):
            table = f"lakehouse.{namespace}.provincial_agri_observation"
            spark.sql(f"""
                UPDATE {table}
                SET geography_code = concat('SRC_DISTRICT_', sha2(concat_ws('|',
                        dataset_id, geography_version, province_code,
                        lower(trim(raw_district))), 256)),
                    coverage_code = 'ALL_PRODUCERS'
                WHERE geography_level = 'DISTRICT'
                  AND lower(trim(source_scope_raw)) = 'district total'
                  AND (geography_code IS NULL OR coverage_code <> 'ALL_PRODUCERS')
            """)
            spark.sql(f"""
                UPDATE {table}
                SET coverage_code = 'ALL_PRODUCERS', quality_status = 'VALID'
                WHERE geography_level = 'PROVINCE'
                  AND lower(trim(source_scope_raw)) = 'province total'
                  AND (coverage_code <> 'ALL_PRODUCERS' OR quality_status <> 'VALID')
            """)
            frame = spark.table(table)
            results[namespace] = {
                "source_district_rows": frame.filter(
                    "geography_level='DISTRICT' AND "
                    "geography_code LIKE 'SRC_DISTRICT_%' AND "
                    "coverage_code='ALL_PRODUCERS'").count(),
                "province_total_rows": frame.filter(
                    "geography_level='PROVINCE' AND "
                    "source_scope_raw='province total' AND "
                    "coverage_code='ALL_PRODUCERS' AND quality_status='VALID'").count(),
            }
        if results["silver_stage"] != results["silver_core"]:
            raise ValueError("Stage/core geography upgrade differs")
        print(json.dumps(results), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
