"""Small Spark regression suite; skip cleanly on hosts without PySpark."""

import importlib.util
import unittest
from datetime import datetime


@unittest.skipUnless(importlib.util.find_spec("pyspark"), "PySpark runtime required")
class SparkQualityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("silver-quality-tests")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def envelope(self, values):
        from pyspark.sql import types as T
        from silver.contracts import schema
        from silver.transforms import rows
        from silver.registry import SOURCES

        d = "provincial_coffee_area"
        spec = SOURCES[d]
        meta = dict(
            dataset_id=d,
            source_system=spec["source_system"],
            source_file_name="a.xlsx",
            bronze_path="s3://bronze/a",
            ingestion_id="id",
            checksum_sha256="a" * 64,
        )
        raw = [
            {"Năm": "2024", "Địa phương": "Đắk Nông", "Giá_trị": v, "ĐVT": "Nghìn ha"}
            for v in values
        ]
        sch = T.StructType(
            [
                T.StructField("record", schema(spec["target"])),
                T.StructField("error_code", T.StringType()),
                T.StructField("error_message", T.StringType()),
                T.StructField("raw_payload_json", T.StringType()),
                T.StructField("raw_id", T.StringType()),
            ]
        )
        return self.spark.createDataFrame(
            list(rows(raw, spec, meta, "v", "run", datetime(2026, 1, 1))), sch
        )

    def coffee_price_envelope(self, dak_lak_values):
        from pyspark.sql import types as T
        from silver.contracts import schema
        from silver.transforms import rows
        from silver.registry import SOURCES

        dataset = "provincial_coffee_average_price"
        spec = SOURCES[dataset]
        metadata = dict(
            dataset_id=dataset,
            source_system=spec["source_system"],
            source_file_name="coffee-price.xlsx",
            bronze_path="s3://bronze/coffee-price.xlsx",
            ingestion_id="price-id",
            checksum_sha256="b" * 64,
        )
        raw = [
            {
                "Năm": "2022",
                "Tháng": "12",
                "Giá Đăk Lăk": value,
                "Giá Lâm Đồng": "40259",
                "ĐVT": "Đồng/kg",
            }
            for value in dak_lak_values
        ]
        envelope_schema = T.StructType(
            [
                T.StructField("record", schema(spec["target"])),
                T.StructField("error_code", T.StringType()),
                T.StructField("error_message", T.StringType()),
                T.StructField("raw_payload_json", T.StringType()),
                T.StructField("raw_id", T.StringType()),
            ]
        )
        return self.spark.createDataFrame(
            list(
                rows(
                    raw,
                    spec,
                    metadata,
                    "v",
                    "run",
                    datetime(2026, 1, 1),
                )
            ),
            envelope_schema,
        )

    def test_exact_duplicates_and_conflicts(self):
        from silver.quality import audit

        valid, _, metrics, passed = audit(
            self.envelope(["2", "2"]), "production_provincial", "generic_source"
        )
        self.assertTrue(passed)
        self.assertEqual(metrics["duplicates_removed"], 1)
        self.assertEqual(valid.count(), 1)
        _, errors, metrics, passed = audit(
            self.envelope(["2", "3"]), "production_provincial", "generic_source"
        )
        self.assertFalse(passed)
        self.assertEqual(metrics["conflicting_keys"], 1)
        self.assertEqual(errors.count(), 2)
        self.assertTrue(metrics["accounting_ok"])

    def test_error_threshold(self):
        from silver.quality import audit

        _, errors, metrics, passed = audit(
            self.envelope(["oops"]), "production_provincial", "generic_source"
        )
        self.assertFalse(passed)
        self.assertEqual(errors.first().error_code, "PARSE_ERROR")

    def test_reviewed_conflict_resolution_is_narrow_and_traceable(self):
        from silver.quality import audit

        dataset = "provincial_coffee_average_price"
        valid, warnings, metrics, passed = audit(
            self.coffee_price_envelope(["40798", "40797"]),
            "market_indicator",
            dataset,
        )
        self.assertTrue(passed)
        self.assertEqual(metrics["resolved_conflicting_keys"], 1)
        self.assertEqual(metrics["unresolved_conflicting_keys"], 0)
        self.assertEqual(metrics["warning_quarantined_rows"], 1)
        self.assertEqual(metrics["duplicates_removed"], 1)
        self.assertEqual(metrics["output_rows"], 2)
        winner = valid.filter("province_code = '66'").first()
        self.assertEqual(str(winner.value_standard), "40798.00000000")
        self.assertIn("resolved_source_conflict", winner.quality_flag)
        self.assertEqual(
            warnings.first().error_code, "RESOLVED_CONFLICTING_DUPLICATE"
        )

        _, errors, metrics, passed = audit(
            self.coffee_price_envelope(["40798", "40796"]),
            "market_indicator",
            dataset,
        )
        self.assertFalse(passed)
        self.assertEqual(metrics["resolved_conflicting_keys"], 0)
        self.assertEqual(metrics["unresolved_conflicting_keys"], 1)
        self.assertEqual(
            errors.filter("error_code = 'CONFLICTING_DUPLICATE'").count(), 2
        )

    def test_nasa_sentinel_and_leap_day(self):
        from silver.transforms.weather_daily import transform
        from silver.registry import SOURCES

        d = "nasa_power_weather_daily_63"
        spec = SOURCES[d]
        r = dict.fromkeys(spec["headers"], "")
        r.update(
            YEAR="2024",
            DOY="366",
            T2M="-999.00",
            T2M_MAX="30",
            T2M_MIN="20",
            RH2M="50",
            WS10M="3",
            PRECTOTCORR="0",
            ALLSKY_SFC_SW_DWN="12",
            weather_point="an_giang",
            province_code_63="89",
            province_name="An Giang",
            latitude="10.3864",
            longitude="105.4352",
        )
        meta = dict(
            dataset_id=d,
            source_system="nasa_power",
            source_file_name="a.csv",
            bronze_path="s3://bronze/a",
            ingestion_id="id",
            checksum_sha256="a" * 64,
        )
        frame = self.spark.createDataFrame([r])
        out = transform(frame, meta, "v", "run", datetime(2026, 1, 1)).first()
        self.assertIsNone(out.error_code)
        self.assertIsNone(out.record.temperature_avg_c)
        self.assertEqual(out.record.precipitation_mm, 0)
        r["YEAR"] = "2023"
        out = transform(
            self.spark.createDataFrame([r]), meta, "v", "run", datetime(2026, 1, 1)
        ).first()
        self.assertEqual(out.error_code, "INVALID_PERIOD")


if __name__ == "__main__":
    unittest.main()
