"""Synthetic temporal dimension cases for Gold Fact mapping."""

import unittest

try:
    from pyspark.sql import SparkSession, functions as F
    from gold.temporal import attach_key
except ImportError:
    SparkSession = None


@unittest.skipUnless(SparkSession, "PySpark is unavailable on the host")
class TemporalJoinTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[2]")
                     .appName("gold-temporal-contract-test").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def dimensions(self, rows):
        frame = self.spark.createDataFrame(
            rows, "series_code string, price_series_key string, "
                  "valid_from string, valid_to string")
        return (frame.withColumn("valid_from", F.to_date("valid_from"))
                .withColumn("valid_to", F.to_date("valid_to")))

    def source(self, code):
        return self.spark.createDataFrame(
            [("observation-1", "A", "MONTH", code)],
            "observation_id string, series_code string, "
            "period_type string, period_code string")

    def map(self, code, rows):
        return attach_key(self.source(code), self.dimensions(rows),
                          "series_code", "series_code", "price_series_key")

    def test_null_start_and_end_cover_month(self):
        result = self.map("2025-01", [("A", "old", None, None)])
        self.assertEqual(result.first()["price_series_key"], "old")

    def test_adjacent_versions_choose_correct_key(self):
        rows = [("A", "old", None, "2025-01-31"),
                ("A", "new", "2025-02-01", None)]
        self.assertEqual(self.map("2025-02", rows).first()["price_series_key"],
                         "new")

    def test_gap_blocks_mapping(self):
        rows = [("A", "old", None, "2025-01-15"),
                ("A", "new", "2025-02-01", None)]
        with self.assertRaises(ValueError):
            self.map("2025-01", rows)

    def test_month_crossing_version_blocks_mapping(self):
        rows = [("A", "old", None, "2025-01-15"),
                ("A", "new", "2025-01-16", None)]
        with self.assertRaises(ValueError):
            self.map("2025-01", rows)

    def test_overlap_blocks_mapping(self):
        rows = [("A", "old", None, "2025-02-01"),
                ("A", "new", "2025-02-01", None)]
        with self.assertRaises(ValueError):
            self.map("2025-01", rows)

    def test_missing_dimension_blocks_mapping(self):
        with self.assertRaises(ValueError):
            self.map("2025-01", [("B", "other", None, None)])


if __name__ == "__main__":
    unittest.main(verbosity=2)
