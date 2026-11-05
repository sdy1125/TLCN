"""Positive and negative rule cases for Gold's whole-WINNER scope filter."""

import unittest

try:
    from pyspark.sql import SparkSession
    from gold.scope import classify
except ImportError:
    SparkSession = None


@unittest.skipUnless(SparkSession, "PySpark is unavailable on the host")
class ScopeRuntimeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[2]")
                     .appName("gold-scope-contract-test").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def classify_rows(self, domain, rows, schema):
        return [row.asDict() for row in classify(
            domain, self.spark.createDataFrame(rows, schema)).select(
                "_scope_status", "_scope_reason").collect()]

    def test_price_included_excluded_and_blocked(self):
        rows = [
            ("world_bank_pink_sheet", None, "GLOBAL"),
            ("provincial_coffee_average_price", "66", "PROVINCE"),
            ("provincial_coffee_average_price", None, "PROVINCE"),
            ("unknown_price_file", "66", "PROVINCE"),
        ]
        result = self.classify_rows(
            "price_observation", rows,
            "dataset_id string, geography_code string, market_scope string")
        self.assertEqual([r["_scope_status"] for r in result],
                         ["INCLUDED", "INCLUDED",
                          "BLOCKED_UNMAPPED", "BLOCKED_UNMAPPED"])
        self.assertTrue(result[1]["_scope_reason"])

    def test_trade_reported_is_never_mixed_with_source_total(self):
        rows = [
            ("faostat_tcl_trade_coffee", "SOURCE_TOTAL"),
            ("provincial_coffee_export_volume_value", "NATIONAL_REPORTED"),
            ("provincial_coffee_export_volume_value", "UNREVIEWED"),
        ]
        result = self.classify_rows(
            "trade_total_observation", rows,
            "dataset_id string, statistical_scope_code string")
        self.assertEqual([r["_scope_status"] for r in result],
                         ["INCLUDED", "INCLUDED", "BLOCKED_UNMAPPED"])

    def test_stock_unverified_country_is_blocked(self):
        rows = [
            ("provincial_coffee_world_ending_stocks", "704", None),
            ("provincial_coffee_world_ending_stocks", None,
             '{"country":"European Union"}'),
            ("provincial_coffee_world_ending_stocks", None, None),
        ]
        result = self.classify_rows(
            "coffee_stock_observation", rows,
            "dataset_id string, country_m49_code string, raw_payload_json string")
        self.assertEqual([r["_scope_status"] for r in result],
                         ["INCLUDED", "EXCLUDED_APPROVED", "BLOCKED_UNMAPPED"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
