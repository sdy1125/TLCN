"""Guard against accidentally writing Silver into the legacy warehouse root."""

import unittest

from silver.storage import ensure_namespace, ensure_table_location


class _Spark:
    def __init__(self, namespace_location, table_location):
        self.namespace_location = namespace_location
        self.table_location = table_location
        self.queries = []

    def sql(self, query):
        self.queries.append(query)
        if query.startswith("DESCRIBE NAMESPACE"):
            return _Result([("Location", self.namespace_location)])
        if query.startswith("DESCRIBE TABLE"):
            return _Result([("Location", self.table_location)])
        return _Result([])


class _Result:
    def __init__(self, rows):
        self.rows = rows

    def collect(self):
        return self.rows


class StorageTests(unittest.TestCase):
    def test_new_namespace_and_table_use_silver_prefix(self):
        spark = _Spark("s3://warehouse/silver/silver_core/",
                       "s3://warehouse/silver/silver_core/production_observation/")
        ensure_namespace(spark, "silver_core")
        ensure_table_location(spark, "silver_core", "production_observation")
        self.assertIn("LOCATION 's3://warehouse/silver/silver_core'", spark.queries[0])

    def test_legacy_namespace_is_rejected(self):
        spark = _Spark("s3://warehouse/silver_core", "")
        with self.assertRaisesRegex(RuntimeError, "Migrate the existing catalog"):
            ensure_namespace(spark, "silver_core")

    def test_legacy_table_is_rejected(self):
        spark = _Spark("", "s3://warehouse/silver_core/production_observation")
        with self.assertRaisesRegex(RuntimeError, "Migrate the existing table"):
            ensure_table_location(spark, "silver_core", "production_observation")


if __name__ == "__main__":
    unittest.main()
