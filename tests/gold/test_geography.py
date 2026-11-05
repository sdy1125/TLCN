"""Official district crosswalk must remain year-specific and unambiguous."""

import unittest

from spark.gold.geography import ROWS, trino_values, validate


class DistrictCrosswalkTests(unittest.TestCase):
    def test_reviewed_rows_have_unique_source_year(self):
        self.assertEqual(validate(), 296)
        self.assertEqual(sum(row["mapping_status"] == "VERIFIED"
                             for row in ROWS), 295)

    def test_annual_boundary_is_not_assigned_year_end_code(self):
        rows = [row for row in ROWS if row["raw_name"] == "Dahuoai district"]
        previous = [row for row in rows if row["year"] < 2024]
        self.assertTrue(previous)
        self.assertTrue(all(row["official_district_code"] == "681"
                            for row in previous))
        last = [row for row in rows if row["year"] == 2024]
        self.assertEqual(len(last), 1)
        self.assertEqual(last[0]["mapping_status"],
                         "UNRESOLVED_ANNUAL_BOUNDARY")
        self.assertIsNone(last[0]["official_district_code"])

    def test_trino_values_escape_source_names(self):
        values = trino_values()
        self.assertIn("Dak G''long district", values)
        self.assertIn("CAST(NULL AS varchar)", values)


if __name__ == "__main__":
    unittest.main()
