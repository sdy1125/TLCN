import unittest

from silver.identity import observation_set_id, observation_id
from silver.selection import decide


class IdentitySelectionTest(unittest.TestCase):
    def test_release_and_row_ids_are_stable(self):
        source_hash = "a" * 64
        first = observation_set_id("price", "2022", source_hash)
        self.assertEqual(first, observation_set_id("price", "2022", source_hash))
        self.assertNotEqual(first, observation_set_id("price", "2023", source_hash))
        self.assertNotEqual(observation_id("price", first, "sheet:data:row:2", "PRICE"), observation_id("price", first, "sheet:data:row:3", "PRICE"))

    def test_price_conflict_has_no_winner(self):
        policy = {"source_priority": {"local": 1}, "allowed_statuses": ["VALID"], "key_fields": ["province", "period", "series"]}
        rows = [dict(dataset_id="local", observation_id=str(i), source_row_locator=f"row:{i}", value_status="VALID", quality_status="VALID", value_canonical=value, province="66", period="2022-12", series="dak_lak") for i, value in [(38, 40798), (50, 40797)]]
        decisions, conflicts = decide(rows, policy)
        self.assertEqual(0, sum(x[1] == "WINNER" for x in decisions))
        self.assertEqual(1, len(conflicts))

    def test_identical_duplicate_selects_one(self):
        policy = {"source_priority": {"local": 1}, "allowed_statuses": ["VALID"], "key_fields": ["province", "period", "series"]}
        rows = [dict(dataset_id="local", observation_id=str(i), source_row_locator=f"row:{i}", value_status="VALID", quality_status="VALID", value_canonical=40000, province="68", period="2022-12", series="lam_dong") for i in (38, 50)]
        decisions, conflicts = decide(rows, policy)
        self.assertEqual(1, sum(x[1] == "WINNER" for x in decisions))
        self.assertFalse(conflicts)


if __name__ == "__main__":
    unittest.main()
