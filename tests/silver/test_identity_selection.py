import unittest

from silver.identity import observation_set_id, observation_id


class IdentitySelectionTest(unittest.TestCase):
    def test_release_and_row_ids_are_stable(self):
        source_hash = "a" * 64
        first = observation_set_id("price", "2022", source_hash)
        self.assertEqual(first, observation_set_id("price", "2022", source_hash))
        self.assertNotEqual(first, observation_set_id("price", "2023", source_hash))
        self.assertNotEqual(observation_id("price", first, "sheet:data:row:2", "PRICE"), observation_id("price", first, "sheet:data:row:3", "PRICE"))


if __name__ == "__main__":
    unittest.main()
