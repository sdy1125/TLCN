import unittest

from gold.contracts import BRIDGE, DIMENSIONS, FACTS, key


class GoldContractsTest(unittest.TestCase):
    def test_model_count(self):
        self.assertEqual((len(DIMENSIONS), len(FACTS), len(BRIDGE)), (8, 8, 1))

    def test_key_is_stable_and_distinguishes_null(self):
        self.assertEqual(key("A", None, ""), key("A", None, ""))
        self.assertNotEqual(key("A", None, ""), key("A", "", None))
        self.assertNotEqual(key("A", "BC"), key("AB", "C"))

    def test_all_tables_have_declared_keys(self):
        for name, fields in {**DIMENSIONS, **FACTS}.items():
            self.assertIn("_key string", fields, name)
