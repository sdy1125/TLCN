import unittest

from gold.candidate import candidate_namespace, validate_id


class CandidateIdentityTest(unittest.TestCase):
    def test_candidate_namespace_is_stable(self):
        value = candidate_namespace("gold-test-20261104")
        self.assertEqual(value, candidate_namespace("gold-test-20261104"))
        self.assertNotEqual(value, candidate_namespace("gold-test-20261105"))
        self.assertTrue(value.startswith("gold_candidate_"))

    def test_candidate_id_rejects_sql_input(self):
        with self.assertRaises(ValueError):
            validate_id("gold-a'; DROP TABLE x")
