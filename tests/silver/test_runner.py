import unittest
import tempfile
from pathlib import Path

from silver.runner import (authorized, gold_input_matches,
                           normalize_silver_release_request, validate_token)


class RunnerAuthenticationTests(unittest.TestCase):
    def test_token_must_be_present_and_long_enough(self):
        for token in (None, "", "short"):
            with self.assertRaises(RuntimeError):
                validate_token(token)

    def test_bearer_authentication_is_exact(self):
        token = "a" * 32
        self.assertTrue(authorized("Bearer " + token, validate_token(token)))
        self.assertFalse(authorized(token, token))
        self.assertFalse(authorized("Bearer " + "b" * 32, token))
        self.assertFalse(authorized(None, token))

    def test_gold_inspect_must_match_pinned_silver_batch(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "spark.log"
            log.write_text('Spark started\n{"silver_batch_id":"silver-batch-a"}\n')
            self.assertTrue(gold_input_matches(log, "silver-batch-a"))
            self.assertFalse(gold_input_matches(log, "silver-batch-b"))
            log.write_text('Spark started\n')
            self.assertFalse(gold_input_matches(log, "silver-batch-a"))

    def test_silver_release_retains_single_dataset_selection(self):
        request = {
            "pipeline": "silver_release", "stage": "backfill",
            "run_id": "repair-1", "bronze_release_id": "bronze-repair-1",
            "batch_id": "silver-repair-1", "dq_run_id": "silver-dq-repair-1",
            "dataset_id": "world_bank_wdi",
        }
        normalized = normalize_silver_release_request(request)
        self.assertEqual(normalized["datasets"], ["world_bank_wdi"])
        self.assertEqual(len(normalize_silver_release_request(
            {key: value for key, value in request.items()
             if key != "dataset_id"})["datasets"]), 26)
        with self.assertRaises(ValueError):
            normalize_silver_release_request({**request, "dataset_id": "missing"})


if __name__ == "__main__":
    unittest.main()
