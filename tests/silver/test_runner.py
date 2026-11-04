import unittest
import tempfile
from pathlib import Path

from silver.runner import authorized, gold_input_matches, validate_token


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


if __name__ == "__main__":
    unittest.main()
