import unittest

from silver.runner import authorized, validate_token


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


if __name__ == "__main__":
    unittest.main()
