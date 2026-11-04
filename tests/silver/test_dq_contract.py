import unittest

from silver.dq_contract import evidence, required_checks


def rows(warnings=None):
    warnings = warnings or {}
    return [dict(object_name=table, rule_code=rule,
                 severity="WARN" if rule in warnings else "BLOCK",
                 failed_count=warnings.get(rule, 0), threshold=0.0,
                 status="WARN" if rule in warnings else "PASS")
            for table, rule in sorted(required_checks())]


class SilverDQContractTest(unittest.TestCase):
    def test_zero_exception_release_is_allowed(self):
        self.assertEqual(evidence(rows()), {})

    def test_warning_counts_are_batch_specific(self):
        for count in (1, 6, 17):
            self.assertEqual(evidence(rows({"OPEN_PRICE_CONFLICTS": count})),
                             {"OPEN_PRICE_CONFLICTS": count})

    def test_missing_rule_blocks_release(self):
        with self.assertRaisesRegex(ValueError, "incomplete"):
            evidence(rows()[:-1])

    def test_inconsistent_status_blocks_release(self):
        result = rows()
        result[0]["status"] = "WARN"
        with self.assertRaisesRegex(ValueError, "disagrees"):
            evidence(result)
