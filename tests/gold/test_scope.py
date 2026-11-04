import unittest

from gold.scope import CONTRACT, SCOPE_VERSION, validate_contract


class ScopeContractTest(unittest.TestCase):
    def test_all_domains_have_approved_scope(self):
        self.assertEqual(validate_contract(), SCOPE_VERSION)
        self.assertEqual(len(CONTRACT["domains"]), 8)

    def test_known_exclusions_have_reasons(self):
        price = CONTRACT["domains"]["price_observation"]["approved_exclusions"]
        trade = CONTRACT["domains"]["trade_total_observation"]["approved_exclusions"]
        stock = CONTRACT["domains"]["coffee_stock_observation"]["approved_exclusions"]
        self.assertEqual(set(price), {"provincial_coffee_average_price"})
        self.assertEqual(set(trade), {"provincial_coffee_export_volume_value"})
        self.assertTrue(all(price.values()) and all(trade.values()))
        self.assertEqual(set(stock), {"provincial_coffee_world_ending_stocks"})
