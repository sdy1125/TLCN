import csv
import importlib.util
import unittest
from collections import Counter
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from silver.common import (
    RowError,
    convert,
    number,
    period,
    province,
    commodity,
    stable_id,
    country,
)
from silver.registry import SOURCES, CONTRACTS, select_datasets
from silver.contracts import fields
from silver.transforms import rows

ROOT = Path(__file__).resolve().parents[2]


class RuleTests(unittest.TestCase):
    def test_inventory(self):
        with (ROOT / "data/raw_manifest.csv").open() as f:
            manifest = {r["dataset_id"]: r for r in csv.DictReader(f)}
        ids = set(manifest)
        self.assertEqual(ids, set(SOURCES))
        self.assertEqual(
            {dataset: row["frequency"] for dataset, row in manifest.items()},
            {dataset: spec["frequency"] for dataset, spec in SOURCES.items()},
        )
        self.assertEqual(
            Counter(s["target"] for s in SOURCES.values()),
            {
                "production_national": 3,
                "production_provincial": 3,
                "market_indicator": 10,
                "trade_partner": 5,
                "trade_total": 4,
                "weather_daily": 1,
            },
        )

    def test_unverified_weather_provenance_is_explicit(self):
        provenance = SOURCES["nasa_power_weather_daily_63"]["provenance"]
        self.assertEqual(provenance["source_parameter"], "ALLSKY_SFC_SW_DWN")
        self.assertEqual(provenance["unit_status"], "UNVERIFIED")
        self.assertIsNone(provenance["unit"])
        self.assertIsNone(provenance["source_community"])
        self.assertIsNone(provenance["source_time_standard"])

    def test_selection(self):
        self.assertEqual(len(select_datasets()), 26)
        self.assertEqual(select_datasets(["world_bank_wdi"] * 2), ["world_bank_wdi"])
        for value in [[], {}, 1, ["unknown"], [1]]:
            with self.assertRaises(ValueError):
                select_datasets(value)

    def test_periods(self):
        self.assertEqual(period("2024")[0], date(2024, 1, 1))
        self.assertEqual(period("202402", "monthly")[1], date(2024, 2, 29))
        self.assertEqual(period("2024M02", "monthly")[0], date(2024, 2, 1))
        self.assertEqual(period("2024", "daily", doy="366")[0], date(2024, 12, 31))
        self.assertEqual(period("Sơ bộ năm 2024")[-1], "provisional")
        self.assertEqual(period("Dự báo 2025/2026", "seasonal")[-1], "forecast")
        self.assertEqual(period("2024/2025", "seasonal")[4], "2024/2025")
        for args in [
            ("2023", "daily", None, "366"),
            ("2024", "daily", None, "0"),
            ("202413", "monthly"),
            ("2024/2026", "seasonal"),
            ("2024 junk",),
        ]:
            with self.assertRaises(RowError):
                period(*args)

    def test_conversions(self):
        for unit, expected in [
            ("Nghìn ha", 2000),
            ("nghìn tấn", 2000),
            ("triệu USD", 2000000),
            ("nghìn bao (60kg/bao)", 120),
            ("kg", Decimal(".002")),
        ]:
            self.assertEqual(convert("2", unit)[0], expected)
        self.assertEqual(convert("1", "LCU/tonne")[1], "LCU/tonne")
        self.assertEqual(convert("0", "t")[0], 0)
        with self.assertRaises(RowError):
            convert("-999", "t")
        with self.assertRaises(RowError):
            convert("1", "unknown")
        with self.assertRaises(RowError):
            convert("1e19", "triệu USD")

    def test_missing_and_invalid(self):
        for value in ["", None, "…", "...", "..", "N/A"]:
            self.assertIsNone(number(value))
        for value in ["oops", "NaN", "Infinity", "1e30"]:
            with self.assertRaises(RowError):
                number(value)
        self.assertEqual(number("-999"), -999)  # sentinel is source-specific

    def test_exact_mappings(self):
        self.assertEqual(commodity("0901"), "coffee")
        self.assertEqual(province("Đắc Nông")[0], "67")
        with self.assertRaises(RowError):
            commodity("0902")
        with self.assertRaises(RowError):
            province("made up")

    @unittest.skipUnless(
        importlib.util.find_spec("pycountry"),
        "country runtime dependency not installed on host",
    )
    def test_country(self):
        self.assertEqual(country("704")["iso3"], "VNM")
        self.assertEqual(country("VNM")["m49"], "704")
        self.assertEqual(country("230")["kind"], "historical")
        with self.assertRaises(RowError):
            country("made up")

    def test_stable_key(self):
        self.assertEqual(
            stable_id("a", {"x": 1, "y": None}), stable_id("a", {"y": None, "x": 1})
        )
        self.assertNotEqual(stable_id("a", [1]), stable_id("b", [1]))
        self.assertNotEqual(stable_id("a", ["a|b", "c"]), stable_id("a", ["a", "b|c"]))

    def test_consumer_contracts(self):
        for target in CONTRACTS:
            names = fields(target)
            self.assertTrue(set(CONTRACTS[target]["keys"]) <= names.keys())
            self.assertTrue(
                {"bronze_checksum_sha256", "source_record_id", "period_start"}
                <= names.keys()
            )
            self.assertFalse(any(c.endswith("_key") for c in names))
        self.assertTrue(
            {"reporter_m49", "partner_m49", "hs_code"} <= fields("trade_partner").keys()
        )
        self.assertTrue(
            {"temperature_avg_c", "precipitation_mm"} <= fields("weather_daily").keys()
        )

    def test_provincial_missing_and_error(self):
        d = "provincial_agriculture_three_commodities"
        spec = SOURCES[d]
        meta = dict(
            dataset_id=d,
            source_system="provincial",
            source_file_name="a.csv",
            bronze_path="s3://bronze/a",
            ingestion_id="id",
            checksum_sha256="a" * 64,
        )
        raw = dict(
            year="2024",
            province_name_raw="Đắk Nông",
            province_name_normalized="Đắk Nông",
            commodity="coffee",
            metric="production",
            value="",
            unit="tonnes",
            parse_status="missing_source_value",
            geographic_level="province",
            district="",
            edition="",
            geography_version="",
        )
        out = list(rows([raw], spec, meta, "v1", "run", datetime(2026, 1, 1)))[0]
        self.assertEqual(out["record"]["observation_status"], "missing")
        self.assertIsNone(out["record"]["value_standard"])
        raw["province_name_normalized"] = "unknown"
        out = list(rows([raw], spec, meta, "v1", "run", datetime(2026, 1, 1)))[0]
        self.assertEqual(out["error_code"], "UNKNOWN_PROVINCE")
        self.assertIsNone(out["record"])


if __name__ == "__main__":
    unittest.main()
