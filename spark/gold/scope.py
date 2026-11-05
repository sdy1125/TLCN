"""Versioned, rule-based coverage of every published Silver WINNER.

The classification exists only within a batch computation. DQ compares the
complete winner set with Fact lineage and approved exclusions; no per-row
status ledger is stored.
"""

import hashlib
import json
from pathlib import Path


CONTRACT_PATH = Path(__file__).with_name("scope_contract.json")
CONTRACT_BYTES = CONTRACT_PATH.read_bytes()
CONTRACT = json.loads(CONTRACT_BYTES)
SCOPE_VERSION = "scope-" + hashlib.sha256(CONTRACT_BYTES).hexdigest()[:16]


def validate_contract():
    if not CONTRACT.get("approved_by") or not CONTRACT.get("approved_on"):
        raise ValueError("Scope contract requires explicit approval provenance")
    from silver.domains import DOMAIN

    if set(CONTRACT["domains"]) != set(DOMAIN):
        raise ValueError("Scope contract must cover every Silver domain")
    for domain, rule in CONTRACT["domains"].items():
        datasets = rule["datasets"]
        if not datasets or len(datasets) != len(set(datasets)):
            raise ValueError(f"Invalid dataset list: {domain}")
        for field in ("grain", "metrics", "periods", "geography", "units",
                      "analysis_scope"):
            if not rule.get(field):
                raise ValueError(f"Missing {field} in scope contract: {domain}")
        if set(rule.get("approved_exclusions", {})) - set(datasets):
            raise ValueError(f"Exclusion outside scope: {domain}")
    return SCOPE_VERSION


def classify(domain, winners):
    """Return each winner once as INCLUDED, EXCLUDED_APPROVED or BLOCKED_UNMAPPED."""
    from pyspark.sql import functions as F

    validate_contract()
    rule = CONTRACT["domains"][domain]
    known = F.coalesce(F.col("dataset_id").isin(*rule["datasets"]), F.lit(False))
    status = F.when(~known, F.lit("BLOCKED_UNMAPPED"))
    reason = F.when(~known, F.lit("DATASET_NOT_IN_SCOPE_CONTRACT"))

    if domain == "price_observation":
        local = F.col("dataset_id") == "provincial_coffee_average_price"
        invalid = local & (F.col("geography_code").isNull() |
                           ~F.col("geography_code").isin("66", "68") |
                           (F.col("market_scope") != "PROVINCE"))
        status = status.when(invalid, "BLOCKED_UNMAPPED").when(
            local, "INCLUDED")
        reason = reason.when(invalid, "LOCAL_PRICE_GEOGRAPHY_MISSING").when(
            local, "SOURCE_PUBLISHED_PROVINCE_CODE")
    elif domain == "trade_total_observation":
        local_trade = F.col("dataset_id") == "provincial_coffee_export_volume_value"
        reported = local_trade & F.col("statistical_scope_code").eqNullSafe(
            F.lit("NATIONAL_REPORTED"))
        invalid = ((local_trade & ~reported) |
                   (~local_trade & ~F.col("statistical_scope_code").eqNullSafe(
                       F.lit("SOURCE_TOTAL"))))
        status = status.when(invalid, "BLOCKED_UNMAPPED")
        reason = reason.when(invalid, "UNREVIEWED_TRADE_SCOPE").when(
            reported, "SOURCE_NATIVE_NATIONAL_REPORTED")
    elif domain == "trade_partner_observation":
        faostat = ((F.col("classification_system") == "FAOSTAT") &
                    (F.col("statistical_scope_code") == "PARTNER_MATRIX") &
                    F.col("partner2_code").isNull() &
                    F.col("customs_code").isNull() &
                    F.col("mode_of_transport_code").isNull())
        hs = ((F.col("classification_system") == "HS") &
              F.col("dataset_id").startswith("un_comtrade_") &
              (F.col("partner2_code") == "0") &
              (F.col("customs_code") == "C00") &
              F.col("mode_of_transport_code").isNotNull())
        eligible = F.coalesce(faostat | hs, F.lit(False))
        status = status.when(~eligible, "BLOCKED_UNMAPPED")
        reason = reason.when(~eligible, "TRADE_PARTNER_POLICY_UNAPPROVED")
    elif domain == "coffee_stock_observation":
        eu = (F.col("country_m49_code").isNull() &
              F.coalesce(F.col("raw_payload_json").contains("European Union"),
                         F.lit(False)))
        missing = F.col("country_m49_code").isNull() & ~eu
        status = status.when(missing, "BLOCKED_UNMAPPED").when(
            eu, "EXCLUDED_APPROVED")
        reason = reason.when(missing, "STOCK_COUNTRY_UNMAPPED").when(
            eu, rule["approved_exclusions"]["provincial_coffee_world_ending_stocks"])
    return winners.withColumn(
        "_scope_status", status.otherwise(F.lit("INCLUDED"))
    ).withColumn("_scope_reason", reason)
