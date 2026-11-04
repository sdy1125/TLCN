"""Required Silver release checks and accepted warning categories."""

from .domains import DOMAIN


DOMAIN_RULES = (
    "NONEMPTY", "ATOMIC_ID_UNIQUE", "LINEAGE_COMPLETE",
    "STAGE_CORE_PARITY", "CANONICAL_AUDIT_COVERAGE",
)
CROSS_RULES = {
    ("silver_core.*", "OBSERVATION_SET_FK"),
    ("silver_core.price_observation", "AMOUNT_INDEX_XOR"),
    ("silver_core.coffee_stock_observation", "BAG_CONVERSION"),
    ("silver_core.weather_daily", "VALID_MEASURE_COUNT"),
    ("silver_core.weather_daily", "NO_SENTINEL_IN_MEASURES"),
    ("silver_core.macro_indicator_observation", "SOURCE_NULLS_RETAINED"),
    ("silver_meta.canonical_decision", "ONE_WINNER_MAX"),
    ("silver_meta.canonical_conflict", "OPEN_PRICE_CONFLICTS"),
    ("silver_meta.canonical_conflict", "OPEN_CONFLICT_NO_WINNER"),
    ("silver_core.provincial_agri_observation", "DISTRICT_KEY_PRESENT"),
    ("silver_core.provincial_agri_observation", "DISTRICT_REFERENCE_FK"),
    ("silver_ref.geography_alias", "PENDING_OFFICIAL_DISTRICT_MAPPING"),
}
META_RULES = {
    ("silver_meta.observation_set", "PUBLISHED_SET_COUNT"),
    ("silver_meta.observation_set", "SOURCE_RECORD_COUNT_PRESENT"),
    ("silver_meta.dataset_registry", "REGISTRY_COUNT"),
}
ACCEPTED_WARNING_RULES = {
    "OPEN_PRICE_CONFLICTS": "Conflicting prices have zero canonical winners",
    "PENDING_OFFICIAL_DISTRICT_MAPPING":
        "District rows retain source-scoped identity and are not official codes",
}


def required_checks():
    return (META_RULES | CROSS_RULES | {
        (f"lakehouse.silver_core.{domain}", rule)
        for domain in DOMAIN for rule in DOMAIN_RULES
    })


def evidence(rows):
    """Validate complete, unique, internally consistent DQ evidence."""
    result = {}
    for row in rows:
        value = row.asDict() if hasattr(row, "asDict") else row
        key = (value["object_name"], value["rule_code"])
        if key in result:
            raise ValueError("Duplicate Silver DQ rule result")
        result[key] = value
    if set(result) != required_checks():
        raise ValueError("Silver DQ evidence is incomplete or has unknown rules")
    for value in result.values():
        failed = int(value["failed_count"])
        threshold = float(value["threshold"])
        status = value["status"]
        severity = value["severity"]
        expected = ("FAIL" if severity == "BLOCK" and failed > threshold else
                    "WARN" if failed else "PASS")
        if status != expected:
            raise ValueError("Silver DQ status disagrees with observed count")
    if any(v["status"] == "FAIL" for v in result.values()):
        raise ValueError("Silver blocking DQ failed")
    warnings = {v["rule_code"]: int(v["failed_count"])
                for v in result.values() if v["status"] == "WARN"}
    if set(warnings) - set(ACCEPTED_WARNING_RULES):
        raise ValueError("Unreviewed Silver warning category")
    return warnings
