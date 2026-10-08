from ..common import RULES, country, fail, measure, time_fields


def transform(r, spec, dataset):
    if dataset == "world_bank_wdi":
        code = r["indicator.id"]
        rule = RULES["wdi"].get(code)
        if not rule:
            fail("UNKNOWN_INDICATOR", code)
        c = country(r["countryiso3code"])
        o = measure(r["value"], rule[0], code, rule[1])
        o.update(time_fields(r["date"], "annual"))
        o["unit_raw"] = r["unit"]
        o.update(
            geo_code=c["code"],
            geo_type=c["kind"],
            country_iso3=c["iso3"],
            indicator_code=code,
            indicator_name_raw=r["indicator.value"],
            market_type="macro",
            currency_code="USD" if rule[0] == "USD" else None,
        )
        return [o]
    # Header/unit/source-sheet filtering is performed and audited in the reader.
    result = []
    for field, (_, comm, indicator) in RULES["pink"].items():
        o = measure(r[field], spec["pink_units"][field], indicator)
        o.update(time_fields(r["column_0"], "monthly"))
        o.update(
            geo_code="001",
            geo_type="world",
            commodity_code=comm,
            indicator_code=indicator,
            market_type="international_benchmark",
            currency_code="USD",
        )
        result.append(o)
    return result
