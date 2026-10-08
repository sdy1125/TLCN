from ..common import RULES, commodity, country, fail, measure, time_fields


def transform(r, spec):
    target = spec["target"]
    element = r["Element Code"]
    rule = RULES["elements"].get(element)
    if not rule:
        fail("UNKNOWN_INDICATOR", element)
    code, expected_unit, flow = rule
    unit = r["Unit"]
    if target == "market_indicator":
        unit = {
            "LCU": "LCU/tonne",
            "SLC": "SLC/tonne",
            "USD": "USD/tonne",
            "": "index_2014_2016",
        }.get(unit, unit)
    out = measure(r["Value"], unit, code)
    out["unit_raw"] = r["Unit"]
    if out["unit_standard"] != expected_unit:
        fail("UNIT_MISMATCH", element)
    allowed = {
        "production_national": {"5312", "5510", "5412"},
        "market_indicator": {"5530", "5531", "5532", "5539"},
        "trade_partner": {"5910", "5922", "5610", "5622"},
        "trade_total": {"5910", "5922", "5610", "5622"},
    }
    if element not in allowed[target]:
        fail("UNKNOWN_INDICATOR", element)
    month = None
    if spec["frequency"] == "monthly":
        try:
            month = str(int(r["Months Code"]) - 7000)
        except ValueError:
            fail("INVALID_PERIOD", "Invalid Months Code")
    out.update(time_fields(r["Year"], spec["frequency"], month))
    flag = r["Flag"]
    if flag not in RULES["status"]:
        fail("UNKNOWN_STATUS", flag)
    out["observation_status"] = RULES["status"][flag]
    out.update(
        commodity_code=commodity(r["Item Code (FAO)"]),
        faostat_item_code=r["Item Code (FAO)"],
        element_code=element,
    )
    if target in {"production_national", "market_indicator"}:
        c = country(r["Area Code (M49)"])
        out.update(
            country_code=c["code"],
            country_m49=c["m49"],
            country_iso3=c["iso3"],
            country_name_raw=r["Area"],
        )
        if target == "market_indicator":
            out.update(
                geo_code=c["code"],
                geo_type=c["kind"],
                geo_kind=c["kind"],
                indicator_code=code,
                market_type="producer",
                currency_code=unit.split("/")[0] if "/" in unit else None,
            )
    else:
        c = country(
            r["Reporter Country Code (M49)"]
            if target == "trade_partner"
            else r["Area Code (M49)"]
        )
        out.update(
            reporter_code=c["code"],
            reporter_m49=c["m49"],
            reporter_iso3=c["iso3"],
            flow_code=flow,
        )
        if target == "trade_partner":
            p = country(r["Partner Country Code (M49)"])
            out.update(
                partner_code=p["code"],
                partner_m49=p["m49"],
                partner_iso3=p["iso3"],
                is_estimated=flag in {"E", "I", "X"},
            )
    return [out]
