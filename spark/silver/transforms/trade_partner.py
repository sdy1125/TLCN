from ..common import RULES, commodity, country, fail, measure, number, time_fields


def boolean(raw):
    if raw in ("True", "true", "1"):
        return True
    if raw in ("False", "false", "0"):
        return False
    if raw in ("", None):
        return None
    fail("PARSE_ERROR", "Invalid boolean")


def transform(r, spec):
    rules = RULES["comtrade"]
    # Non-total transport/customs rows cannot fit this contract without double counting.
    if (
        r["partner2Code"] != rules["partner2"]
        or r["customsCode"] != rules["customs"]
        or r["motCode"] != rules["total_transport"]
    ):
        fail("UNSUPPORTED_GRAIN", "Non-total secondary partner/customs/transport")
    c, p = [
        RULES["comtrade_special"].get(r[k]) or country(r[k])
        for k in ["reporterCode", "partnerCode"]
    ]
    base = time_fields(
        r["refYear"],
        spec["frequency"],
        r["refMonth"] if spec["frequency"] == "monthly" else None,
    )
    expected = str(base["year"]) + (
        f"{base['month']:02d}" if spec["frequency"] == "monthly" else ""
    )
    if r["period"] != expected:
        fail("INVALID_PERIOD", "period conflicts with refYear/refMonth")
    flow = rules["flow"].get(r["flowCode"])
    if not flow:
        fail("UNKNOWN_FLOW", r["flowCode"])
    base.update(
        reporter_code=c["code"],
        reporter_m49=c["m49"],
        reporter_iso3=c["iso3"],
        partner_code=p["code"],
        partner_m49=p["m49"],
        partner_iso3=p["iso3"],
        commodity_code=commodity(r["cmdCode"]),
        hs_code=r["cmdCode"],
        flow_code=flow,
        classification_code=r["classificationCode"],
        transport_code=r["motCode"],
    )
    for field in [
        "isNetWgtEstimated",
        "isReported",
        "isAggregate",
        "isQtyEstimated",
        "isAltQtyEstimated",
        "isGrossWgtEstimated",
    ]:
        base[field] = boolean(r[field])
    field = next(
        (f for f in rules["value_preference"] if number(r[f]) is not None),
        "primaryValue",
    )
    q = dict(
        base,
        **measure(r["netWgt"], "kg", "trade_quantity"),
        is_estimated=base["isNetWgtEstimated"],
    )
    v = dict(
        base,
        **measure(r[field], "USD", "trade_value"),
        value_source_field=field,
        is_estimated=not base["isReported"],
    )
    for o in (q, v):
        if o["is_estimated"]:
            o["observation_status"] = "estimated"
    return [q, v]
