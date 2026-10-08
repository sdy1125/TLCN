from ..common import RULES, commodity, country, fail, measure, province, time_fields


def transform(r, spec, dataset):
    if dataset == "provincial_agriculture_three_commodities":
        metric = r["metric"]
        if metric not in {"production", "area_planted", "area_harvested"}:
            fail("UNKNOWN_INDICATOR", metric)
        missing = r["parse_status"] == "missing_source_value"
        o = measure(None if missing else r["value"], r["unit"], metric)
        o.update(time_fields(r["year"], "annual"))
        if missing:
            o["observation_status"] = "missing"
        p, name = province(r["province_name_normalized"])
        level = r["geographic_level"]
        if level not in {"province", "district"} or (
            level == "district" and not r["district"]
        ):
            fail("MISSING_KEY", "geography")
        o.update(
            province_code=p,
            province_name_raw=r["province_name_raw"],
            province_name_normalized=name,
            district_name=r["district"] or None,
            geographic_level=level,
            geography_version=r["geography_version"] or RULES["geography_default"],
            edition=r["edition"] or RULES["edition_default"],
            parse_status=r["parse_status"],
            commodity_code=commodity(r["commodity"]),
        )
        return [o]
    if dataset in {"provincial_coffee_area", "provincial_coffee_production"}:
        p, name = province(r["Địa phương"])
        metric = "area_planted" if dataset.endswith("_area") else "production"
        o = measure(r["Giá_trị"], r["ĐVT"], metric)
        o.update(time_fields(r["Năm"], "annual"))
        o.update(
            province_code=p,
            province_name_raw=r["Địa phương"],
            province_name_normalized=name,
            geographic_level="province",
            geography_version=RULES["geography_default"],
            edition=RULES["edition_default"],
            commodity_code="coffee",
            parse_status="parsed_workbook",
        )
        return [o]
    if dataset == "provincial_coffee_average_price":
        result = []
        for field, name in [("Giá Đăk Lăk", "Đắk Lắk"), ("Giá Lâm Đồng", "Lâm Đồng")]:
            p, _ = province(name)
            o = measure(r[field], r["ĐVT"], "domestic_coffee_price")
            o.update(time_fields(r["Năm"], "monthly", r["Tháng"]))
            o.update(
                province_code=p,
                geo_code=p,
                geo_type="province",
                commodity_code="coffee",
                indicator_code="domestic_coffee_price",
                market_type="domestic",
                currency_code="VND",
            )
            result.append(o)
        return result
    if dataset == "provincial_coffee_export_volume_value":
        result = []
        for field, unit, metric in [
            ("Lượng (nghìn tấn)", "nghìn tấn", "trade_quantity"),
            ("Kim Ngạch (triệu USD)", "triệu USD", "trade_value"),
        ]:
            o = measure(r[field], unit, metric)
            o.update(time_fields(r["Năm"], "monthly", r["Tháng"]))
            o.update(
                reporter_code="704",
                reporter_m49="704",
                reporter_iso3="VNM",
                commodity_code="coffee",
                flow_code="export",
            )
            result.append(o)
        return result
    c = country(r["Quốc gia"])
    o = measure(r["Giá_trị"], r["ĐVT"], "ending_stocks")
    o.update(time_fields(r["Năm"], "seasonal"))
    o.update(
        geo_code=c["code"],
        geo_type=c["kind"],
        country_iso3=c["iso3"],
        commodity_code="coffee",
        indicator_code="ending_stocks",
        market_type="stock",
        quality_flag="season_calendar_envelope",
    )
    return [o]
