"""Bronze row to atomic Silver V2 observations; no canonical choice here."""

import json
import re
from datetime import date, datetime, timedelta
from decimal import Decimal

from silver.common import RULES, RowError, commodity, country, number, period, province
from silver.transforms import faostat, provincial
from .identity import observation_id, observation_set_id


COMMON = {
    "observation_id": "string", "dataset_id": "string", "observation_set_id": "string",
    "source_row_locator": "string", "ingestion_run_id": "string", "source_record_hash": "string",
    "metric_code": "string", "value_native": "decimal(28,8)", "native_unit_code": "string",
    "value_canonical": "decimal(28,8)", "canonical_unit_code": "string",
    "value_status": "string", "quality_flag_source": "string", "record_status_code": "string",
    "available_at": "timestamp", "quality_status": "string", "processed_at": "timestamp",
    "raw_payload_json": "string", "bronze_path": "string", "source_system": "string",
    "source_file": "string", "period_type": "string", "period_code": "string",
}
DOMAIN = {
    "production_observation": {
        "m49_code": "string", "faostat_item_code": "string", "commodity_code": "string",
        "calendar_year": "int", "element_code": "string", "element_name": "string",
    },
    "provincial_agri_observation": {
        "geography_code": "string", "geography_version": "string", "geography_level": "string",
        "province_code": "string", "district_code": "string", "raw_province": "string",
        "raw_district": "string", "commodity_code": "string", "calendar_year": "int",
        "coverage_code": "string", "coverage_end_date": "date", "source_scope_raw": "string",
    },
    "price_observation": {
        "series_code": "string", "commodity_code": "string", "country_m49_code": "string",
        "geography_code": "string", "market_scope": "string", "price_type": "string",
        "currency_code": "string", "price_amount_native": "decimal(28,8)",
        "price_amount_canonical": "decimal(28,8)", "price_index_value": "decimal(28,8)",
        "index_base_label": "string", "valuation_basis": "string",
    },
    "trade_total_observation": {
        "reporter_m49_code": "string", "classification_system": "string",
        "classification_version": "string", "product_code": "string", "commodity_code": "string",
        "mapping_status": "string", "trade_flow_code": "string", "statistical_scope_code": "string",
    },
    "trade_partner_observation": {
        "reporter_m49_code": "string", "partner_m49_code": "string", "partner2_code": "string",
        "customs_code": "string", "mode_of_transport_code": "string",
        "classification_system": "string", "classification_version": "string",
        "product_code": "string", "commodity_code": "string", "aggregation_level": "int",
        "is_leaf": "boolean", "is_aggregate": "boolean", "is_reported": "boolean",
        "trade_flow_code": "string", "statistical_scope_code": "string",
        "qty": "decimal(28,8)", "qty_unit_code": "string", "net_weight_kg": "decimal(28,8)",
        "gross_weight_kg": "decimal(28,8)", "primary_value_usd": "decimal(28,8)",
        "cif_value_usd": "decimal(28,8)", "fob_value_usd": "decimal(28,8)",
        "is_qty_estimated": "boolean", "is_net_weight_estimated": "boolean",
        "legacy_estimation_flag": "string",
    },
    "weather_daily": {
        "weather_location_code": "string", "observation_date": "date", "calendar_year": "int",
        "day_of_year": "int", "latitude": "double", "longitude": "double",
        "province_code_source": "string", "weather_provider": "string",
        "temperature_avg_c": "double", "temperature_max_c": "double", "temperature_min_c": "double",
        "relative_humidity_pct": "double", "wind_speed_m_s": "double",
        "precipitation_mm": "double", "solar_radiation_mj_m2_day": "double",
        "temperature_avg_valid": "boolean", "temperature_max_valid": "boolean",
        "temperature_min_valid": "boolean", "humidity_valid": "boolean", "wind_valid": "boolean",
        "precipitation_valid": "boolean", "solar_valid": "boolean",
        "valid_measure_count": "int", "has_missing_measure": "boolean",
        "solar_native_value": "double", "solar_native_unit": "string",
    },
    "coffee_stock_observation": {
        "country_m49_code": "string", "commodity_code": "string", "crop_year_code": "string",
        "crop_year_label": "string", "scenario_code": "string",
        "ending_stock_thousand_bags": "decimal(28,8)", "bag_weight_kg": "decimal(28,8)",
        "ending_stock_tonnes": "decimal(28,8)",
    },
    "macro_indicator_observation": {
        "country_m49_code": "string", "calendar_year": "int", "indicator_code": "string",
        "indicator_value_native": "decimal(28,8)", "indicator_value_canonical": "decimal(28,8)",
        "unit_code": "string", "scale_factor": "decimal(28,8)",
        "value_type": "string", "aggregation_rule": "string",
    },
}


def target(dataset):
    if dataset.startswith("faostat_production_"):
        return "production_observation"
    if dataset == "provincial_agriculture_three_commodities" or dataset in {
        "provincial_coffee_area", "provincial_coffee_production"}:
        return "provincial_agri_observation"
    if "producer_prices" in dataset or dataset in {
        "world_bank_pink_sheet", "provincial_coffee_average_price"}:
        return "price_observation"
    if "tcl_trade" in dataset or dataset == "provincial_coffee_export_volume_value":
        return "trade_total_observation"
    if "detailed_trade_matrix" in dataset or dataset.startswith("un_comtrade_"):
        return "trade_partner_observation"
    if dataset == "nasa_power_weather_daily_63":
        return "weather_daily"
    if dataset == "provincial_coffee_world_ending_stocks":
        return "coffee_stock_observation"
    if dataset == "world_bank_wdi":
        return "macro_indicator_observation"
    raise ValueError("Unmapped dataset: " + dataset)


def schema(table):
    from pyspark.sql import types as T

    types = {"string": T.StringType(), "int": T.IntegerType(), "date": T.DateType(),
             "timestamp": T.TimestampType(), "boolean": T.BooleanType(),
             "double": T.DoubleType(), "decimal(28,8)": T.DecimalType(28, 8)}
    return T.StructType([T.StructField(k, types[v], True)
                         for k, v in {**COMMON, **DOMAIN[table]}.items()])


def _decimal(raw):
    v = number(raw)
    return v.quantize(Decimal("0.00000001")) if v is not None else None


def _boolean(raw):
    if raw in ("True", "true", "1"):
        return True
    if raw in ("False", "false", "0"):
        return False
    return None


def _time(year, frequency, month=None):
    start, _, y, m, crop, status = period(year, frequency, month)
    kind = {"annual": "YEAR", "monthly": "MONTH", "seasonal": "CROP_YEAR"}[frequency]
    return kind, crop or (f"{y:04d}-{m:02d}" if m else f"{y:04d}"), y, status


def _base(raw, metadata, run_id, table, metric, now):
    dataset = metadata["dataset_id"]
    release = metadata.get("source_modified_time") or metadata["ingestion_id"]
    set_id = observation_set_id(dataset, release, metadata["checksum_sha256"])
    locator = raw["_source_row_locator"]
    retrieved = datetime.fromisoformat(metadata["ingested_at"]).replace(tzinfo=None)
    return dict(observation_id=observation_id(dataset, set_id, locator, metric),
                dataset_id=dataset, observation_set_id=set_id, source_row_locator=locator,
                ingestion_run_id=run_id, source_record_hash=raw["_source_record_hash"],
                metric_code=metric, value_status="VALID", record_status_code="FINAL",
                quality_status="VALID", available_at=retrieved, processed_at=now,
                raw_payload_json=raw["_raw_payload_json"], bronze_path=metadata["bronze_path"],
                source_system=metadata["source_system"], source_file=metadata["source_file_name"])


def transform(raw, spec, metadata, run_id, now):
    dataset = metadata["dataset_id"]
    table = target(dataset)
    if table == "weather_daily":
        return _weather(raw, metadata, run_id, now)
    if table == "coffee_stock_observation":
        return _stock(raw, metadata, run_id, now)
    if table == "macro_indicator_observation":
        return _macro(raw, metadata, run_id, now)
    if dataset == "world_bank_pink_sheet":
        return _pink(raw, metadata, run_id, now)
    if dataset.startswith("un_comtrade_"):
        return _comtrade(raw, spec, metadata, run_id, now)
    if dataset.startswith("faostat_"):
        return _faostat(raw, spec, metadata, run_id, now)
    return _provincial(raw, spec, metadata, run_id, now)


def _faostat(r, spec, m, run_id, now):
    table = target(m["dataset_id"])
    old = faostat.transform(r, spec)[0]
    metric = old["measure_code"].upper()
    base = _base(r, m, run_id, table, metric, now)
    base.update(period_type="MONTH" if spec["frequency"] == "monthly" else "YEAR",
                period_code=old["period_start"].strftime("%Y-%m") if spec["frequency"] == "monthly" else str(old["year"]),
                value_native=_decimal(r["Value"]), native_unit_code=r["Unit"],
                value_canonical=old["value_standard"], canonical_unit_code=old["unit_standard"],
                quality_flag_source=r["Flag"],
                value_status="MISSING" if old["value_standard"] is None else "ESTIMATED" if old["observation_status"] == "estimated" else "VALID")
    if table == "production_observation":
        base.update(m49_code=old["country_m49"], faostat_item_code=old["faostat_item_code"],
                    commodity_code=old["commodity_code"].upper(), calendar_year=old["year"],
                    element_code=old["element_code"], element_name=r["Element"])
    elif table == "price_observation":
        index = metric == "PRODUCER_PRICE_INDEX"
        currency = r["Unit"] if r["Unit"] in {"USD", "LCU", "SLC"} else None
        base.update(series_code=f"FAOSTAT:{m['dataset_id']}:{old['element_code']}:{old['country_m49']}:{spec['frequency']}",
                    commodity_code=old["commodity_code"].upper(), country_m49_code=old["country_m49"],
                    market_scope="COUNTRY", price_type="PRICE_INDEX" if index else "PRODUCER",
                    currency_code=currency, price_amount_native=None if index else _decimal(r["Value"]),
                    price_amount_canonical=None if index or currency != "USD" else old["value_standard"],
                    price_index_value=_decimal(r["Value"]) if index else None,
                    index_base_label="2014-2016" if index else None,
                    valuation_basis=r["Element"])
        if currency != "USD" or index:
            base["value_canonical"] = None if currency != "USD" else base["value_canonical"]
            base["canonical_unit_code"] = None if currency != "USD" else base["canonical_unit_code"]
    else:
        product = old["faostat_item_code"]
        base.update(reporter_m49_code=old["reporter_m49"], classification_system="FAOSTAT",
                    classification_version="AS_PUBLISHED", product_code=product,
                    commodity_code=old["commodity_code"].upper(), trade_flow_code=old["flow_code"].upper(),
                    statistical_scope_code="SOURCE_TOTAL", mapping_status="APPROVED_1_TO_1")
        if table == "trade_partner_observation":
            base.update(partner_m49_code=old["partner_m49"],
                        is_reported=r["Flag"] == "A", is_aggregate=False,
                        statistical_scope_code="PARTNER_MATRIX")
    return [base]


def _provincial(r, spec, m, run_id, now):
    table = target(m["dataset_id"])
    old = provincial.transform(r, spec, m["dataset_id"])
    out = []
    for o in old:
        metric = o["measure_code"].upper()
        base = _base(r, m, run_id, table, metric, now)
        base.update(period_type="MONTH" if spec["frequency"] == "monthly" else "YEAR",
                    period_code=o["period_start"].strftime("%Y-%m") if spec["frequency"] == "monthly" else str(o["year"]),
                    value_native=_decimal(o["raw_value"]), native_unit_code=o["unit_raw"],
                    value_canonical=o["value_standard"], canonical_unit_code=o["unit_standard"],
                    value_status="MISSING" if o["value_standard"] is None else "VALID")
        if table == "provincial_agri_observation":
            scope = r.get("source_scope", "").lower()
            coverage = "SMALLHOLDER_ONLY" if "smallholder" in scope else "UNKNOWN" if scope else "ALL_PRODUCERS"
            level = (o["geographic_level"] or "").upper()
            base.update(geography_code=o["province_code"] if level == "PROVINCE" else None,
                        geography_version=o["geography_version"], geography_level=level,
                        province_code=o["province_code"], district_code=None,
                        raw_province=o["province_name_raw"], raw_district=o.get("district_name"),
                        commodity_code=o["commodity_code"].upper(), calendar_year=o["year"],
                        coverage_code=coverage, source_scope_raw=r.get("source_scope"))
            if level == "DISTRICT" or coverage == "UNKNOWN":
                base["quality_status"] = "WARN"
        elif table == "price_observation":
            base["observation_id"] = observation_id(
                m["dataset_id"], base["observation_set_id"],
                r["_source_row_locator"], f"{metric}:{o['province_code']}"
            )
            base.update(series_code=f"LOCAL:COFFEE:{o['province_code']}:MONTH:VND/KG",
                        commodity_code="COFFEE", geography_code=o["province_code"],
                        market_scope="PROVINCE", price_type="LOCAL", currency_code="VND",
                        price_amount_native=_decimal(o["raw_value"]),
                        price_amount_canonical=o["value_standard"], valuation_basis="COFFEE_GREEN")
        else:
            base.update(reporter_m49_code="704", classification_system="LOCAL_REPORTED",
                        classification_version="AS_PUBLISHED", product_code="COFFEE",
                        commodity_code="COFFEE", mapping_status="SOURCE_DECLARED",
                        trade_flow_code="EXPORT", statistical_scope_code="NATIONAL_REPORTED")
        out.append(base)
    return out


def _pink(r, m, run_id, now):
    if r["source_sheet"] != "Monthly Prices" or not re.fullmatch(r"\d{4}M\d{2}", r["column_0"]):
        return []
    y, month = int(r["column_0"][:4]), int(r["column_0"][-2:])
    date(y, month, 1)
    out = []
    for field, (_, commodity_code, series) in RULES["pink"].items():
        metric = series.upper()
        value = _decimal(r[field])
        base = _base(r, m, run_id, "price_observation", metric, now)
        base.update(period_type="MONTH", period_code=f"{y:04d}-{month:02d}",
                    value_native=value, native_unit_code="USD/kg", value_canonical=value,
                    canonical_unit_code="USD/kg", value_status="MISSING" if value is None else "VALID",
                    series_code=f"PINK:{series}:MONTH:USD/KG", commodity_code=commodity_code.upper(),
                    country_m49_code="001", market_scope="GLOBAL", price_type="WORLD",
                    currency_code="USD", price_amount_native=value, price_amount_canonical=value,
                    valuation_basis=series)
        out.append(base)
    return out


def _stock(r, m, run_id, now):
    raw_unit = r["ĐVT"]
    if raw_unit != "nghìn bao (60kg/bao)":
        raise RowError("UNVERIFIED_BAG_WEIGHT", raw_unit)
    _, crop, _, status = _time(r["Năm"], "seasonal")
    value = _decimal(r["Giá_trị"])
    c = country(r["Quốc gia"])
    base = _base(r, m, run_id, "coffee_stock_observation", "ENDING_STOCK", now)
    base.update(period_type="CROP_YEAR", period_code=crop, value_native=value,
                native_unit_code=raw_unit, value_canonical=value * Decimal(60) if value is not None else None,
                canonical_unit_code="tonne", value_status="MISSING" if value is None else "VALID",
                record_status_code="FORECAST" if status == "forecast" else "PRELIMINARY" if status == "provisional" else "FINAL",
                country_m49_code=c["m49"], commodity_code="COFFEE", crop_year_code=crop,
                crop_year_label=r["Năm"], scenario_code="FORECAST" if status == "forecast" else "ACTUAL",
                ending_stock_thousand_bags=value, bag_weight_kg=Decimal(60),
                ending_stock_tonnes=value * Decimal(60) if value is not None else None)
    return [base]


def _macro(r, m, run_id, now):
    code = r["indicator.id"]
    if code not in RULES["wdi"]:
        raise RowError("UNKNOWN_INDICATOR", code)
    c = country(r["countryiso3code"])
    _, p, year, _ = _time(r["date"], "annual")
    value = _decimal(r["value"])
    unit = RULES["wdi"][code][0]
    base = _base(r, m, run_id, "macro_indicator_observation", code, now)
    base.update(period_type="YEAR", period_code=p, value_native=value,
                native_unit_code=r["unit"] or unit, value_canonical=value,
                canonical_unit_code=unit, value_status="MISSING" if value is None else "VALID",
                country_m49_code=c["m49"], calendar_year=year, indicator_code=code,
                indicator_value_native=value, indicator_value_canonical=value,
                unit_code=unit, scale_factor=Decimal(1),
                value_type="PERCENT" if unit == "%" else "NUMBER", aggregation_rule="NO_SUM")
    return [base]


def _comtrade(r, spec, m, run_id, now):
    year = int(r["refYear"])
    month = int(r["refMonth"]) if spec["frequency"] == "monthly" else None
    kind, p, _, _ = _time(str(year), spec["frequency"], str(month) if month else None)
    if r["period"] != p.replace("-", ""):
        raise RowError("INVALID_PERIOD", "Comtrade period disagreement")
    flow = RULES["comtrade"]["flow"].get(r["flowCode"])
    if not flow:
        raise RowError("UNKNOWN_FLOW", r["flowCode"])
    scope = f"partner2={r['partner2Code']}|customs={r['customsCode']}|mot={r['motCode']}"
    net = _decimal(r["netWgt"])
    primary = _decimal(r["primaryValue"])
    values = [("TRADE_QUANTITY", net, "kg", net / Decimal(1000) if net is not None else None, "tonne"),
              ("TRADE_VALUE", primary, "USD", None, None)]
    out = []
    for metric, native, unit, canonical, canon_unit in values:
        base = _base(r, m, run_id, "trade_partner_observation", metric, now)
        base.update(period_type=kind, period_code=p, value_native=native,
                    native_unit_code=unit, value_canonical=canonical,
                    canonical_unit_code=canon_unit, value_status="MISSING" if native is None else "ESTIMATED" if _boolean(r["isNetWgtEstimated"] if metric == "TRADE_QUANTITY" else "False") else "VALID",
                    quality_status="WARN", reporter_m49_code=r["reporterCode"].zfill(3),
                    partner_m49_code=r["partnerCode"].zfill(3), partner2_code=r["partner2Code"],
                    customs_code=r["customsCode"], mode_of_transport_code=r["motCode"],
                    classification_system="HS", classification_version=r["classificationCode"],
                    product_code=r["cmdCode"], commodity_code=None,
                    aggregation_level=int(r["aggrLevel"]) if r["aggrLevel"].isdigit() else None,
                    is_leaf=_boolean(r["isLeaf"]), is_aggregate=_boolean(r["isAggregate"]),
                    is_reported=_boolean(r["isReported"]), trade_flow_code=flow.upper(),
                    statistical_scope_code=scope, qty=_decimal(r["qty"]),
                    qty_unit_code=r["qtyUnitCode"], net_weight_kg=net,
                    gross_weight_kg=_decimal(r["grossWgt"]), primary_value_usd=primary,
                    cif_value_usd=_decimal(r["cifvalue"]), fob_value_usd=_decimal(r["fobvalue"]),
                    is_qty_estimated=_boolean(r["isQtyEstimated"]),
                    is_net_weight_estimated=_boolean(r["isNetWgtEstimated"]),
                    legacy_estimation_flag=r["legacyEstimationFlag"])
        out.append(base)
    return out


def _weather(r, m, run_id, now):
    year, doy = int(r["YEAR"]), int(r["DOY"])
    day = date(year, 1, 1) + timedelta(days=doy - 1)
    if day.year != year or doy < 1:
        raise RowError("INVALID_PERIOD", r["DOY"])
    fields = {
        "T2M": ("temperature_avg_c", "temperature_avg_valid"),
        "T2M_MAX": ("temperature_max_c", "temperature_max_valid"),
        "T2M_MIN": ("temperature_min_c", "temperature_min_valid"),
        "RH2M": ("relative_humidity_pct", "humidity_valid"),
        "WS10M": ("wind_speed_m_s", "wind_valid"),
        "PRECTOTCORR": ("precipitation_mm", "precipitation_valid"),
        "ALLSKY_SFC_SW_DWN": ("solar_native_value", "solar_valid"),
    }
    base = _base(r, m, run_id, "weather_daily", "WEATHER_DAILY", now)
    base.update(period_type="DAY", period_code=day.isoformat(),
                weather_location_code=r["weather_point"], observation_date=day,
                calendar_year=year, day_of_year=doy, latitude=float(r["latitude"]),
                longitude=float(r["longitude"]), province_code_source=r["province_code_63"].zfill(2),
                weather_provider="NASA_POWER", solar_native_unit="SOURCE_UNVERIFIED",
                quality_status="WARN")
    missing = 0
    for source, (field, flag) in fields.items():
        raw = r[source]
        value = None if raw in {"", "-999", "-999.0"} else float(raw)
        if value is not None:
            _, lower, upper = RULES["weather"][source]
            if not lower <= value <= upper:
                raise RowError("IMPOSSIBLE_VALUE", source)
        base[field] = value
        base[flag] = value is not None
        missing += value is None
    if all(base[k] is not None for k in ("temperature_min_c", "temperature_avg_c", "temperature_max_c")):
        if not base["temperature_min_c"] <= base["temperature_avg_c"] <= base["temperature_max_c"]:
            raise RowError("IMPOSSIBLE_VALUE", "Temperature ordering")
    base["solar_radiation_mj_m2_day"] = None
    base["valid_measure_count"] = len(fields) - missing
    base["has_missing_measure"] = missing > 0
    return [base]
