"""Complete source-derived reference rows without approving ambiguous mappings."""

import json
from datetime import date
from decimal import Decimal

import pycountry
from pyspark.sql import SparkSession, functions as F

from silver_v2.foundation import _upsert, create_tables
from silver_v2.domains import DOMAIN


def dates(kind, code):
    if kind == "DAY":
        d = date.fromisoformat(code)
        return d, d, d.year, d.month, None
    if kind == "MONTH":
        y, m = map(int, code.split("-"))
        import calendar
        return date(y, m, 1), date(y, m, calendar.monthrange(y, m)[1]), y, m, None
    if kind == "YEAR":
        y = int(code)
        return date(y, 1, 1), date(y, 12, 31), y, None, None
    if kind == "CROP_YEAR":
        # The source label is known; agricultural start/end boundaries are not.
        return None, None, int(code[:4]), None, code
    raise ValueError((kind, code))


def main():
    spark = (SparkSession.builder.appName("silver-v2-reference-seed")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        create_tables(spark)
        countries = [dict(m49_code=c.numeric, iso2_code=c.alpha_2, iso3_code=c.alpha_3,
                          country_name=c.name, country_type="COUNTRY", is_aggregate=False,
                          source_code="ISO_3166")
                     for c in pycountry.countries if hasattr(c, "numeric")]
        countries.append(dict(m49_code="001", country_name="World", country_type="WORLD",
                              is_aggregate=True, source_code="SOURCE_AGGREGATE"))
        _upsert(spark, "lakehouse.silver_ref.country", countries, ["m49_code"])

        all_periods = None
        metrics = []
        for domain in DOMAIN:
            frame = spark.table(f"lakehouse.silver_core.{domain}")
            periods = frame.select("period_type", "period_code").distinct()
            all_periods = periods if all_periods is None else all_periods.unionByName(periods)
            for row in frame.select("metric_code", "native_unit_code", "canonical_unit_code").distinct().collect():
                metrics.append(dict(metric_code=row[0], business_process=domain,
                                    native_unit=row[1], canonical_unit=row[2],
                                    value_type="INDEX" if "INDEX" in (row[0] or "") else "MEASURE",
                                    aggregation_rule="NO_SUM_UNTIL_REVIEWED",
                                    nullable_policy="RETAIN_SOURCE_NULL"))
        _upsert(spark, "lakehouse.silver_ref.metric_definition", metrics,
                ["metric_code", "business_process", "native_unit", "canonical_unit"])
        rows = []
        for row in all_periods.distinct().collect():
            start, end, year, month, crop = dates(row[0], row[1])
            rows.append(dict(period_type=row[0], period_code=row[1], start_date=start,
                             end_date=end, calendar_year=year, calendar_month=month,
                             crop_year_label=crop))
        _upsert(spark, "lakehouse.silver_ref.period", rows,
                ["period_type", "period_code"])

        prices = spark.table("lakehouse.silver_core.price_observation")
        series = [dict(series_code=r[0], dataset_id=r[1], commodity_code=r[2],
                       price_type=r[3], market_scope=r[4], native_currency=r[5],
                       native_unit=r[6], canonical_unit=r[7], frequency=r[8],
                       index_base=r[9], valuation_basis=r[10])
                  for r in prices.select("series_code", "dataset_id", "commodity_code",
                                         "price_type", "market_scope", "currency_code",
                                         "native_unit_code", "canonical_unit_code",
                                         "period_type", "index_base_label",
                                         "valuation_basis").distinct().collect()]
        _upsert(spark, "lakehouse.silver_ref.price_series", series,
                ["series_code", "dataset_id"])

        trade = spark.table("lakehouse.silver_core.trade_partner_observation")
        products = [dict(classification_system=r[0], classification_version=r[1],
                         product_code=r[2], product_name=r[2], aggregation_level=r[3],
                         is_leaf=r[4])
                    for r in trade.select("classification_system", "classification_version",
                                          "product_code", "aggregation_level", "is_leaf")
                                  .distinct().collect()]
        _upsert(spark, "lakehouse.silver_ref.trade_product", products,
                ["classification_system", "classification_version", "product_code"])
        bridges = [dict(classification_system="FAOSTAT", classification_version="AS_PUBLISHED",
                        product_code=item, commodity_code=commodity,
                        mapping_type="SOURCE_ITEM", allocation_weight=Decimal(1),
                        mapping_status="APPROVED", mapping_rule="FAOSTAT_ITEM_REGISTRY")
                   for item, commodity in [("656", "COFFEE"), ("836", "RUBBER"), ("687", "PEPPER")]]
        for version in [r[0] for r in trade.filter("classification_system='HS'")
                        .select("classification_version").distinct().collect()]:
            for code, commodity in [("0901", "COFFEE"), ("0904", "PEPPER")]:
                bridges.append(dict(classification_system="HS", classification_version=version,
                                    product_code=code, commodity_code=commodity,
                                    mapping_type="CANDIDATE", allocation_weight=None,
                                    mapping_status="PENDING", mapping_rule="NO_AUTO_ALLOCATION"))
        _upsert(spark, "lakehouse.silver_ref.trade_product_commodity_bridge", bridges,
                ["classification_system", "classification_version", "product_code", "commodity_code"])

        districts = spark.table("lakehouse.silver_core.provincial_agri_observation")\
            .filter("geography_level='DISTRICT'")\
            .select("dataset_id", "raw_district", "province_code").distinct().collect()
        pending = [dict(source_dataset=r[0], raw_name=r[1], raw_code=None,
                        normalized_name=r[1], geography_code=None, confidence=0.0,
                        approval_status="PENDING") for r in districts]
        _upsert(spark, "lakehouse.silver_ref.geography_alias", pending,
                ["source_dataset", "raw_name"])
        print(json.dumps({"countries": len(countries), "periods": len(rows),
                          "metric_definitions": len(metrics), "price_series": len(series),
                          "trade_products": len(products), "trade_bridges": len(bridges),
                          "pending_district_aliases": len(pending)}), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
