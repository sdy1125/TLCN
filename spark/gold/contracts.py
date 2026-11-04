"""Physical contracts from Gold.pdf; technical columns support safe publication."""

ROOT = "s3://warehouse/gold"
VERSION = "gold-v3"

DIMENSIONS = {
    "dim_period": "period_key string, period_type string, period_code string, period_start_date date, period_end_date date, calendar_year int, calendar_quarter int, calendar_month int, calendar_day int, day_of_year int, crop_year_label string, days_in_period int, period_label string, period_sort_key string, is_complete_period boolean",
    "dim_commodity": "commodity_key string, commodity_code string, commodity_name string, commodity_name_vi string, commodity_group string, faostat_item_code string, faostat_item_name string, canonical_mass_unit string, is_active boolean",
    "dim_country": "country_key string, m49_code string, iso2_code string, iso3_code string, country_name string, country_name_vi string, country_type string, region_name string, income_group string, currency_code string, is_aggregate boolean, is_active boolean",
    "dim_geography": "geography_key string, geography_code string, geography_version string, geography_name string, geography_name_normalized string, geography_level string, parent_geography_code string, country_key string, province_code string, province_name string, district_code string, district_name string, valid_from date, valid_to date, is_current boolean",
    "dim_weather_location": "weather_location_key string, weather_location_code string, weather_provider string, grid_method string, latitude double, longitude double, elevation_m double, geography_key string, valid_from date, valid_to date, is_current boolean",
    "dim_price_series": "price_series_key string, series_code string, series_name string, price_type string, source_dataset_code string, market_scope string, commodity_variant string, native_currency_code string, native_unit_code string, canonical_unit_code string, index_base_label string, frequency_code string, valuation_basis string, valid_from date, valid_to date, is_current boolean",
    "dim_trade_product": "trade_product_key string, classification_system string, classification_version string, product_code string, product_name string, aggregation_level int, is_leaf boolean, is_active boolean",
    "dim_macro_indicator": "macro_indicator_key string, indicator_code string, indicator_name string, indicator_group string, unit_code string, scale_factor decimal(28,8), aggregation_rule string, value_type string, description string, is_active boolean",
}

BRIDGE = {
    "bridge_trade_product_commodity": "trade_product_key string, commodity_key string, mapping_type string, allocation_weight decimal(28,8), mapping_status string, mapping_rule string, valid_from date, valid_to date, is_current boolean",
}

FACT_COMMON = "observation_set_id string, record_source_code string, is_canonical boolean, loaded_at timestamp"
FACTS = {
    "fact_production_national": "production_national_key string, period_key string, country_key string, commodity_key string, area_harvested_ha decimal(28,8), production_tonnes decimal(28,8), yield_kg_ha decimal(28,8), yield_calculation_method string, area_flag_code string, production_flag_code string, yield_flag_code string, " + FACT_COMMON,
    "fact_production_provincial": "production_provincial_key string, period_key string, geography_key string, commodity_key string, coverage_code string, record_status_code string, area_planted_ha decimal(28,8), area_harvested_ha decimal(28,8), production_tonnes decimal(28,8), yield_kg_ha decimal(28,8), yield_calculation_method string, area_flag_code string, production_flag_code string, yield_flag_code string, " + FACT_COMMON,
    "fact_price": "price_key string, period_key string, commodity_key string, country_key string, geography_key string, price_series_key string, price_amount_native decimal(28,8), price_amount_canonical decimal(28,8), price_index_value decimal(28,8), value_flag_code string, " + FACT_COMMON,
    "fact_trade_total": "trade_total_key string, period_key string, reporter_country_key string, commodity_key string, trade_product_key string, trade_flow_code string, trade_quantity_tonnes decimal(28,8), trade_value_usd decimal(28,8), quantity_flag_code string, value_flag_code string, " + FACT_COMMON,
    "fact_trade_partner": "trade_partner_key string, period_key string, reporter_country_key string, partner_country_key string, commodity_key string, trade_product_key string, trade_flow_code string, trade_quantity_tonnes decimal(28,8), trade_value_usd decimal(28,8), quantity_estimated_flag boolean, value_estimated_flag boolean, " + FACT_COMMON,
    "fact_weather_daily": "weather_daily_key string, period_key string, weather_location_key string, temperature_avg_c double, temperature_max_c double, temperature_min_c double, relative_humidity_pct double, wind_speed_m_s double, precipitation_mm double, solar_radiation_mj_m2_day double, valid_measure_count int, has_missing_measure boolean, " + FACT_COMMON,
    "fact_coffee_stock": "coffee_stock_key string, period_key string, country_key string, commodity_key string, scenario_code string, ending_stock_thousand_bags decimal(28,8), bag_weight_kg decimal(28,8), ending_stock_tonnes decimal(28,8), " + FACT_COMMON,
    "fact_macro_indicator": "macro_indicator_fact_key string, period_key string, country_key string, macro_indicator_key string, indicator_value decimal(28,8), observation_status_code string, " + FACT_COMMON,
}

META = {
    "publish_batch": "gold_batch_id string, silver_batch_id string, policy_version string, transform_version string, input_snapshots_json string, output_snapshots_json string, row_counts_json string, dq_run_id string, status string, started_at timestamp, completed_at timestamp, error_summary string",
    "fact_lineage": "gold_batch_id string, fact_table string, fact_key string, metric_code string, observation_id string, silver_snapshot_id bigint, contribution_status string",
    "dq_result": "gold_batch_id string, table_name string, rule_code string, failed_count bigint, status string, checked_at timestamp",
}

def key(*parts):
    """Canonical, length-delimited serialization; NULL differs from empty text."""
    import hashlib
    import json

    return hashlib.sha256(json.dumps(parts, ensure_ascii=False,
                                     separators=(",", ":")).encode()).hexdigest()
