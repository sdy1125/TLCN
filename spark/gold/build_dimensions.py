"""Build conformed Gold dimensions from a pinned published Silver release."""

from pyspark.sql import functions as F

from .contracts import DIMENSIONS, BRIDGE, ensure_storage
from .source import guard_existing_publication, pinned, resolve
from silver.common import GEO, RULES


def hashed(*parts):
    return F.sha2(F.to_json(F.array(*[F.col(part).cast('string') if isinstance(part, str)
                                     else part.cast('string') for part in parts]),
                            {'ignoreNullFields': 'false'}), 256)


def insert_new(spark, namespace, name, frame, key_columns):
    table = f'lakehouse.{namespace}.{name}'
    target = spark.table(table)
    frame = frame.select(*target.columns)
    duplicate = frame.groupBy(*key_columns).count().filter('count > 1').limit(1).count()
    if duplicate:
        raise ValueError(f'Duplicate business key in {name}')
    source_alias = frame.alias('source')
    target_alias = target.select(*key_columns).alias('target')
    condition = None
    for column in key_columns:
        part = F.col(f'source.{column}').eqNullSafe(F.col(f'target.{column}'))
        condition = part if condition is None else condition & part
    missing = source_alias.join(target_alias, condition, 'left_anti')
    count = missing.count()
    if count:
        missing.writeTo(table).append()
    return count


def build(spark):
    batch, snapshots = resolve(spark)
    ensure_storage(spark)
    guard_existing_publication(spark, batch['batch_id'])
    ref = lambda name: pinned(spark, f'lakehouse.silver_ref.{name}', snapshots)
    lit = F.lit
    period = ref('period').select(
        hashed('period_type', 'period_code').alias('period_key'),
        'period_type', 'period_code', F.col('start_date').alias('period_start_date'),
        F.col('end_date').alias('period_end_date'), 'calendar_year',
        F.when(F.col('calendar_month').isNotNull(),
               F.quarter('start_date')).alias('calendar_quarter'),
        'calendar_month', F.when(F.col('period_type') == 'DAY', F.dayofmonth('start_date')).alias('calendar_day'),
        F.when(F.col('period_type') == 'DAY', F.dayofyear('start_date')).alias('day_of_year'),
        'crop_year_label', F.when(F.col('start_date').isNotNull() & F.col('end_date').isNotNull(),
                                  F.datediff('end_date', 'start_date') + 1).alias('days_in_period'),
        F.col('period_code').alias('period_label'),
        F.col('period_code').alias('period_sort_key'), lit(None).cast('boolean').alias('is_complete_period'))
    commodity = ref('commodity').select(
        hashed('commodity_code').alias('commodity_key'), 'commodity_code', 'commodity_name',
        lit(None).cast('string').alias('commodity_name_vi'), 'commodity_group',
        'faostat_item_code', lit(None).cast('string').alias('faostat_item_name'),
        lit('tonne').alias('canonical_mass_unit'), 'is_active')
    country = ref('country').select(
        hashed('m49_code').alias('country_key'), 'm49_code', 'iso2_code', 'iso3_code',
        'country_name', lit(None).cast('string').alias('country_name_vi'),
        'country_type', lit(None).cast('string').alias('region_name'),
        lit(None).cast('string').alias('income_group'),
        lit(None).cast('string').alias('currency_code'), 'is_aggregate', lit(True).alias('is_active'))
    historical = [code for code, item in RULES['country_special'].items()
                  if item.get('kind') == 'historical' and item.get('m49')]
    historical_rows = []
    for code in historical:
        historical_rows.append(dict(country_key=None, m49_code=code, iso2_code=None,
                                    iso3_code=None, country_name=code, country_name_vi=None,
                                    country_type='HISTORICAL', region_name=None,
                                    income_group=None, currency_code=None,
                                    is_aggregate=False, is_active=True))
    if historical_rows:
        fields = country.columns
        extra = spark.createDataFrame(
            [tuple(row.get(field) for field in fields) for row in historical_rows],
            country.schema).withColumn('country_key', hashed('m49_code'))
        country = country.unionByName(extra).dropDuplicates(['country_key'])
    official_names = {code: name for name, code in GEO['provinces'].items()}
    name_map = F.create_map(*[part for code, name in official_names.items()
                              for part in (lit(code), lit(name))])
    geo_name = F.coalesce(name_map[F.col('geography_code')], F.col('geography_name'))
    geography = ref('geography').select(
        hashed('geography_code', 'geography_version').alias('geography_key'),
        'geography_code', 'geography_version', geo_name.alias('geography_name'),
        geo_name.alias('geography_name_normalized'), 'geography_level',
        F.col('parent_code').alias('parent_geography_code'),
        F.sha2(F.to_json(F.array(lit('704'))), 256).alias('country_key'),
        'province_code', lit(None).cast('string').alias('province_name'), 'district_code',
        lit(None).cast('string').alias('district_name'), 'valid_from', 'valid_to',
        F.col('valid_to').isNull().alias('is_current')).distinct()
    weather = ref('weather_location').select(
        hashed('weather_location_code', 'valid_from').alias('weather_location_key'),
        'weather_location_code', F.col('provider').alias('weather_provider'),
        F.col('mapping_method').alias('grid_method'), 'latitude', 'longitude',
        lit(None).cast('double').alias('elevation_m'), lit(None).cast('string').alias('geography_key'),
        'valid_from', 'valid_to', F.col('valid_to').isNull().alias('is_current'))
    series = ref('price_series').select(
        hashed('series_code', 'valid_from').alias('price_series_key'), 'series_code',
        F.col('series_code').alias('series_name'), 'price_type',
        F.col('dataset_id').alias('source_dataset_code'), 'market_scope',
        lit(None).cast('string').alias('commodity_variant'),
        F.col('native_currency').alias('native_currency_code'),
        F.col('native_unit').alias('native_unit_code'),
        F.col('canonical_unit').alias('canonical_unit_code'),
        F.col('index_base').alias('index_base_label'),
        F.col('frequency').alias('frequency_code'), 'valuation_basis',
        'valid_from', 'valid_to', F.col('valid_to').isNull().alias('is_current'))
    product = ref('trade_product').select(
        hashed('classification_system', 'classification_version', 'product_code').alias('trade_product_key'),
        'classification_system', 'classification_version', 'product_code', 'product_name',
        'aggregation_level', 'is_leaf', lit(True).alias('is_active'))
    indicator = ref('macro_indicator').select(
        hashed('indicator_code').alias('macro_indicator_key'), 'indicator_code', 'indicator_name',
        lit(None).cast('string').alias('indicator_group'), 'unit_code', 'scale_factor',
        'aggregation_rule', 'value_type', lit(None).cast('string').alias('description'),
        lit(True).alias('is_active'))
    frames = {'dim_period': period, 'dim_commodity': commodity, 'dim_country': country,
              'dim_geography': geography, 'dim_weather_location': weather,
              'dim_price_series': series, 'dim_trade_product': product,
              'dim_macro_indicator': indicator}
    counts = {}
    for name in DIMENSIONS:
        counts[name] = insert_new(spark, 'gold_dim', name, frames[name], [name.removeprefix('dim_') + '_key'])
    bridge = ref('trade_product_commodity_bridge').select(
        hashed('classification_system', 'classification_version', 'product_code').alias('trade_product_key'),
        hashed('commodity_code').alias('commodity_key'), 'mapping_type', 'allocation_weight',
        'mapping_status', 'mapping_rule', 'valid_from', 'valid_to',
        F.col('valid_to').isNull().alias('is_current'))
    counts['bridge_trade_product_commodity'] = insert_new(
        spark, 'gold_bridge', 'bridge_trade_product_commodity', bridge,
        ['trade_product_key', 'commodity_key', 'valid_from'])
    return {'silver_batch': batch['batch_id'], 'rows_inserted': counts}
