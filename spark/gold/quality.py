"""Gold DQ and publication gates over a reproducible Silver batch."""

from datetime import datetime

from pyspark.sql import functions as F

from .contracts import BRIDGE, DIMENSIONS, FACTS
from .geography import ROWS as DISTRICT_ROWS, validate as validate_district_crosswalk
from .build_facts import SPECS
from .source import current_snapshot, winners
from .scope import CONTRACT, SCOPE_VERSION, classify


FOREIGN_KEYS = {
    'fact_production_national': {'period_key': 'dim_period', 'country_key': 'dim_country', 'commodity_key': 'dim_commodity'},
    'fact_production_provincial': {'period_key': 'dim_period', 'geography_key': 'dim_geography', 'commodity_key': 'dim_commodity'},
    'fact_price': {'period_key': 'dim_period', 'country_key': 'dim_country', 'geography_key': 'dim_geography', 'commodity_key': 'dim_commodity', 'price_series_key': 'dim_price_series'},
    'fact_trade_total': {'period_key': 'dim_period', 'reporter_country_key': 'dim_country', 'commodity_key': 'dim_commodity', 'trade_product_key': 'dim_trade_product'},
    'fact_trade_partner': {'period_key': 'dim_period', 'reporter_country_key': 'dim_country', 'partner_country_key': 'dim_country', 'commodity_key': 'dim_commodity', 'trade_product_key': 'dim_trade_product'},
    'fact_weather_daily': {'period_key': 'dim_period', 'weather_location_key': 'dim_weather_location'},
    'fact_coffee_stock': {'period_key': 'dim_period', 'country_key': 'dim_country', 'commodity_key': 'dim_commodity'},
    'fact_macro_indicator': {'period_key': 'dim_period', 'country_key': 'dim_country', 'macro_indicator_key': 'dim_macro_indicator'},
}

REQUIRED_FKS = {
    name: set(keys) - ({'geography_key'} if name == 'fact_price' else
                       {'partner_country_key', 'commodity_key'}
                       if name == 'fact_trade_partner' else set())
    for name, keys in FOREIGN_KEYS.items()
}

BUSINESS_GRAINS = {
    'fact_production_national': ['period_key', 'country_key', 'commodity_key',
                                 'observation_set_id', 'record_source_code'],
    'fact_production_provincial': ['period_key', 'geography_key', 'commodity_key',
                                   'coverage_code', 'record_status_code',
                                   'observation_set_id', 'record_source_code'],
    'fact_price': ['period_key', 'country_key', 'geography_key', 'commodity_key',
                   'price_series_key', 'observation_set_id', 'record_source_code'],
    'fact_trade_total': ['period_key', 'reporter_country_key', 'trade_product_key',
                         'trade_flow_code', 'statistical_scope_code',
                         'observation_set_id',
                         'record_source_code'],
    'fact_trade_partner': ['period_key', 'reporter_country_key',
                           'partner_m49_code', 'trade_product_key',
                           'trade_flow_code', 'partner2_code', 'customs_code',
                           'mode_of_transport_code', 'statistical_scope_code',
                           'observation_set_id', 'record_source_code'],
    'fact_weather_daily': ['period_key', 'weather_location_key',
                           'observation_set_id'],
    'fact_coffee_stock': ['period_key', 'country_key', 'commodity_key',
                          'scenario_code', 'observation_set_id'],
    'fact_macro_indicator': ['period_key', 'country_key',
                             'macro_indicator_key', 'observation_set_id'],
}
ANALYTIC_GRAINS = {
    name: [field for field in grain if field not in (
        'observation_set_id', 'record_source_code')]
    for name, grain in BUSINESS_GRAINS.items()
    if name != 'fact_production_provincial'
}


def audit(spark, context, namespace):
    batch, inputs = context
    gold_batch = batch['gold_batch_id']
    dim_namespace = namespace
    bridge_namespace = namespace
    fact_namespace = namespace
    meta_namespace = namespace
    checks = []
    counts = {}
    snapshots = {}

    def record(table, rule, failed):
        checks.append(dict(gold_batch_id=gold_batch, table_name=table,
                           rule_code=rule, failed_count=int(failed),
                           status='PASS' if failed == 0 else 'FAIL',
                           checked_at=datetime.utcnow()))

    for kind, tables in [('gold_dim', DIMENSIONS), ('gold_bridge', BRIDGE),
                         ('gold_fact', FACTS)]:
        for name in tables:
            full = f'lakehouse.{namespace}.{name}'
            frame = spark.table(full)
            count = frame.count()
            counts[full] = count
            record(name, 'NONEMPTY', int(count == 0))
            key = (name.removeprefix('dim_') + '_key' if kind == 'gold_dim'
                   else name.removeprefix('fact_') + '_key' if kind == 'gold_fact'
                   else None)
            if name == 'fact_production_national':
                key = 'production_national_key'
            if name == 'fact_production_provincial':
                key = 'production_provincial_key'
            if name == 'fact_weather_daily':
                key = 'weather_daily_key'
            if name == 'fact_macro_indicator':
                key = 'macro_indicator_fact_key'
            if key:
                record(name, 'KEY_NONNULL', frame.filter(F.col(key).isNull()).count())
                record(name, 'KEY_UNIQUE', frame.groupBy(key).count().filter('count > 1').count())
            if kind == 'gold_fact':
                record(name, 'CANONICAL_ONLY', frame.filter(
                    ~F.col('is_canonical').eqNullSafe(F.lit(True))).count())
                record(name, 'BUSINESS_GRAIN_UNIQUE', frame.groupBy(
                    *BUSINESS_GRAINS[name]).count().filter('count > 1').count())
                if name in ANALYTIC_GRAINS:
                    record(name, 'ANALYTIC_GRAIN_UNIQUE', frame.groupBy(
                        *ANALYTIC_GRAINS[name]).count().filter('count > 1').count())
                for foreign, dimension in FOREIGN_KEYS[name].items():
                    if foreign in REQUIRED_FKS[name]:
                        record(name, 'REQUIRED_' + foreign.upper(),
                               frame.filter(F.col(foreign).isNull()).count())
                    values = frame.filter(F.col(foreign).isNotNull()).select(foreign).distinct()
                    allowed = spark.table(f'lakehouse.{dim_namespace}.{dimension}').select(
                        F.col(dimension.removeprefix('dim_') + '_key').alias(foreign)).distinct()
                    record(name, 'FK_' + foreign.upper(), values.join(
                        allowed, foreign, 'left_anti').count())
            snapshots[full] = current_snapshot(spark, full)

    price = spark.table(f'lakehouse.{fact_namespace}.fact_price')
    record('fact_price', 'AMOUNT_INDEX_XOR', price.filter(
        F.col('price_amount_native').isNotNull() ==
        F.col('price_index_value').isNotNull()).count())
    price_scope = (price.alias('f').join(
        spark.table(f'lakehouse.{dim_namespace}.dim_price_series').select(
            'price_series_key', 'market_scope').alias('s'),
        F.col('f.price_series_key') == F.col('s.price_series_key'), 'left'
    ).join(
        spark.table(f'lakehouse.{dim_namespace}.dim_country').select(
            F.col('country_key').alias('_country_key'), 'm49_code').alias('c'),
        F.col('f.country_key') == F.col('c._country_key'), 'left'
    ).join(
        spark.table(f'lakehouse.{dim_namespace}.dim_geography').select(
            F.col('geography_key').alias('_geography_key'),
            F.col('geography_level').alias('_geography_level'),
            F.col('country_key').alias('_geography_country_key')).alias('g'),
        F.col('f.geography_key') == F.col('g._geography_key'), 'left'))
    global_ok = ((F.col('s.market_scope') == 'GLOBAL') &
                 (F.col('c.m49_code') == '001') &
                 F.col('f.geography_key').isNull())
    country_ok = ((F.col('s.market_scope') == 'COUNTRY') &
                  (F.col('c.m49_code') != '001') &
                  F.col('f.geography_key').isNull())
    local_ok = (F.col('s.market_scope').isin('PROVINCE', 'DISTRICT') &
                (F.col('s.market_scope') == F.col('g._geography_level')) &
                F.col('f.geography_key').isNotNull() &
                (F.col('f.country_key') == F.col('g._geography_country_key')))
    record('fact_price', 'GEOGRAPHY_SCOPE', price_scope.filter(
        ~F.coalesce(global_ok | country_ok | local_ok, F.lit(False))).count())
    trade_total = spark.table(f'lakehouse.{fact_namespace}.fact_trade_total')
    record('fact_trade_total', 'STATISTICAL_SCOPE', trade_total.filter(
        ~F.coalesce(F.col('statistical_scope_code').isin(
            'SOURCE_TOTAL', 'NATIONAL_REPORTED'), F.lit(False))).count())
    partner = spark.table(f'lakehouse.{fact_namespace}.fact_trade_partner').join(
        spark.table(f'lakehouse.{dim_namespace}.dim_trade_product').select(
            'trade_product_key', 'classification_system'),
        'trade_product_key', 'left')
    faostat = ((F.col('classification_system') == 'FAOSTAT') &
               (F.col('statistical_scope_code') == 'PARTNER_MATRIX') &
               (F.col('valuation_basis_code') == 'AS_PUBLISHED') &
               F.col('commodity_key').isNotNull() &
               F.col('partner_country_key').isNotNull() &
               F.col('partner2_code').isNull() &
               F.col('customs_code').isNull() &
               F.col('mode_of_transport_code').isNull())
    hs = ((F.col('classification_system') == 'HS') &
          (F.col('valuation_basis_code') == 'PRIMARY_VALUE_USD') &
          F.col('commodity_key').isNull() &
          (F.col('partner2_code') == '0') &
          (F.col('customs_code') == 'C00') &
          F.col('mode_of_transport_code').isNotNull())
    record('fact_trade_partner', 'SOURCE_SCOPE', partner.filter(
        ~F.coalesce(faostat | hs, F.lit(False))).count())
    stock = spark.table(f'lakehouse.{fact_namespace}.fact_coffee_stock')
    record('fact_coffee_stock', 'BAG_CONVERSION', stock.filter(
        F.col('ending_stock_tonnes').isNull() |
        F.col('ending_stock_thousand_bags').isNull() |
        F.col('bag_weight_kg').isNull() |
        (F.col('ending_stock_tonnes') !=
         F.col('ending_stock_thousand_bags') * F.col('bag_weight_kg'))).count())
    validate_district_crosswalk()
    district_crosswalk = spark.createDataFrame(DISTRICT_ROWS).select(
        F.col('province_code').alias('_district_province_code'),
        F.col('raw_name').alias('_district_raw_name'),
        F.col('year').alias('_district_year'),
        F.col('mapping_status').alias('_district_mapping_status'))
    district_facts = spark.table(
        f'lakehouse.{fact_namespace}.fact_production_provincial').select(
            'production_provincial_key', 'geography_key', 'period_key').join(
        spark.table(f'lakehouse.{dim_namespace}.dim_geography').select(
            'geography_key', 'geography_level', 'province_code', 'geography_name'),
        'geography_key').filter("geography_level='DISTRICT'").join(
        spark.table(f'lakehouse.{dim_namespace}.dim_period').select(
            'period_key', 'calendar_year'), 'period_key')
    district_join = district_facts.join(
        district_crosswalk,
        (F.col('province_code') == F.col('_district_province_code')) &
        (F.col('geography_name') == F.col('_district_raw_name')) &
        (F.col('calendar_year') == F.col('_district_year')), 'left')
    record('fact_production_provincial', 'DISTRICT_CROSSWALK_COVERAGE',
           district_join.filter(F.col('_district_mapping_status').isNull()).count())
    record('fact_production_provincial', 'DISTRICT_CROSSWALK_UNIQUE',
           district_join.groupBy('production_provincial_key').count()
           .filter('count > 1').count())
    bridge = spark.table(
        f'lakehouse.{bridge_namespace}.bridge_trade_product_commodity')
    record('bridge_trade_product_commodity', 'PENDING_NO_WEIGHT', bridge.filter(
        (F.col('mapping_status') == 'PENDING') & F.col('allocation_weight').isNotNull()).count())
    record('bridge_trade_product_commodity', 'NON_EQUIVALENT_NO_WEIGHT', bridge.filter(
        (F.col('mapping_status') == 'NOT_EQUIVALENT') &
        F.col('allocation_weight').isNotNull()).count())
    record('bridge_trade_product_commodity', 'WEIGHT_RANGE', bridge.filter(
        F.col('allocation_weight').isNotNull() &
        ((F.col('allocation_weight') < 0) | (F.col('allocation_weight') > 1))).count())
    approved = bridge.filter(F.col('mapping_status') == 'APPROVED')
    record('bridge_trade_product_commodity', 'APPROVED_WEIGHT', approved.groupBy(
        'trade_product_key').agg(F.sum('allocation_weight').alias('total'))
           .filter("total <> 1 OR total IS NULL").count())
    lineage = spark.table(f'lakehouse.{meta_namespace}.fact_lineage').filter(
        F.col('gold_batch_id') == gold_batch)
    record('fact_lineage', 'OBSERVATION_ONCE', lineage.groupBy(
        'observation_id').count().filter('count > 1').count())
    for name in FACTS:
        key = (name.removeprefix('fact_') + '_key')
        if name == 'fact_macro_indicator':
            key = 'macro_indicator_fact_key'
        facts = spark.table(f'lakehouse.{fact_namespace}.{name}').select(
            F.col(key).alias('fact_key'))
        links = lineage.filter(F.col('fact_table') == name).select('fact_key').distinct()
        record(name, 'LINEAGE_COVERAGE', facts.join(links, 'fact_key', 'left_anti').count())
        record(name, 'LINEAGE_FK', links.join(facts, 'fact_key', 'left_anti').count())
    for domain, (fact, _) in SPECS.items():
        fact_periods = spark.table(f'lakehouse.{fact_namespace}.{fact}').join(
            spark.table(f'lakehouse.{dim_namespace}.dim_period').select(
                'period_key', 'period_type'), 'period_key', 'left')
        record(fact, 'PERIOD_SCOPE', fact_periods.filter(
            ~F.coalesce(F.col('period_type').isin(
                *CONTRACT['domains'][domain]['periods']), F.lit(False))).count())
        source = classify(
            domain, winners(spark, domain, batch['policy_version'], inputs)
        ).select('observation_id', '_scope_status', '_scope_reason').cache()
        source.count()
        included = source.filter("_scope_status='INCLUDED'").select('observation_id').distinct()
        excluded = source.filter("_scope_status='EXCLUDED_APPROVED'").select('observation_id').distinct()
        blocked = source.filter("_scope_status='BLOCKED_UNMAPPED'")
        record(fact, 'SCOPE_VERSION_' + SCOPE_VERSION.upper(), 0)
        record(fact, 'BLOCKED_UNMAPPED', blocked.count())
        record(fact, 'EXCLUSION_REASON', source.filter(
            "_scope_status='EXCLUDED_APPROVED' AND _scope_reason IS NULL").count())
        record(fact, 'SCOPE_CLASSIFICATION', source.filter(
            "_scope_status IS NULL OR _scope_status NOT IN "
            "('INCLUDED','EXCLUDED_APPROVED','BLOCKED_UNMAPPED')").count())
        used = lineage.filter(F.col('fact_table') == fact).select('observation_id').distinct()
        record(fact, 'SOURCE_CONTRIBUTION_COVERAGE', included.join(
            used, 'observation_id', 'left_anti').count())
        record(fact, 'UNEXPECTED_CONTRIBUTION', used.join(
            included, 'observation_id', 'left_anti').count())
        accounted = used.unionByName(excluded).distinct()
        record(fact, 'ALL_WINNER_COVERAGE', source.select('observation_id').distinct().join(
            accounted, 'observation_id', 'left_anti').count())
        record(fact, 'EXCLUDED_IN_LINEAGE', used.join(
            excluded, 'observation_id').count())
        source.unpersist()
    return gold_batch, batch, inputs, counts, snapshots, checks
