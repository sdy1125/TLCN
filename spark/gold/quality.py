"""Gold DQ and publication gates over a reproducible Silver batch."""

from datetime import datetime

from pyspark.sql import functions as F

from .contracts import BRIDGE, DIMENSIONS, FACTS
from .source import current_snapshot, winners
from .scope import SCOPE_VERSION, classify


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
    name: set(keys) - ({'geography_key'} if name == 'fact_price' else set())
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
                         'trade_flow_code', 'observation_set_id',
                         'record_source_code'],
    'fact_trade_partner': ['period_key', 'reporter_country_key',
                           'partner_country_key', 'trade_product_key',
                           'trade_flow_code', 'observation_set_id',
                           'record_source_code'],
    'fact_weather_daily': ['period_key', 'weather_location_key',
                           'observation_set_id'],
    'fact_coffee_stock': ['period_key', 'country_key', 'commodity_key',
                          'scenario_code', 'observation_set_id'],
    'fact_macro_indicator': ['period_key', 'country_key',
                             'macro_indicator_key', 'observation_set_id'],
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
                record(name, 'CANONICAL_ONLY', frame.filter(~F.col('is_canonical')).count())
                record(name, 'BUSINESS_GRAIN_UNIQUE', frame.groupBy(
                    *BUSINESS_GRAINS[name]).count().filter('count > 1').count())
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
        F.col('price_amount_native').isNotNull() & F.col('price_index_value').isNotNull()).count())
    stock = spark.table(f'lakehouse.{fact_namespace}.fact_coffee_stock')
    record('fact_coffee_stock', 'BAG_CONVERSION', stock.filter(
        F.col('ending_stock_tonnes') !=
        F.col('ending_stock_thousand_bags') * F.col('bag_weight_kg')).count())
    bridge = spark.table(
        f'lakehouse.{bridge_namespace}.bridge_trade_product_commodity')
    record('bridge_trade_product_commodity', 'PENDING_NO_WEIGHT', bridge.filter(
        (F.col('mapping_status') == 'PENDING') & F.col('allocation_weight').isNotNull()).count())
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
    domains = {
        'production_observation': 'fact_production_national',
        'provincial_agri_observation': 'fact_production_provincial',
        'price_observation': 'fact_price',
        'trade_total_observation': 'fact_trade_total',
        'trade_partner_observation': 'fact_trade_partner',
        'weather_daily': 'fact_weather_daily',
        'coffee_stock_observation': 'fact_coffee_stock',
        'macro_indicator_observation': 'fact_macro_indicator',
    }
    for domain, fact in domains.items():
        source = classify(domain, winners(spark, domain, batch['policy_version'], inputs))
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
    return gold_batch, batch, inputs, counts, snapshots, checks
