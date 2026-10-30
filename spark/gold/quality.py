"""Gold DQ and publication gates over a reproducible Silver batch."""

import json
from datetime import datetime

from pyspark.sql import functions as F

from .contracts import BRIDGE, DIMENSIONS, FACTS, VERSION
from .source import current_snapshot, resolve, winners
from silver.foundation import _upsert


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


def audit(spark):
    batch, inputs = resolve(spark)
    gold_batch = f"gold-{batch['batch_id']}-{VERSION}"
    checks = []
    counts = {}
    snapshots = {}

    def record(table, rule, failed):
        checks.append(dict(gold_batch_id=gold_batch, table_name=table,
                           rule_code=rule, failed_count=int(failed),
                           status='PASS' if failed == 0 else 'FAIL',
                           checked_at=datetime.utcnow()))

    for namespace, tables in [('gold_dim', DIMENSIONS), ('gold_bridge', BRIDGE),
                              ('gold_fact', FACTS)]:
        for name in tables:
            full = f'lakehouse.{namespace}.{name}'
            frame = spark.table(full)
            count = frame.count()
            counts[full] = count
            record(name, 'NONEMPTY', int(count == 0))
            key = (name.removeprefix('dim_') + '_key' if namespace == 'gold_dim'
                   else name.removeprefix('fact_') + '_key' if namespace == 'gold_fact'
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
            if namespace == 'gold_fact':
                record(name, 'CANONICAL_ONLY', frame.filter(~F.col('is_canonical')).count())
                for foreign, dimension in FOREIGN_KEYS[name].items():
                    # Some optional scope keys (country/geography) may be NULL.
                    values = frame.filter(F.col(foreign).isNotNull()).select(foreign).distinct()
                    allowed = spark.table(f'lakehouse.gold_dim.{dimension}').select(
                        F.col(dimension.removeprefix('dim_') + '_key').alias(foreign)).distinct()
                    record(name, 'FK_' + foreign.upper(), values.join(
                        allowed, foreign, 'left_anti').count())
            snapshots[full] = current_snapshot(spark, full)

    price = spark.table('lakehouse.gold_fact.fact_price')
    record('fact_price', 'AMOUNT_INDEX_XOR', price.filter(
        F.col('price_amount_native').isNotNull() & F.col('price_index_value').isNotNull()).count())
    stock = spark.table('lakehouse.gold_fact.fact_coffee_stock')
    record('fact_coffee_stock', 'BAG_CONVERSION', stock.filter(
        F.col('ending_stock_tonnes') !=
        F.col('ending_stock_thousand_bags') * F.col('bag_weight_kg')).count())
    bridge = spark.table('lakehouse.gold_bridge.bridge_trade_product_commodity')
    record('bridge_trade_product_commodity', 'PENDING_NO_WEIGHT', bridge.filter(
        (F.col('mapping_status') == 'PENDING') & F.col('allocation_weight').isNotNull()).count())
    approved = bridge.filter(F.col('mapping_status') == 'APPROVED')
    record('bridge_trade_product_commodity', 'APPROVED_WEIGHT', approved.groupBy(
        'trade_product_key').agg(F.sum('allocation_weight').alias('total'))
           .filter("total <> 1 OR total IS NULL").count())
    lineage = spark.table('lakehouse.gold_meta.fact_lineage').filter(
        F.col('gold_batch_id') == gold_batch)
    for name in FACTS:
        key = (name.removeprefix('fact_') + '_key')
        if name == 'fact_macro_indicator':
            key = 'macro_indicator_fact_key'
        facts = spark.table(f'lakehouse.gold_fact.{name}').select(F.col(key).alias('fact_key'))
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
        source = winners(spark, domain, batch['policy_version'], inputs)
        if domain == 'price_observation':
            source = source.filter(F.col('geography_code').isNull())
        elif domain == 'trade_total_observation':
            source = source.filter(F.col('statistical_scope_code') == 'SOURCE_TOTAL')
        elif domain == 'trade_partner_observation':
            source = source.filter(
                (F.col('classification_system') == 'FAOSTAT') &
                (F.col('statistical_scope_code') == 'PARTNER_MATRIX') &
                F.col('partner2_code').isNull() & F.col('customs_code').isNull() &
                F.col('mode_of_transport_code').isNull())
        expected = source.select('observation_id').distinct()
        used = lineage.filter(F.col('fact_table') == fact).select('observation_id').distinct()
        record(fact, 'SOURCE_CONTRIBUTION_COVERAGE', expected.join(
            used, 'observation_id', 'left_anti').count())
        record(fact, 'UNEXPECTED_CONTRIBUTION', used.join(
            expected, 'observation_id', 'left_anti').count())
    return gold_batch, batch, inputs, counts, snapshots, checks


def publish(spark):
    gold_batch, silver, inputs, counts, snapshots, checks = audit(spark)
    checks_table = 'lakehouse.gold_meta.dq_result'
    _upsert(spark, checks_table, checks,
            ['gold_batch_id', 'table_name', 'rule_code'])
    failures = [item for item in checks if item['status'] == 'FAIL']
    if failures:
        return {'status': 'BLOCKED', 'gold_batch': gold_batch,
                'failed_checks': [(x['table_name'], x['rule_code'], x['failed_count'])
                                  for x in failures], 'row_counts': counts}
    table = 'lakehouse.gold_meta.publish_batch'
    if spark.table(table).filter(F.col('gold_batch_id') == gold_batch).limit(1).count():
        return {'status': 'ALREADY_PUBLISHED', 'gold_batch': gold_batch, 'row_counts': counts}
    row = dict(gold_batch_id=gold_batch, silver_batch_id=silver['batch_id'],
               policy_version=silver['policy_version'], transform_version=VERSION,
               input_snapshots_json=json.dumps(inputs, sort_keys=True),
               output_snapshots_json=json.dumps(snapshots, sort_keys=True),
               row_counts_json=json.dumps(counts, sort_keys=True),
               dq_run_id=gold_batch + '-dq', status='READY_WITH_EXCEPTIONS',
               started_at=datetime.utcnow(), completed_at=datetime.utcnow(),
               error_summary='Published Silver source has documented price/district exceptions; local price version mapping withheld.')
    spark.createDataFrame([row], spark.table(table).schema).writeTo(table).append()
    if spark.table(table).filter(F.col('status').startswith('ACTIVE')).limit(1).count():
        raise RuntimeError('Existing ACTIVE Gold batch requires explicit retirement gate')
    escaped = gold_batch.replace("'", "''")
    spark.sql(f"UPDATE {table} SET status='ACTIVE_WITH_EXCEPTIONS' WHERE gold_batch_id='{escaped}'")
    return {'status': 'ACTIVE_WITH_EXCEPTIONS', 'gold_batch': gold_batch,
            'row_counts': counts, 'snapshots': snapshots}
