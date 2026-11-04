"""Load only published Silver winners into grain-safe Gold Facts."""

from pyspark.sql import functions as F

from .build_dimensions import hashed, insert_new
from .contracts import FACTS
from .source import winners
from .scope import classify
from .temporal import attach_key


SPECS = {
    'production_observation': ('fact_production_national', 'production_national_key'),
    'provincial_agri_observation': ('fact_production_provincial', 'production_provincial_key'),
    'price_observation': ('fact_price', 'price_key'),
    'trade_total_observation': ('fact_trade_total', 'trade_total_key'),
    'trade_partner_observation': ('fact_trade_partner', 'trade_partner_key'),
    'weather_daily': ('fact_weather_daily', 'weather_daily_key'),
    'coffee_stock_observation': ('fact_coffee_stock', 'coffee_stock_key'),
    'macro_indicator_observation': ('fact_macro_indicator', 'macro_indicator_fact_key'),
}


def _pivot(frame, grain, metrics):
    if frame.groupBy(*(grain + ['metric_code'])).count().filter('count > 1').limit(1).count():
        raise ValueError('Silver winner rows collapse at target Fact grain')
    exprs = []
    for metric, target in metrics.items():
        exprs.append(F.max(F.when(F.col('metric_code') == metric,
                                  F.col('value_canonical'))).alias(target))
        exprs.append(F.max(F.when(F.col('metric_code') == metric,
                                  F.col('quality_flag_source'))).alias(target + '_flag'))
    return frame.groupBy(*grain).agg(*exprs)


def _common(frame):
    return frame.withColumn('is_canonical', F.lit(True)).withColumn('loaded_at', F.current_timestamp())


def _key(frame, fields, name):
    return frame.withColumn(name, hashed(*fields))


def _period(frame):
    return frame.withColumn('period_key', hashed('period_type', 'period_code'))


def _commodity(frame):
    return frame.withColumn('commodity_key', F.when(
        F.col('commodity_code').isNotNull(), hashed('commodity_code')))


def _country(frame, source, target):
    return frame.withColumn(target, F.when(F.col(source).isNotNull(), hashed(source)))


def _product(frame):
    return frame.withColumn('trade_product_key', hashed(
        'classification_system', 'classification_version', 'product_code'))


def _lineage(spark, source, fact, key, snapshot, batch_id, namespace):
    rows = source.select(
        F.lit(batch_id).alias('gold_batch_id'), F.lit(fact).alias('fact_table'),
        F.col(key).alias('fact_key'), 'metric_code', 'observation_id',
        F.lit(snapshot).cast('long').alias('silver_snapshot_id'),
        F.lit('WINNER').alias('contribution_status'))
    return insert_new(spark, namespace, 'fact_lineage', rows,
                      ['gold_batch_id', 'fact_table', 'fact_key', 'metric_code', 'observation_id'])


def build(spark, context, namespace):
    batch, snapshots = context
    fact_namespace = namespace
    dimension_namespace = namespace
    lineage_namespace = namespace
    batch_id = batch['gold_batch_id']
    counts = {}
    for domain, (name, key) in SPECS.items():
        source = classify(domain, winners(
            spark, domain, batch['policy_version'], snapshots
        )).filter(F.col('_scope_status') == 'INCLUDED')
        if domain == 'price_observation':
            source = source.withColumn(
                'country_m49_code', F.when(F.col('market_scope') == 'GLOBAL',
                                           F.lit('001')).otherwise(F.col('country_m49_code')))
            source = attach_key(
                source, spark.table(f'lakehouse.{dimension_namespace}.dim_price_series'),
                'series_code', 'series_code', 'price_series_key')
        if domain == 'weather_daily':
            source = attach_key(
                source, spark.table(f'lakehouse.{dimension_namespace}.dim_weather_location'),
                'weather_location_code', 'weather_location_code',
                'weather_location_key')
        if domain == 'production_observation':
            grain = ['m49_code', 'commodity_code', 'period_type', 'period_code',
                     'observation_set_id', 'dataset_id']
            base = _pivot(source, grain, {'AREA_HARVESTED': 'area_harvested_ha',
                                          'PRODUCTION': 'production_tonnes',
                                          'YIELD': 'reported_yield'})
            base = _country(_commodity(_period(base)), 'm49_code', 'country_key')
            base = base.withColumn('yield_kg_ha', F.coalesce(
                'reported_yield', F.when(F.col('area_harvested_ha') > 0,
                                         F.col('production_tonnes') * 1000 / F.col('area_harvested_ha'))))
            base = base.withColumn('yield_calculation_method',
                F.when(F.col('reported_yield').isNotNull(), 'REPORTED')
                 .when(F.col('yield_kg_ha').isNotNull(), 'DERIVED').otherwise('UNAVAILABLE'))
            base = _key(base, grain, key).select(
                key, 'period_key', 'country_key', 'commodity_key', 'area_harvested_ha',
                'production_tonnes', 'yield_kg_ha', 'yield_calculation_method',
                F.col('area_harvested_ha_flag').alias('area_flag_code'),
                F.col('production_tonnes_flag').alias('production_flag_code'),
                F.col('reported_yield_flag').alias('yield_flag_code'),
                'observation_set_id', F.col('dataset_id').alias('record_source_code'))
        elif domain == 'provincial_agri_observation':
            grain = ['geography_code', 'geography_version', 'commodity_code',
                     'period_type', 'period_code', 'coverage_code', 'record_status_code',
                     'observation_set_id', 'dataset_id']
            if source.groupBy(*(grain + ['metric_code'])).agg(
                    F.countDistinct('coverage_end_date').alias('variants')).filter('variants > 1').limit(1).count():
                raise ValueError('Coverage end date creates a finer Silver grain')
            base = _pivot(source, grain, {'AREA_PLANTED': 'area_planted_ha',
                                          'AREA_HARVESTED': 'area_harvested_ha',
                                          'PRODUCTION': 'production_tonnes',
                                          'YIELD': 'reported_yield'})
            base = _commodity(_period(base)).withColumn(
                'geography_key', hashed('geography_code', 'geography_version'))
            base = base.withColumn('yield_kg_ha', F.coalesce(
                'reported_yield', F.when(F.col('area_harvested_ha') > 0,
                                         F.col('production_tonnes') * 1000 / F.col('area_harvested_ha'))))
            base = base.withColumn('yield_calculation_method',
                F.when(F.col('reported_yield').isNotNull(), 'REPORTED')
                 .when(F.col('yield_kg_ha').isNotNull(), 'DERIVED').otherwise('UNAVAILABLE'))
            base = _key(base, grain, key).select(
                key, 'period_key', 'geography_key', 'commodity_key', 'coverage_code',
                'record_status_code', 'area_planted_ha', 'area_harvested_ha',
                'production_tonnes', 'yield_kg_ha', 'yield_calculation_method',
                F.coalesce('area_harvested_ha_flag', 'area_planted_ha_flag').alias('area_flag_code'),
                F.col('production_tonnes_flag').alias('production_flag_code'),
                F.col('reported_yield_flag').alias('yield_flag_code'),
                'observation_set_id', F.col('dataset_id').alias('record_source_code'))
        elif domain == 'price_observation':
            grain = ['series_code', 'commodity_code', 'country_m49_code',
                     'geography_code', 'period_type', 'period_code', 'observation_set_id']
            if source.groupBy(*grain).count().filter('count > 1').limit(1).count():
                raise ValueError('Price series has duplicate target grain')
            base = _commodity(_period(source))
            base = _country(base, 'country_m49_code', 'country_key')
            base = base.withColumn('geography_key', F.when(
                F.col('geography_code').isNotNull(), hashed('geography_code', F.lit(None))))
            base = _key(base, grain, key).select(
                key, 'period_key', 'commodity_key', 'country_key', 'geography_key',
                'price_series_key', 'price_amount_native', 'price_amount_canonical',
                'price_index_value', F.col('quality_flag_source').alias('value_flag_code'),
                'observation_set_id', F.col('dataset_id').alias('record_source_code'))
        elif domain in {'trade_total_observation', 'trade_partner_observation'}:
            partner = domain == 'trade_partner_observation'
            grain = ['reporter_m49_code'] + (['partner_m49_code'] if partner else []) + [
                'classification_system', 'classification_version', 'product_code',
                'commodity_code', 'trade_flow_code', 'period_type', 'period_code',
                'observation_set_id', 'dataset_id']
            base = _pivot(source, grain, {'TRADE_QUANTITY': 'trade_quantity_tonnes',
                                          'TRADE_VALUE': 'trade_value_usd'})
            if partner:
                estimated = source.groupBy(*grain).agg(
                    F.max(F.when(F.col('metric_code') == 'TRADE_QUANTITY',
                                 (F.col('value_status') == 'ESTIMATED').cast('int')))
                     .cast('boolean').alias('quantity_estimated_flag'),
                    F.max(F.when(F.col('metric_code') == 'TRADE_VALUE',
                                 (F.col('value_status') == 'ESTIMATED').cast('int')))
                     .cast('boolean').alias('value_estimated_flag'))
                base = base.join(estimated, grain)
            base = _product(_commodity(_period(base)))
            base = _country(base, 'reporter_m49_code', 'reporter_country_key')
            if partner:
                base = _country(base, 'partner_m49_code', 'partner_country_key')
            base = _key(base, grain, key)
            columns = [key, 'period_key', 'reporter_country_key'] + (
                ['partner_country_key'] if partner else []) + [
                'commodity_key', 'trade_product_key', 'trade_flow_code',
                'trade_quantity_tonnes', 'trade_value_usd']
            if partner:
                columns += ['quantity_estimated_flag', 'value_estimated_flag']
            else:
                columns += [F.col('trade_quantity_tonnes_flag').alias('quantity_flag_code'),
                            F.col('trade_value_usd_flag').alias('value_flag_code')]
            base = base.select(*columns, 'observation_set_id',
                               F.col('dataset_id').alias('record_source_code'))
        elif domain == 'weather_daily':
            grain = ['weather_location_code', 'observation_date', 'observation_set_id']
            if source.groupBy(*grain).count().filter('count > 1').limit(1).count():
                raise ValueError('Weather has duplicate target grain')
            base = source
            base = _period(base)
            base = _key(base, grain, key).select(
                key, 'period_key', 'weather_location_key', 'temperature_avg_c',
                'temperature_max_c', 'temperature_min_c', 'relative_humidity_pct',
                'wind_speed_m_s', 'precipitation_mm', 'solar_radiation_mj_m2_day',
                'valid_measure_count', 'has_missing_measure', 'observation_set_id',
                F.col('dataset_id').alias('record_source_code'))
        elif domain == 'coffee_stock_observation':
            grain = ['country_m49_code', 'commodity_code', 'crop_year_code',
                     'scenario_code', 'observation_set_id']
            base = _country(_commodity(_period(source)), 'country_m49_code', 'country_key')
            base = _key(base, grain, key).select(
                key, 'period_key', 'country_key', 'commodity_key', 'scenario_code',
                'ending_stock_thousand_bags', 'bag_weight_kg', 'ending_stock_tonnes',
                'observation_set_id', F.col('dataset_id').alias('record_source_code'))
        else:
            grain = ['country_m49_code', 'calendar_year', 'indicator_code',
                     'observation_set_id']
            base = _country(_period(source), 'country_m49_code', 'country_key')
            base = base.withColumn('macro_indicator_key', hashed('indicator_code'))
            base = _key(base, grain, key).select(
                key, 'period_key', 'country_key', 'macro_indicator_key',
                F.col('indicator_value_canonical').alias('indicator_value'),
                F.col('value_status').alias('observation_status_code'),
                'observation_set_id', F.col('dataset_id').alias('record_source_code'))
        base = _common(base)
        counts[name] = insert_new(spark, fact_namespace, name, base, [key])
        if name == 'fact_trade_partner':
            flags = base.select(key, 'quantity_estimated_flag', 'value_estimated_flag')
            current = spark.table(f'lakehouse.{fact_namespace}.fact_trade_partner').select(
                key, 'quantity_estimated_flag', 'value_estimated_flag')
            changed = flags.alias('s').join(current.alias('t'), key).filter(
                ~F.col('s.quantity_estimated_flag').eqNullSafe(F.col('t.quantity_estimated_flag')) |
                ~F.col('s.value_estimated_flag').eqNullSafe(F.col('t.value_estimated_flag')))
            if changed.limit(1).count():
                changed.select(F.col(f's.{key}').alias(key),
                               F.col('s.quantity_estimated_flag').alias('quantity_estimated_flag'),
                               F.col('s.value_estimated_flag').alias('value_estimated_flag'))\
                    .createOrReplaceTempView('_gold_partner_flags')
                spark.sql(f"MERGE INTO lakehouse.{fact_namespace}.fact_trade_partner t "
                          "USING _gold_partner_flags s ON t.trade_partner_key=s.trade_partner_key "
                          "WHEN MATCHED THEN UPDATE SET "
                          "t.quantity_estimated_flag=s.quantity_estimated_flag, "
                          "t.value_estimated_flag=s.value_estimated_flag")
        # Observation-level lineage is materialized after the fact write.
        # The join uses the source grain, never the measure value.
        source_key = _key(source, grain, key)
        _lineage(spark, source_key, name, key,
                 snapshots[f'lakehouse.silver_core.{domain}'], batch_id,
                 lineage_namespace)
    return {'gold_batch': batch_id, 'rows_inserted': counts}
