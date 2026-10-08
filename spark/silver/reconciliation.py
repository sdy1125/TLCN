"""Opt-in, definition-compatible comparisons; never modify source observations."""

from pyspark.sql import functions as F


def compare(left, right, keys, *, definition_confirmed=False):
    if not definition_confirmed:
        raise ValueError(
            "Confirm identical commodity definition, grain and units before comparison"
        )
    join_keys = list(
        dict.fromkeys(keys + ["measure_code", "unit_standard", "frequency"])
    )
    for frame in (left, right):
        if frame.groupBy(*join_keys).count().filter("count > 1").limit(1).count():
            raise ValueError("Reconciliation inputs must already share a unique grain")
    a = left.select(*join_keys, F.col("value_standard").alias("left_value"))
    b = right.select(*join_keys, F.col("value_standard").alias("right_value"))
    return (
        a.join(b, join_keys, "full")
        .withColumn("absolute_variance", F.col("left_value") - F.col("right_value"))
        .withColumn(
            "relative_variance",
            F.when(
                F.col("right_value") != 0,
                (F.col("left_value") - F.col("right_value")) / F.abs("right_value"),
            ),
        )
    )


def run(spark, namespace, run_id):
    """Write auditable comparability decisions and only approved variances."""
    import json
    from datetime import datetime
    from .registry import load, SOURCES
    from .writer import CONTROL, upsert_control

    now = datetime.utcnow()
    for pair in load('reconciliation')['pairs']:
        left_id, right_id = pair['left'], pair['right']
        target = SOURCES[left_id]['target']
        check = 'reconciliation:' + left_id + ':' + right_id
        if not pair['definition_confirmed']:
            row = (run_id, left_id, 'not_evaluated', target, check, 'source_pair',
                   'WARNING', 'NOT_EVALUATED', None, 'approved compatible definitions',
                   None, json.dumps(pair), now)
            result = spark.createDataFrame([row], CONTROL['dq_results'])
        else:
            left = spark.table(namespace + '.' + target).filter(F.col('dataset_id') == left_id)
            right = spark.table(namespace + '.' + SOURCES[right_id]['target']).filter(F.col('dataset_id') == right_id)
            for field, value in pair.get('filters', {}).items():
                left = left.filter(F.col(field) == F.lit(value))
                right = right.filter(F.col(field) == F.lit(value))
            compared = compare(left, right, pair['keys'], definition_confirmed=True)
            threshold = float(pair['relative_warning_threshold'])
            details = F.to_json(F.struct(*[F.col(c) for c in compared.columns]))
            warning = F.col('left_value').isNull() | F.col('right_value').isNull() | (F.abs('relative_variance') > threshold)
            result = compared.select(
                F.lit(run_id).alias('run_id'), F.lit(left_id).alias('dataset_id'),
                F.lit('cross_source').alias('bronze_ingestion_id'), F.lit(target).alias('target_contract'),
                F.concat(F.lit(check + ':'), F.sha2(details, 256)).alias('check_name'),
                F.lit('source_pair_key').alias('check_scope'),
                F.when(warning, 'WARNING').otherwise('INFO').alias('severity'),
                F.when(warning, 'WARNING').otherwise('PASS').alias('status'),
                F.col('relative_variance').cast('string').alias('observed_value'),
                F.lit('definition-compatible comparison only').alias('expected_value'),
                F.lit(threshold).alias('threshold'), details.alias('details_json'),
                F.lit(now).alias('checked_at'))
        upsert_control(spark, namespace, 'dq_results', result, ['run_id', 'dataset_id', 'check_name'])
