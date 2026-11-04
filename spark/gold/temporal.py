"""Resolve a Fact's dimension key for its complete observation period."""

from pyspark.sql import Window, functions as F


def _min_date():
    return F.to_date(F.lit("0001-01-01"))


def _max_date():
    return F.to_date(F.lit("9999-12-31"))


def period_bounds(frame):
    """Attach complete DAY/MONTH/YEAR intervals, never a single midpoint."""
    code = F.col("period_code")
    start = (F.when(F.col("period_type") == "DAY", F.to_date(code))
             .when(F.col("period_type") == "MONTH",
                   F.to_date(F.concat(code, F.lit("-01"))))
             .when(F.col("period_type") == "YEAR",
                   F.to_date(F.concat(code, F.lit("-01-01")))))
    end = (F.when(F.col("period_type") == "DAY", start)
           .when(F.col("period_type") == "MONTH", F.last_day(start))
           .when(F.col("period_type") == "YEAR",
                 F.to_date(F.concat(code, F.lit("-12-31")))))
    return frame.withColumn("_period_start", start).withColumn("_period_end", end)


def validate_intervals(dimensions, code_column):
    """Reject invalid bounds and overlapping versions of one business code."""
    invalid = dimensions.filter(
        F.col("valid_from").isNotNull() & F.col("valid_to").isNotNull() &
        (F.col("valid_from") >= F.col("valid_to")))
    if invalid.limit(1).count():
        raise ValueError(f"Invalid validity interval: {code_column}")
    window = Window.partitionBy(code_column).orderBy(
        F.coalesce(F.col("valid_from"), _min_date()),
        F.coalesce(F.col("valid_to"), _max_date()))
    ordered = (
        dimensions
        .withColumn("_start", F.coalesce(F.col("valid_from"), _min_date()))
        .withColumn("_end", F.coalesce(F.col("valid_to"), _max_date()))
        .withColumn("_previous_end", F.lag("_end").over(window))
    )
    if ordered.filter(F.col("_start") < F.col("_previous_end")).limit(1).count():
        raise ValueError(f"Overlapping dimension versions: {code_column}")


def attach_key(source, dimensions, source_code, dimension_code, key_column):
    """Map each observation to exactly one dimension covering its full period."""
    validate_intervals(dimensions, dimension_code)
    left = period_bounds(source).alias("s")
    right = dimensions.select(
        F.col(dimension_code).alias("_dim_code"),
        F.col(key_column).alias("_dim_key"),
        "valid_from", "valid_to").alias("d")
    condition = (
        F.col(f"s.{source_code}") == F.col("d._dim_code")
    ) & (F.col("s._period_start") >= F.coalesce(F.col("d.valid_from"), _min_date())) & (
        F.col("s._period_end") < F.coalesce(F.col("d.valid_to"), _max_date()))
    matched = left.join(right, condition, "left")
    invalid = matched.filter(
        F.col("s._period_start").isNull() | F.col("s._period_end").isNull() |
        F.col("d._dim_key").isNull()).limit(1).count()
    duplicate = (
        matched.groupBy(F.col("s.observation_id")).count()
        .filter("count <> 1").limit(1).count()
    )
    if invalid or duplicate:
        raise ValueError(
            f"Temporal mapping requires exactly one {dimension_code} for every observation")
    return (
        matched.select("s.*", F.col("d._dim_key").alias(key_column))
        .drop("_period_start", "_period_end")
    )
