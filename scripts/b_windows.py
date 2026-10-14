"""B: make one forecasting example per observed station and five-minute time.

The timestamp-as-interval-end convention is inherited from DATA_CONTRACT_V1.
At time t we assume the reading stamped t has become available.
"""

from decimal import Decimal, InvalidOperation

from pyspark.sql import Window, functions as F


THRESHOLDS_MM = ("0.000000000", "1.000000000", "5.000000000")


def add_windows(observations):
    """Keep exact time windows; missing slots are never replaced by zero."""
    obs = observations.select("station_id", "observation_time", "rainfall_mm")
    order = Window.partitionBy("station_id").orderBy("observation_time")
    previous_3 = order.rowsBetween(-2, 0)
    previous_6 = order.rowsBetween(-5, 0)
    previous_12 = order.rowsBetween(-11, 0)
    next_6 = order.rowsBetween(1, 6)
    seconds = F.unix_timestamp("observation_time")

    result = (obs
        .withColumn("past_15_mm", F.sum("rainfall_mm").over(previous_3))
        .withColumn("past_30_mm", F.sum("rainfall_mm").over(previous_6))
        .withColumn("past_60_mm", F.sum("rainfall_mm").over(previous_12))
        .withColumn("future_30_mm", F.sum("rainfall_mm").over(next_6))
        .withColumn("past_3_count", F.count("rainfall_mm").over(previous_3))
        .withColumn("past_6_count", F.count("rainfall_mm").over(previous_6))
        .withColumn("past_12_count", F.count("rainfall_mm").over(previous_12))
        .withColumn("future_6_count", F.count("rainfall_mm").over(next_6))
        .withColumn("past_3_start", F.lag("observation_time", 2).over(order))
        .withColumn("past_6_start", F.lag("observation_time", 5).over(order))
        .withColumn("past_12_start", F.lag("observation_time", 11).over(order))
        .withColumn("future_1_time", F.lead("observation_time", 1).over(order))
        .withColumn("future_6_time", F.lead("observation_time", 6).over(order)))

    # One row per station and grid time is guaranteed by A's formal data contract.
    # With 5-minute grid times and six ordered rows, exact first/last offsets
    # imply all intervening slots exist. Count also rejects null readings.
    result = (result
        .withColumn("past_15_complete", (F.col("past_3_count") == 3) &
                    (seconds - F.unix_timestamp("past_3_start") == 600))
        .withColumn("past_30_complete", (F.col("past_6_count") == 6) &
                    (seconds - F.unix_timestamp("past_6_start") == 1500))
        .withColumn("past_60_complete", (F.col("past_12_count") == 12) &
                    (seconds - F.unix_timestamp("past_12_start") == 3300))
        .withColumn("future_complete", (F.col("future_6_count") == 6) &
                    (F.unix_timestamp("future_1_time") - seconds == 300) &
                    (F.unix_timestamp("future_6_time") - seconds == 1800)))
    for amount, complete in (("past_15_mm", "past_15_complete"),
                             ("past_30_mm", "past_30_complete"),
                             ("past_60_mm", "past_60_complete"),
                             ("future_30_mm", "future_complete")):
        result = result.withColumn(amount, F.when(F.col(complete), F.col(amount)))

    result = result.withColumn(
        "split", F.when((F.col("observation_time") >= F.lit("2017-01-01 00:00:00").cast("timestamp")) &
                         (F.col("observation_time") < F.lit("2023-01-01 00:00:00").cast("timestamp")), "train")
        .when((F.col("observation_time") >= F.lit("2023-01-01 00:00:00").cast("timestamp")) &
              (F.col("observation_time") < F.lit("2024-01-01 00:00:00").cast("timestamp")), "validation")
        .when((F.col("observation_time") >= F.lit("2024-01-01 00:00:00").cast("timestamp")) &
              (F.col("observation_time") < F.lit("2025-01-01 00:00:00").cast("timestamp")), "test"))
    split_end = (F.when(F.col("split") == "train", F.lit("2023-01-01 00:00:00").cast("timestamp"))
                 .when(F.col("split") == "validation", F.lit("2024-01-01 00:00:00").cast("timestamp"))
                 .when(F.col("split") == "test", F.lit("2025-01-01 00:00:00").cast("timestamp")))
    return (result
        .withColumn("label_usable", F.coalesce(F.col("future_complete") &
                    (F.col("future_6_time") < split_end), F.lit(False)))
        .withColumn("features_usable", F.col("past_60_complete"))
        .withColumn("month", F.month("observation_time"))
        .withColumn("hour", F.hour("observation_time"))
        .select("station_id", "observation_time", "split", "month", "hour",
                F.col("rainfall_mm").alias("current_5_mm"), "past_15_mm", "past_30_mm",
                "past_60_mm", "future_30_mm", "future_complete", "label_usable",
                "features_usable"))


def add_thresholds(examples, thresholds_mm=THRESHOLDS_MM):
    """Expand a chosen threshold grid only when needed for model training."""
    try:
        amounts = [Decimal(str(q)) for q in thresholds_mm]
        if not amounts or any(not q.is_finite() or q < 0 or q >= Decimal("100000000000")
                              or q != q.quantize(Decimal("0.000000001")) for q in amounts):
            raise ValueError("Thresholds must be nonnegative Decimal(20,9) amounts in mm")
    except (InvalidOperation, TypeError) as error:
        raise ValueError("Invalid rainfall threshold") from error
    values = [F.lit(format(q, ".9f")).cast("decimal(20,9)") for q in amounts]
    return (examples.withColumn("q_mm", F.explode(F.array(*values)))
            .withColumn("label", (F.col("future_30_mm") > F.col("q_mm")).cast("integer")))
