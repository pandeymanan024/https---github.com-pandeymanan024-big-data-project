from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def build_forecast(demand: DataFrame, rolling_window_days: int) -> DataFrame:
    w = (
        Window.partitionBy("store_id", "item_id")
        .orderBy("date")
        .rowsBetween(-rolling_window_days, -1)
    )

    forecast = (
        demand.withColumn("rolling_mean", F.avg("units_sold").over(w))
        .withColumn(
            "forecast_units",
            F.coalesce(F.col("rolling_mean"), F.col("units_sold"), F.lit(0.0)),
        )
        .select("date", "store_id", "item_id", "forecast_units")
    )

    return forecast
