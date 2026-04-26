from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from supply_chain_ai.config import PipelineConfig


def run_validation_checks(demand: DataFrame, inventory_fulfillment: DataFrame, config: PipelineConfig) -> DataFrame:
    w = Window.partitionBy("store_id", "item_id").orderBy("date").rowsBetween(-config.rolling_window_days, -1)

    demand_enriched = demand.withColumn("rolling_demand", F.avg("units_sold").over(w))

    spikes = (
        demand_enriched.filter(F.col("rolling_demand").isNotNull())
        .filter(F.col("units_sold") > F.col("rolling_demand") * F.lit(config.demand_spike_multiplier))
        .select(
            F.sha2(F.concat_ws("|", F.lit("demand_spike"), "date", "store_id", "item_id"), 256).alias("anomaly_id"),
            "date",
            "store_id",
            "item_id",
            F.lit("demand_spike").alias("rule_name"),
            F.lit("high").alias("severity"),
            F.col("units_sold").alias("metric_value"),
            F.col("rolling_demand").alias("expected_value"),
            F.lit("Demand significantly above rolling baseline.").alias("details"),
        )
    )

    drops = (
        demand_enriched.filter(F.col("rolling_demand").isNotNull())
        .filter(F.col("units_sold") < F.col("rolling_demand") * F.lit(config.demand_drop_multiplier))
        .select(
            F.sha2(F.concat_ws("|", F.lit("demand_drop"), "date", "store_id", "item_id"), 256).alias("anomaly_id"),
            "date",
            "store_id",
            "item_id",
            F.lit("demand_drop").alias("rule_name"),
            F.lit("medium").alias("severity"),
            F.col("units_sold").alias("metric_value"),
            F.col("rolling_demand").alias("expected_value"),
            F.lit("Demand significantly below rolling baseline.").alias("details"),
        )
    )

    low_inventory = (
        inventory_fulfillment.filter(
            F.col("closing_stock") < F.col("forecast_units") * F.lit(config.low_inventory_cover_days)
        )
        .select(
            F.sha2(F.concat_ws("|", F.lit("low_inventory"), "date", "store_id", "item_id"), 256).alias("anomaly_id"),
            "date",
            "store_id",
            "item_id",
            F.lit("low_inventory").alias("rule_name"),
            F.lit("high").alias("severity"),
            F.col("closing_stock").cast("double").alias("metric_value"),
            (F.col("forecast_units") * F.lit(config.low_inventory_cover_days)).alias("expected_value"),
            F.lit("Inventory coverage below threshold.").alias("details"),
        )
    )

    fulfillment_drop = (
        inventory_fulfillment.filter(F.col("fulfillment_rate") < F.lit(config.min_fulfillment_rate))
        .select(
            F.sha2(F.concat_ws("|", F.lit("fulfillment_drop"), "date", "store_id", "item_id"), 256).alias("anomaly_id"),
            "date",
            "store_id",
            "item_id",
            F.lit("fulfillment_drop").alias("rule_name"),
            F.lit("high").alias("severity"),
            F.col("fulfillment_rate").alias("metric_value"),
            F.lit(config.min_fulfillment_rate).alias("expected_value"),
            F.lit("Fulfillment rate below SLA threshold.").alias("details"),
        )
    )

    missing_price = (
        demand.filter(F.col("price").isNull())
        .select(
            F.sha2(F.concat_ws("|", F.lit("missing_price"), "date", "store_id", "item_id"), 256).alias("anomaly_id"),
            "date",
            "store_id",
            "item_id",
            F.lit("missing_price").alias("rule_name"),
            F.lit("medium").alias("severity"),
            F.lit(None).cast("double").alias("metric_value"),
            F.lit(None).cast("double").alias("expected_value"),
            F.lit("Missing sell price for date/item/store record.").alias("details"),
        )
    )

    return spikes.unionByName(drops).unionByName(low_inventory).unionByName(fulfillment_drop).unionByName(missing_price)
