from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def generate_recommendations(validation_logs: DataFrame, inventory_fulfillment: DataFrame) -> DataFrame:
    latest_w = Window.partitionBy("store_id", "item_id").orderBy(F.col("date").desc())

    latest_inventory = (
        inventory_fulfillment.withColumn("rn", F.row_number().over(latest_w))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    rec_base = validation_logs.join(
        latest_inventory.select(
            "store_id",
            "item_id",
            "date",
            "closing_stock",
            "forecast_units",
            "dept_id",
            "cat_id",
        ).withColumnRenamed("date", "latest_date"),
        on=["store_id", "item_id"],
        how="left",
    )

    action_type = (
        F.when(F.col("rule_name") == "low_inventory", F.lit("inventory_restock"))
        .when(F.col("rule_name") == "fulfillment_drop", F.lit("store_allocation"))
        .when(F.col("rule_name") == "demand_spike", F.lit("demand_response"))
        .when(F.col("rule_name") == "missing_price", F.lit("data_quality_fix"))
        .otherwise(F.lit("substitution_suggestion"))
    )

    restock_qty = F.greatest(F.lit(0.0), F.ceil(F.col("forecast_units") * F.lit(3.0) - F.col("closing_stock")))

    recommendation_text = (
        F.when(
            F.col("rule_name") == "low_inventory",
            F.concat_ws(
                " ",
                F.lit("Increase replenishment for"),
                F.col("item_id"),
                F.lit("at"),
                F.col("store_id"),
                F.lit("by"),
                restock_qty.cast("int"),
                F.lit("units to restore 3-day forecast cover."),
            ),
        )
        .when(
            F.col("rule_name") == "fulfillment_drop",
            F.concat_ws(
                " ",
                F.lit("Rebalance inventory to"),
                F.col("store_id"),
                F.lit("for"),
                F.col("item_id"),
                F.lit("and prioritize fast replenishment lanes."),
            ),
        )
        .when(
            F.col("rule_name") == "demand_spike",
            F.concat_ws(
                " ",
                F.lit("Activate demand surge plan for"),
                F.col("item_id"),
                F.lit("in"),
                F.col("store_id"),
                F.lit("(temporary safety stock and expedited reorder)."),
            ),
        )
        .when(
            F.col("rule_name") == "missing_price",
            F.concat_ws(
                " ",
                F.lit("Backfill missing price records for"),
                F.col("item_id"),
                F.lit("in"),
                F.col("store_id"),
                F.lit("and validate upstream pricing feed."),
            ),
        )
        .otherwise(
            F.concat_ws(
                " ",
                F.lit("Suggest substitute items from category"),
                F.col("cat_id"),
                F.lit("for"),
                F.col("item_id"),
                F.lit("at"),
                F.col("store_id"),
                F.lit("to reduce lost sales."),
            )
        )
    )

    return rec_base.select(
        "anomaly_id",
        "date",
        "store_id",
        "item_id",
        action_type.alias("action_type"),
        recommendation_text.alias("recommendation"),
    ).dropDuplicates(["anomaly_id", "action_type"])
