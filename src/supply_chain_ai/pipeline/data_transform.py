from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F


def load_raw_m5(spark: SparkSession, raw_data_dir: Path) -> tuple[DataFrame, DataFrame, DataFrame]:
    sales = spark.read.option("header", True).option("inferSchema", True).csv(
        str(raw_data_dir / "sales_train_validation.csv")
    )
    calendar = spark.read.option("header", True).option("inferSchema", True).csv(
        str(raw_data_dir / "calendar.csv")
    )
    prices = spark.read.option("header", True).option("inferSchema", True).csv(
        str(raw_data_dir / "sell_prices.csv")
    )
    return sales, calendar, prices


def build_demand_timeseries(sales: DataFrame, calendar: DataFrame, prices: DataFrame) -> DataFrame:
    d_cols = [c for c in sales.columns if c.startswith("d_")]
    if not d_cols:
        raise ValueError("No d_* columns found in sales table.")

    stack_expr = "stack({n}, {pairs}) as (d, units_sold)".format(
        n=len(d_cols),
        pairs=", ".join([f"'{c}', `{c}`" for c in d_cols]),
    )

    long_sales = sales.selectExpr(
        "item_id",
        "dept_id",
        "cat_id",
        "store_id",
        "state_id",
        stack_expr,
    )

    demand = (
        long_sales.join(calendar.select("d", "date", "wm_yr_wk", "event_name_1", "event_type_1"), on="d", how="left")
        .join(prices.select("store_id", "item_id", "wm_yr_wk", F.col("sell_price").alias("price")), on=["store_id", "item_id", "wm_yr_wk"], how="left")
        .withColumn("date", F.to_date("date"))
        .withColumn("units_sold", F.col("units_sold").cast("double"))
        .withColumn("price", F.col("price").cast("double"))
        .withColumn("is_promo", F.when(F.col("event_name_1").isNotNull(), F.lit(1)).otherwise(F.lit(0)))
        .select(
            "date",
            "d",
            "store_id",
            "state_id",
            "item_id",
            "dept_id",
            "cat_id",
            "units_sold",
            "price",
            "event_name_1",
            "event_type_1",
            "is_promo",
        )
    )

    return demand
