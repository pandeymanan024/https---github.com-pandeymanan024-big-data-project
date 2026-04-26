from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


@dataclass(frozen=True)
class SimulationParams:
    initial_stock: int
    reorder_point: int
    reorder_qty: int
    replenishment_lead_days: int


def simulate_inventory_fulfillment(demand: DataFrame, forecast: DataFrame, params: SimulationParams) -> DataFrame:
    joined = (
        demand.join(forecast, on=["date", "store_id", "item_id"], how="left")
        .select(
            "date",
            "store_id",
            "state_id",
            "item_id",
            "dept_id",
            "cat_id",
            "units_sold",
            F.coalesce("forecast_units", F.col("units_sold")).alias("forecast_units"),
        )
        .orderBy("store_id", "item_id", "date")
    )

    schema = StructType(
        [
            StructField("date", DateType(), False),
            StructField("store_id", StringType(), False),
            StructField("state_id", StringType(), True),
            StructField("item_id", StringType(), False),
            StructField("dept_id", StringType(), True),
            StructField("cat_id", StringType(), True),
            StructField("demand", DoubleType(), False),
            StructField("forecast_units", DoubleType(), False),
            StructField("opening_stock", IntegerType(), False),
            StructField("replenished_qty", IntegerType(), False),
            StructField("fulfilled_qty", DoubleType(), False),
            StructField("closing_stock", IntegerType(), False),
            StructField("stockout_flag", IntegerType(), False),
            StructField("fulfillment_rate", DoubleType(), False),
        ]
    )

    def _simulate(pdf: pd.DataFrame) -> pd.DataFrame:
        pdf = pdf.sort_values("date").reset_index(drop=True)
        stock = int(params.initial_stock)
        pending_orders: list[tuple[int, int]] = []

        rows = []
        for idx, row in pdf.iterrows():
            replenished = sum(q for (arrival_idx, q) in pending_orders if arrival_idx == idx)
            pending_orders = [(a, q) for (a, q) in pending_orders if a != idx]
            stock += replenished

            opening = stock
            demand_qty = float(row["units_sold"] or 0.0)
            forecast_qty = float(row["forecast_units"] or demand_qty)

            if stock <= params.reorder_point:
                pending_orders.append((idx + params.replenishment_lead_days, int(params.reorder_qty)))

            fulfilled = min(float(stock), demand_qty)
            stock = int(max(0.0, stock - fulfilled))
            closing = stock
            stockout = int(demand_qty > fulfilled)
            rate = float(fulfilled / demand_qty) if demand_qty > 0 else 1.0

            rows.append(
                {
                    "date": row["date"],
                    "store_id": row["store_id"],
                    "state_id": row.get("state_id"),
                    "item_id": row["item_id"],
                    "dept_id": row.get("dept_id"),
                    "cat_id": row.get("cat_id"),
                    "demand": demand_qty,
                    "forecast_units": forecast_qty,
                    "opening_stock": int(opening),
                    "replenished_qty": int(replenished),
                    "fulfilled_qty": float(fulfilled),
                    "closing_stock": int(closing),
                    "stockout_flag": int(stockout),
                    "fulfillment_rate": float(rate),
                }
            )

        return pd.DataFrame(rows)

    return joined.groupBy("store_id", "item_id").applyInPandas(_simulate, schema=schema)
