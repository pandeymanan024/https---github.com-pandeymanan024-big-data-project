from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


def generate_synthetic_m5_like_dataset(output_dir: Path, num_days: int = 120) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    rng = np.random.default_rng(42)

    stores = ["CA_1", "TX_1", "WI_1"]
    items = [
        ("FOODS_1_001", "FOODS_1", "FOODS", "CA"),
        ("FOODS_1_002", "FOODS_1", "FOODS", "TX"),
        ("HOBBIES_1_001", "HOBBIES_1", "HOBBIES", "WI"),
        ("HOUSEHOLD_1_001", "HOUSEHOLD_1", "HOUSEHOLD", "CA"),
    ]

    start = pd.Timestamp("2016-01-01")
    dates = pd.date_range(start, periods=num_days, freq="D")
    d_keys = [f"d_{i}" for i in range(1, num_days + 1)]

    # Calendar table
    calendar = pd.DataFrame(
        {
            "d": d_keys,
            "date": dates.astype(str),
            "wm_yr_wk": [(i // 7) + 1 for i in range(num_days)],
            "event_name_1": ["Promo" if i % 30 in (5, 6, 7) else None for i in range(num_days)],
            "event_type_1": ["Cultural" if i % 30 in (5, 6, 7) else None for i in range(num_days)],
        }
    )

    # Sell prices table
    price_rows = []
    for store in stores:
        for item_id, _, _, _ in items:
            base_price = rng.uniform(2.5, 15.0)
            for wk in calendar["wm_yr_wk"].unique():
                price = max(0.5, base_price + rng.normal(0, 0.5))
                if wk % 13 == 0:
                    price *= 0.9  # periodic promotion
                price_rows.append((store, item_id, int(wk), round(float(price), 2)))

    sell_prices = pd.DataFrame(
        price_rows, columns=["store_id", "item_id", "wm_yr_wk", "sell_price"]
    )

    # Sales table (wide, M5 style)
    sales_rows = []
    for item_id, dept_id, cat_id, state_id in items:
        for store in stores:
            base_demand = rng.uniform(8, 30)
            trend = rng.uniform(-0.05, 0.12)

            series = []
            for t in range(num_days):
                weekly = 1.0 + 0.25 * np.sin(2 * np.pi * (t % 7) / 7)
                promo_boost = 1.6 if t % 30 in (5, 6, 7) else 1.0
                spike = 2.2 if (store == "TX_1" and item_id == "FOODS_1_001" and t in (60, 61)) else 1.0
                lam = max(0.2, (base_demand + trend * t) * weekly * promo_boost * spike)
                qty = rng.poisson(lam)
                series.append(int(qty))

            sales_rows.append(
                [f"{item_id}_{store}_validation", item_id, dept_id, cat_id, store, state_id, *series]
            )

    sales_cols = ["id", "item_id", "dept_id", "cat_id", "store_id", "state_id", *d_keys]
    sales = pd.DataFrame(sales_rows, columns=sales_cols)

    sales.to_csv(output_dir / "sales_train_validation.csv", index=False)
    calendar.to_csv(output_dir / "calendar.csv", index=False)
    sell_prices.to_csv(output_dir / "sell_prices.csv", index=False)
