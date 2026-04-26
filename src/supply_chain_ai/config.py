from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineConfig:
    spark_app_name: str = "SupplyChainAgenticAI"
    rolling_window_days: int = 14
    demand_spike_multiplier: float = 1.8
    demand_drop_multiplier: float = 0.45
    low_inventory_cover_days: float = 1.5
    min_fulfillment_rate: float = 0.90

    initial_stock: int = 150
    reorder_point: int = 40
    reorder_qty: int = 120
    replenishment_lead_days: int = 2
