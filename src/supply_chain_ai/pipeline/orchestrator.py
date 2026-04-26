from __future__ import annotations

from pathlib import Path

from supply_chain_ai.config import PipelineConfig
from supply_chain_ai.pipeline.data_transform import build_demand_timeseries, load_raw_m5
from supply_chain_ai.pipeline.forecast import build_forecast
from supply_chain_ai.pipeline.simulation import SimulationParams, simulate_inventory_fulfillment
from supply_chain_ai.recommendation.engine import generate_recommendations
from supply_chain_ai.utils.spark import build_spark_session
from supply_chain_ai.validation.rules import run_validation_checks


class SupplyChainPipeline:
    def __init__(self, config: PipelineConfig | None = None) -> None:
        self.config = config or PipelineConfig()

    def run(self, raw_data_dir: Path, output_dir: Path) -> dict[str, str]:
        output_dir.mkdir(parents=True, exist_ok=True)
        spark = build_spark_session(self.config.spark_app_name)

        try:
            sales, calendar, prices = load_raw_m5(spark, raw_data_dir)
            demand = build_demand_timeseries(sales, calendar, prices)
            forecast = build_forecast(demand, self.config.rolling_window_days)

            sim_params = SimulationParams(
                initial_stock=self.config.initial_stock,
                reorder_point=self.config.reorder_point,
                reorder_qty=self.config.reorder_qty,
                replenishment_lead_days=self.config.replenishment_lead_days,
            )
            inventory = simulate_inventory_fulfillment(demand, forecast, sim_params)

            validation_logs = run_validation_checks(demand, inventory, self.config)
            recommendations = generate_recommendations(validation_logs, inventory)

            paths = {
                "processed_demand": str(output_dir / "processed_demand.parquet"),
                "forecast": str(output_dir / "forecast.parquet"),
                "inventory_fulfillment": str(output_dir / "inventory_fulfillment.parquet"),
                "validation_logs": str(output_dir / "validation_logs.parquet"),
                "recommendations": str(output_dir / "recommendations.parquet"),
            }

            demand.write.mode("overwrite").parquet(paths["processed_demand"])
            forecast.write.mode("overwrite").parquet(paths["forecast"])
            inventory.write.mode("overwrite").parquet(paths["inventory_fulfillment"])
            validation_logs.write.mode("overwrite").parquet(paths["validation_logs"])
            recommendations.write.mode("overwrite").parquet(paths["recommendations"])

            return paths
        finally:
            spark.stop()
