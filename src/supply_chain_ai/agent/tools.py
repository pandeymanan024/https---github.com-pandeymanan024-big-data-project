from __future__ import annotations

from pathlib import Path

import pandas as pd


class AgentDataTools:
    def __init__(self, artifacts_dir: Path) -> None:
        self.artifacts_dir = artifacts_dir
        self.validation_logs = pd.read_parquet(artifacts_dir / "validation_logs.parquet")
        self.inventory = pd.read_parquet(artifacts_dir / "inventory_fulfillment.parquet")
        self.recommendations = pd.read_parquet(artifacts_dir / "recommendations.parquet")

    def get_top_anomaly_id(self) -> str:
        if self.validation_logs.empty:
            raise ValueError("No anomalies found in validation logs.")
        return str(self.validation_logs.iloc[0]["anomaly_id"])

    def get_anomaly(self, anomaly_id: str) -> dict:
        row = self.validation_logs[self.validation_logs["anomaly_id"] == anomaly_id]
        if row.empty:
            raise ValueError(f"Anomaly id not found: {anomaly_id}")
        return row.iloc[0].to_dict()

    def get_latest_inventory_context(self, store_id: str, item_id: str) -> dict:
        subset = self.inventory[(self.inventory["store_id"] == store_id) & (self.inventory["item_id"] == item_id)]
        if subset.empty:
            return {}
        latest = subset.sort_values("date").iloc[-1]
        return latest.to_dict()

    def get_recommendations_for_anomaly(self, anomaly_id: str) -> list[str]:
        rec = self.recommendations[self.recommendations["anomaly_id"] == anomaly_id]
        if rec.empty:
            return []
        return rec["recommendation"].astype(str).tolist()
