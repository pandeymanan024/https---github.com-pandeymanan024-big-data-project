from pathlib import Path

import pandas as pd

from supply_chain_ai.agent.tools import AgentDataTools
from supply_chain_ai.agent.workflow import SupplyChainDiagnosisAgent


def test_agent_diagnosis_roundtrip(tmp_path: Path) -> None:
    validation = pd.DataFrame(
        [
            {
                "anomaly_id": "a1",
                "date": "2016-01-10",
                "store_id": "CA_1",
                "item_id": "FOODS_1_001",
                "rule_name": "low_inventory",
                "severity": "high",
                "details": "Inventory coverage below threshold.",
            }
        ]
    )
    inventory = pd.DataFrame(
        [
            {
                "date": "2016-01-10",
                "store_id": "CA_1",
                "item_id": "FOODS_1_001",
                "closing_stock": 5,
                "forecast_units": 18.0,
            }
        ]
    )
    recs = pd.DataFrame(
        [
            {
                "anomaly_id": "a1",
                "recommendation": "Increase replenishment for FOODS_1_001 at CA_1 by 40 units",
            }
        ]
    )

    validation.to_parquet(tmp_path / "validation_logs.parquet", index=False)
    inventory.to_parquet(tmp_path / "inventory_fulfillment.parquet", index=False)
    recs.to_parquet(tmp_path / "recommendations.parquet", index=False)

    tools = AgentDataTools(tmp_path)
    agent = SupplyChainDiagnosisAgent(tools)
    result = agent.diagnose("a1")

    assert "Root Cause Analysis" in result["explanation"]
    assert "Recommendations" in result["explanation"]
