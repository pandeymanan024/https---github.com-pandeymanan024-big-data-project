from __future__ import annotations

from typing import TypedDict

from langgraph.graph import END, START, StateGraph

from supply_chain_ai.agent.tools import AgentDataTools


class AgentState(TypedDict, total=False):
    anomaly_id: str
    anomaly: dict
    context: dict
    root_cause: str
    recommendations: list[str]
    explanation: str


class SupplyChainDiagnosisAgent:
    def __init__(self, tools: AgentDataTools) -> None:
        self.tools = tools
        self.graph = self._build_graph()

    def _build_graph(self):
        graph = StateGraph(AgentState)
        graph.add_node("fetch_anomaly", self._fetch_anomaly)
        graph.add_node("trace_context", self._trace_context)
        graph.add_node("infer_root_cause", self._infer_root_cause)
        graph.add_node("fetch_recommendations", self._fetch_recommendations)
        graph.add_node("compose_explanation", self._compose_explanation)

        graph.add_edge(START, "fetch_anomaly")
        graph.add_edge("fetch_anomaly", "trace_context")
        graph.add_edge("trace_context", "infer_root_cause")
        graph.add_edge("infer_root_cause", "fetch_recommendations")
        graph.add_edge("fetch_recommendations", "compose_explanation")
        graph.add_edge("compose_explanation", END)

        return graph.compile()

    def _fetch_anomaly(self, state: AgentState) -> AgentState:
        anomaly_id = state["anomaly_id"]
        return {"anomaly": self.tools.get_anomaly(anomaly_id)}

    def _trace_context(self, state: AgentState) -> AgentState:
        anomaly = state["anomaly"]
        ctx = self.tools.get_latest_inventory_context(
            store_id=str(anomaly["store_id"]),
            item_id=str(anomaly["item_id"]),
        )
        return {"context": ctx}

    def _infer_root_cause(self, state: AgentState) -> AgentState:
        anomaly = state["anomaly"]
        context = state.get("context", {})
        rule = str(anomaly.get("rule_name", "unknown"))

        if rule == "low_inventory":
            rc = "Inventory coverage is insufficient for expected near-term demand. Reorder trigger and lead-time policy are likely too conservative."
        elif rule == "fulfillment_drop":
            rc = "Fulfillment degradation indicates stock mismatch or replenishment latency at the store-item level."
        elif rule == "demand_spike":
            rc = "Observed demand significantly exceeds historical baseline, likely due to promotion/event-driven uplift."
        elif rule == "demand_drop":
            rc = "Demand has fallen below baseline, indicating potential over-forecasting or post-promotion normalization."
        elif rule == "missing_price":
            rc = "Upstream pricing feed has missing records, causing incomplete commercial context in demand analysis."
        else:
            rc = "An anomaly was detected but no specific rule-based root cause mapping exists."

        if context:
            rc += f" Current closing stock: {context.get('closing_stock', 'NA')}, latest forecast: {round(float(context.get('forecast_units', 0.0)), 2)}."

        return {"root_cause": rc}

    def _fetch_recommendations(self, state: AgentState) -> AgentState:
        anomaly_id = state["anomaly_id"]
        return {"recommendations": self.tools.get_recommendations_for_anomaly(anomaly_id)}

    def _compose_explanation(self, state: AgentState) -> AgentState:
        anomaly = state["anomaly"]
        recommendations = state.get("recommendations", [])
        recommendation_text = "\n".join([f"- {r}" for r in recommendations]) if recommendations else "- No recommendation generated"

        explanation = (
            f"Anomaly ID: {state['anomaly_id']}\n"
            f"Rule: {anomaly.get('rule_name')}\n"
            f"Date: {anomaly.get('date')}\n"
            f"Store: {anomaly.get('store_id')}\n"
            f"Item: {anomaly.get('item_id')}\n"
            f"Severity: {anomaly.get('severity')}\n"
            f"Details: {anomaly.get('details')}\n\n"
            f"Root Cause Analysis:\n{state.get('root_cause', 'N/A')}\n\n"
            f"Recommendations:\n{recommendation_text}\n"
        )
        return {"explanation": explanation}

    def diagnose(self, anomaly_id: str) -> AgentState:
        return self.graph.invoke({"anomaly_id": anomaly_id})
