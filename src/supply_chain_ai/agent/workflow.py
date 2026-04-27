from __future__ import annotations

from typing import TypedDict

from langgraph.graph import END, START, StateGraph

from supply_chain_ai.agent.prompt_evolution import evolve_prompt_for_diagnosis
from supply_chain_ai.agent.tools import AgentDataTools


class AgentState(TypedDict, total=False):
    anomaly_id: str
    anomaly: dict
    context: dict
    root_cause: str
    recommendations: list[str]
    prompt_evolution: dict
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
        graph.add_node("evolve_prompt", self._evolve_prompt)
        graph.add_node("compose_explanation", self._compose_explanation)

        graph.add_edge(START, "fetch_anomaly")
        graph.add_edge("fetch_anomaly", "trace_context")
        graph.add_edge("trace_context", "infer_root_cause")
        graph.add_edge("infer_root_cause", "fetch_recommendations")
        graph.add_edge("fetch_recommendations", "evolve_prompt")
        graph.add_edge("evolve_prompt", "compose_explanation")
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

    def _evolve_prompt(self, state: AgentState) -> AgentState:
        evolution = evolve_prompt_for_diagnosis(
            anomaly_id=state["anomaly_id"],
            anomaly=state["anomaly"],
            root_cause=state.get("root_cause", "N/A"),
            recommendations=state.get("recommendations", []),
            context=state.get("context", {}),
        )
        return {"prompt_evolution": evolution}

    def _compose_explanation(self, state: AgentState) -> AgentState:
        evolution = state.get("prompt_evolution", {})
        best_response = evolution.get("best_response", "No diagnosis produced.")
        best_candidate = evolution.get("best_candidate", "N/A")
        best_score = evolution.get("best_score", "N/A")
        rounds = evolution.get("rounds", [])
        rounds_text = "\n".join(
            [
                f"- Round {r.get('round')}: {r.get('candidate_name')} | score={r.get('score')}"
                for r in rounds
            ]
        )

        explanation = (
            f"{best_response}\n"
            f"\nPrompt Evolution:\n"
            f"Selected candidate: {best_candidate}\n"
            f"Best score: {best_score}\n"
            f"Rounds:\n{rounds_text}\n"
        )
        return {"explanation": explanation}

    def diagnose(self, anomaly_id: str) -> AgentState:
        return self.graph.invoke({"anomaly_id": anomaly_id})
