from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PromptCandidate:
    name: str
    system_prompt: str


def _build_candidate_response(
    candidate: PromptCandidate,
    anomaly_id: str,
    anomaly: dict,
    root_cause: str,
    recommendations: list[str],
    context: dict,
) -> str:
    metric_value = anomaly.get("metric_value")
    expected_value = anomaly.get("expected_value")
    context_text = (
        f"Closing stock={context.get('closing_stock', 'NA')}, "
        f"forecast={round(float(context.get('forecast_units', 0.0)), 2)}"
        if context
        else "No inventory context available"
    )
    rec_text = "\n".join([f"- {r}" for r in recommendations]) if recommendations else "- No recommendation generated"

    if candidate.name == "baseline_analyst":
        return (
            f"Anomaly ID: {anomaly_id}\n"
            f"Rule: {anomaly.get('rule_name')}\n"
            f"Date: {anomaly.get('date')}\n"
            f"Store: {anomaly.get('store_id')}\n"
            f"Item: {anomaly.get('item_id')}\n"
            f"Severity: {anomaly.get('severity')}\n"
            f"Observed vs expected: {metric_value} vs {expected_value}\n"
            f"Details: {anomaly.get('details')}\n"
            f"Context: {context_text}\n\n"
            f"Root Cause Analysis:\n{root_cause}\n\n"
            f"Recommendations:\n{rec_text}\n"
        )

    if candidate.name == "structured_rca":
        return (
            f"Incident Summary\n"
            f"- anomaly_id: {anomaly_id}\n"
            f"- rule: {anomaly.get('rule_name')}\n"
            f"- severity: {anomaly.get('severity')}\n"
            f"- location: store={anomaly.get('store_id')}, item={anomaly.get('item_id')}\n"
            f"- time: {anomaly.get('date')}\n"
            f"- metric_gap: observed={metric_value}, expected={expected_value}\n\n"
            f"Root Cause\n{root_cause}\n\n"
            f"Operational Context\n- {context_text}\n\n"
            f"Action Plan\n{rec_text}\n"
        )

    return (
        f"Decision Intelligence Brief\n"
        f"Signal: {anomaly.get('rule_name')} ({anomaly.get('severity')}) on {anomaly.get('date')} for "
        f"{anomaly.get('item_id')} at {anomaly.get('store_id')}.\n"
        f"Deviation: observed={metric_value}, expected={expected_value}.\n"
        f"Why it happened: {root_cause}\n"
        f"Current operating state: {context_text}.\n"
        f"What to do now:\n{rec_text}\n"
    )


def _score_response(response: str, anomaly: dict, context: dict, has_recommendations: bool) -> tuple[int, list[str]]:
    score = 0
    notes: list[str] = []

    checks = [
        (str(anomaly.get("rule_name", "")), "captures anomaly rule"),
        (str(anomaly.get("store_id", "")), "captures store context"),
        (str(anomaly.get("item_id", "")), "captures item context"),
        ("Root Cause", "includes root-cause section"),
    ]

    for token, note in checks:
        if token and token in response:
            score += 2
            notes.append(note)

    if "observed=" in response or "Observed vs expected" in response:
        score += 2
        notes.append("includes observed-vs-expected metrics")

    if context and ("Closing stock" in response or "closing_stock" in response or "forecast" in response):
        score += 1
        notes.append("includes inventory context")

    if has_recommendations and ("Recommendations" in response or "Action Plan" in response or "What to do now" in response):
        score += 2
        notes.append("includes actionable recommendations")

    return score, notes


def evolve_prompt_for_diagnosis(
    anomaly_id: str,
    anomaly: dict,
    root_cause: str,
    recommendations: list[str],
    context: dict,
) -> dict:
    candidates = [
        PromptCandidate(
            name="baseline_analyst",
            system_prompt="You are a supply-chain incident analyst. Explain anomaly, root cause, and recommendations with operational context.",
        ),
        PromptCandidate(
            name="structured_rca",
            system_prompt="You are an RCA specialist. Produce incident summary, root cause, operational context, and action plan in structured form.",
        ),
        PromptCandidate(
            name="decision_brief",
            system_prompt="You are a decision-intelligence assistant. Create an executive brief with deviation, why, and immediate actions.",
        ),
    ]

    trials: list[dict] = []
    best_trial: dict | None = None

    for round_no, candidate in enumerate(candidates, start=1):
        response = _build_candidate_response(
            candidate=candidate,
            anomaly_id=anomaly_id,
            anomaly=anomaly,
            root_cause=root_cause,
            recommendations=recommendations,
            context=context,
        )
        score, notes = _score_response(response, anomaly, context, bool(recommendations))

        trial = {
            "round": round_no,
            "candidate_name": candidate.name,
            "system_prompt": candidate.system_prompt,
            "score": score,
            "score_notes": notes,
            "response": response,
        }
        trials.append(trial)

        if best_trial is None or trial["score"] > best_trial["score"]:
            best_trial = trial

    if best_trial is None:
        raise ValueError("Prompt evolution failed to produce a candidate.")

    return {
        "rounds": trials,
        "best_candidate": best_trial["candidate_name"],
        "best_score": best_trial["score"],
        "best_response": best_trial["response"],
    }
