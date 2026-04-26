# Agentic AI System for Supply Chain Resilience and Decision Intelligence

Authors: **Sanjeet Mishra**, **Manan Pandey**

This project implements an end-to-end supply-chain intelligence platform that:

- Processes retail demand data at scale using **PySpark**
- Simulates inventory and fulfillment behavior
- Detects anomalies through rule-based validation
- Performs root-cause reasoning with a **LangGraph-based agent**
- Generates actionable recommendations for operations teams

---

## 1) Architecture

1. **Data Transformation Layer**  
   Converts raw M5 data into product-store-day time series.

2. **Forecast Layer**  
   Creates baseline short-term forecasts via rolling averages.

3. **Simulation Layer**  
   Simulates inventory depletion, delayed replenishment, stockouts, and fulfillment.

4. **Validation Layer**  
   Detects anomalies such as demand spikes/drops, low inventory cover, fulfillment drops, and missing prices.

5. **Agentic AI Layer (LangGraph)**  
   Traces anomaly context and returns explainable root-cause summaries.

6. **Recommendation Layer**  
   Produces restocking, allocation, and substitution actions.

---

## 2) Project Structure

- [src/supply_chain_ai/main.py](src/supply_chain_ai/main.py) - CLI entrypoint
- [src/supply_chain_ai/config.py](src/supply_chain_ai/config.py) - configuration and thresholds
- [src/supply_chain_ai/pipeline/data_transform.py](src/supply_chain_ai/pipeline/data_transform.py) - M5 ingestion + feature transforms
- [src/supply_chain_ai/pipeline/forecast.py](src/supply_chain_ai/pipeline/forecast.py) - forecasting logic
- [src/supply_chain_ai/pipeline/simulation.py](src/supply_chain_ai/pipeline/simulation.py) - inventory and fulfillment simulation
- [src/supply_chain_ai/validation/rules.py](src/supply_chain_ai/validation/rules.py) - anomaly checks
- [src/supply_chain_ai/recommendation/engine.py](src/supply_chain_ai/recommendation/engine.py) - recommendations
- [src/supply_chain_ai/agent/workflow.py](src/supply_chain_ai/agent/workflow.py) - LangGraph agent
- [src/supply_chain_ai/utils/synthetic_data.py](src/supply_chain_ai/utils/synthetic_data.py) - demo data generator

---

## 3) Setup

```bash
cd /Users/mananpandey/Documents/assignments/big-data/project
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

---

## 4) Usage

### A) Full demo run (generates synthetic M5-like data first)

```bash
supply-chain-ai demo --base-dir ./workspace_demo
```

### B) Run pipeline on real M5 files

Expected files in input directory:

- `sales_train_validation.csv`
- `calendar.csv`
- `sell_prices.csv`

```bash
supply-chain-ai run-pipeline ./m5 ./artifacts
```

### C) Run agent diagnosis for top anomaly

```bash
supply-chain-ai diagnose ./artifacts
```

### D) Run agent diagnosis for a specific anomaly

```bash
supply-chain-ai diagnose ./artifacts <id>
```

---

## 5) Output Artifacts

The pipeline writes:

- `processed_demand.parquet`
- `forecast.parquet`
- `inventory_fulfillment.parquet`
- `validation_logs.parquet`
- `recommendations.parquet`

and a human-readable diagnosis report:

- `diagnosis_report.txt`

---

## 6) Validation Rules Included

- Demand spike (`units_sold` above rolling profile)
- Demand drop (`units_sold` below rolling profile)
- Low inventory cover (stock not covering near-term forecast)
- Fulfillment drop (`fulfillment_rate` below threshold)
- Missing price entries

---

## 7) Notes

- Spark runs locally by default but logic is distributed-ready.
- LangGraph is used for multi-step stateful reasoning without requiring a paid LLM API.
- Rules and thresholds are configurable via [src/supply_chain_ai/config.py](src/supply_chain_ai/config.py).
