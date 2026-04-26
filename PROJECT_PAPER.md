# Agentic AI System for Supply Chain Resilience and Decision Intelligence

**Authors:** Sanjeet Mishra, Manan Pandey

## 1. Problem Statement

Modern retail supply chains are tightly coupled systems where demand sensing, pricing, inventory, and fulfillment interact continuously. Existing monitoring systems detect KPI deviations but do not provide rapid diagnosis and corrective action. Consequently, teams spend significant time manually investigating anomalies across disconnected data tables.

This project addresses that gap by building an agentic AI system that:

- Detects anomalies in supply-chain signals,
- Performs root-cause analysis across dependent layers,
- Produces actionable recommendations for operational response.

## 2. Motivation

Supply-chain failures translate directly into missed revenue, higher costs, and lower customer satisfaction. Reactive analysis workflows are slow and fragile under high-volume retail data.

The project aims to:

- Reduce manual debugging and triage time,
- Improve observability and resilience,
- Deliver explainable diagnostics and decision support.

## 3. Approach

### 3.1 Data Transformation and Supply Chain Modeling

Raw M5 sales, calendar, and pricing data are transformed into a unified daily time-series model by store and item. Since real operational inventory states are unavailable, inventory and fulfillment behavior are simulated.

### 3.2 Distributed Data Processing Pipeline

PySpark is used for scalable transformations:

- Wide-to-long conversion of M5 `d_*` demand columns,
- Time alignment via calendar join,
- Price enrichment via store-item-week joins,
- Structured parquet outputs for downstream layers.

### 3.3 Supply Chain Simulation

A simulation engine reproduces:

- Inventory depletion from demand,
- Reorder policy behavior (reorder point, quantity, lead time),
- Stockout and fulfillment effects.

### 3.4 Validation Layer

Rule-based checks detect failures such as:

- Demand spikes and drops vs rolling baseline,
- Low inventory cover vs forecast,
- Fulfillment SLA violations,
- Missing upstream price data.

### 3.5 Agentic AI Layer (LangGraph)

A LangGraph workflow orchestrates multi-step diagnosis:

1. Fetch anomaly,
2. Trace latest inventory context,
3. Infer root cause,
4. Fetch associated recommendations,
5. Compose explainable output.

### 3.6 Recommendation Layer

Decision support actions include:

- Restocking suggestions,
- Store-level allocation advice,
- Demand surge response triggers,
- Substitution guidance and data quality fixes.

## 4. Dataset

**M5 Forecasting Accuracy Dataset**

- Product-store daily sales,
- Calendar and event metadata,
- Sell-price history.

## 5. Technology Stack

- **Data Processing:** Apache Spark (PySpark)
- **Agent Orchestration:** LangGraph
- **Language:** Python 3.10+
- **Storage:** Local parquet artifacts (Delta-ready extension path)

## 6. Expected Outcomes

- Automated anomaly detection over multi-layer supply-chain signals,
- Explainable root-cause analysis,
- Actionable recommendations for operations,
- Reproducible and scalable observability framework.

## 7. Conclusion

The implementation demonstrates an end-to-end pattern for upgrading traditional analytics into agentic decision intelligence. By coupling distributed processing with structured reasoning and prescriptive actions, the system strengthens supply-chain resilience and response quality.
