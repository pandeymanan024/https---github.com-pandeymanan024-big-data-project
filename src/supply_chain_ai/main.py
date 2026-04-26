from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from supply_chain_ai.agent.tools import AgentDataTools
from supply_chain_ai.agent.workflow import SupplyChainDiagnosisAgent
from supply_chain_ai.pipeline.orchestrator import SupplyChainPipeline
from supply_chain_ai.utils.synthetic_data import generate_synthetic_m5_like_dataset

app = typer.Typer(help="Supply Chain Agentic AI CLI")
console = Console()


@app.command("run-pipeline")
def run_pipeline(raw_data_dir: Path, output_dir: Path) -> None:
    """Run the full Spark-based supply chain pipeline."""
    pipeline = SupplyChainPipeline()
    paths = pipeline.run(raw_data_dir=raw_data_dir, output_dir=output_dir)

    table = Table(title="Pipeline Outputs")
    table.add_column("Artifact")
    table.add_column("Path")
    for name, path in paths.items():
        table.add_row(name, path)
    console.print(table)


@app.command("diagnose")
def diagnose(artifacts_dir: Path, anomaly_id: str | None = None) -> None:
    """Run LangGraph-based diagnosis for one anomaly."""
    tools = AgentDataTools(artifacts_dir)
    chosen_id = anomaly_id or tools.get_top_anomaly_id()

    agent = SupplyChainDiagnosisAgent(tools)
    result = agent.diagnose(chosen_id)

    report = result.get("explanation", "No diagnosis produced.")
    console.print(report)

    report_path = artifacts_dir / "diagnosis_report.txt"
    report_path.write_text(report, encoding="utf-8")
    console.print(f"\nSaved report: {report_path}")


@app.command("demo")
def demo(base_dir: Path = Path("./workspace_demo")) -> None:
    """Generate synthetic dataset and run pipeline + diagnosis."""
    raw_dir = base_dir / "m5_raw"
    out_dir = base_dir / "artifacts"

    console.print(f"Generating synthetic M5-like data at: {raw_dir}")
    generate_synthetic_m5_like_dataset(raw_dir)

    console.print("Running pipeline...")
    pipeline = SupplyChainPipeline()
    pipeline.run(raw_data_dir=raw_dir, output_dir=out_dir)

    console.print("Running diagnosis on top anomaly...")
    tools = AgentDataTools(out_dir)
    agent = SupplyChainDiagnosisAgent(tools)
    result = agent.diagnose(tools.get_top_anomaly_id())

    report = result.get("explanation", "No diagnosis produced.")
    report_path = out_dir / "diagnosis_report.txt"
    report_path.write_text(report, encoding="utf-8")

    console.print("Demo complete.")
    console.print(f"Artifacts: {out_dir}")
    console.print(f"Diagnosis report: {report_path}")


if __name__ == "__main__":
    app()
