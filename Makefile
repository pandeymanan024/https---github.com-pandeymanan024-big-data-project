.PHONY: install demo run diagnose test

install:
	pip install -e ".[dev]"

demo:
	supply-chain-ai demo --base-dir ./workspace_demo

run:
	supply-chain-ai run-pipeline ./m5 ./artifacts

diagnose:
	supply-chain-ai diagnose ./artifacts

test:
	pytest -q
