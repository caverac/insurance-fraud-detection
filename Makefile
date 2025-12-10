.PHONY: help install test lint format type-check docs docs-serve clean sample-data run-local analyze

help:
	@echo "Available commands:"
	@echo "  install       Install all dependencies (uv sync)"
	@echo "  test          Run all tests"
	@echo "  lint          Run linting (pylint + flake8 + pydocstyle)"
	@echo "  format        Format code (black + isort)"
	@echo "  type-check    Run type checking (mypy)"
	@echo "  docs          Build documentation"
	@echo "  docs-serve    Serve documentation locally"
	@echo "  clean         Clean build artifacts"
	@echo "  sample-data   Generate sample data"
	@echo "  run-local     Run fraud detection locally on sample data"

# Development setup
install:
	uv sync
	uv run pre-commit install
	cd packages/infra && yarn install

# Testing (requires Java 17 for PySpark compatibility)
test:
	uv run pytest

# Code quality
lint:
	uv run flake8 packages/
	uv run pylint packages/
	uv run pydocstyle packages/

format:
	uv run black packages/
	uv run isort packages/

type-check:
	uv run mypy packages/

# Documentation
docs:
	uv run mkdocs build -f packages/docs/mkdocs.yml

docs-serve:
	cd packages/docs && uv run mkdocs serve --livereload --watch ../fraud_detection/src

# Cleanup
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "htmlcov" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "cdk.out" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "site" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "node_modules" -exec rm -rf {} + 2>/dev/null || true
	rm -rf .coverage

# Fraud detection commands (requires Java 17 for PySpark)
sample-data:
	uv run fraud-detect generate-sample --output /tmp/sample_data --num-claims 10000 --fraud-rate 0.05

run-local:
	uv run fraud-detect run --input /tmp/sample_data --output /tmp/results --format csv --local

analyze:
	uv run fraud-detect analyze --results /tmp/results --report summary

# CDK commands
cdk-synth:
	cd packages/infra && yarn synth

cdk-deploy:
	cd packages/infra && yarn deploy

cdk-destroy:
	cd packages/infra && yarn destroy

cdk-diff:
	cd packages/infra && yarn diff

cdk-bootstrap:
	cd packages/infra && yarn bootstrap
