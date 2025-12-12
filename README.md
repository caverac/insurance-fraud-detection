# Insurance Claims Fraud Detection

A PySpark-based fraud detection system for insurance claims, deployed on AWS using CDK.

## Overview

This project demonstrates:
- **Batch processing** of insurance claims using PySpark
- **Rule-based anomaly detection** for flagging unusual billing patterns
- **Statistical outlier detection** for identifying suspicious charges
- **Duplicate claim detection** using similarity matching

## Project Structure

```
insurance-fraud/
├── packages/
│   ├── fraud_detection/     # PySpark fraud detection application
│   │   ├── src/
│   │   │   └── fraud_detection/
│   │   │       ├── jobs/           # Spark jobs
│   │   │       ├── rules/          # Rule-based detection
│   │   │       ├── statistics/     # Statistical anomaly detection
│   │   │       └── utils/          # Shared utilities
│   │   └── tests/
│   ├── infra/               # AWS CDK infrastructure
│   │   ├── src/
│   │   │   └── infra/
│   │   │       └── stacks/
│   │   └── tests/
│   └── docs/                # Documentation site
│       └── docs/
├── pyproject.toml           # Root project configuration (uv workspace)
├── package.json             # Node.js dependencies (CDK)
└── README.md
```

## Data Sources

This project is designed to work with CMS Medicare public datasets:
- [Medicare Provider Utilization and Payment Data](https://data.cms.gov/)
- Synthetic claims data for development/testing

## Getting Started

### Prerequisites

- [uv](https://docs.astral.sh/uv/) (Python package manager)
- Java 11+ (for PySpark)
- Node.js 22+ (for CDK)
- Yarn 4+ (for package management)
- AWS CLI configured with appropriate credentials

### Installation

```bash
# Install all dependencies (Python + Node.js + pre-commit hooks)
make install
```

Or manually:

```bash
# Install Python dependencies and create virtual environment
uv sync

# Install pre-commit hooks
uv run pre-commit install

# Install Node.js dependencies (CDK)
yarn install
```

### Local Development

```bash
# Run tests
make test
# Or: uv run pytest

# Run linting (pylint + flake8 + pydocstyle)
make lint

# Format code (black + isort)
make format

# Type checking
make type-check

# Generate sample data
make sample-data

# Run fraud detection locally
make run-local

# Analyze results
make analyze
```

### Deploy to AWS

```bash
# Bootstrap CDK (first time only)
make cdk-bootstrap

# Deploy all stacks
make cdk-deploy

# View changes before deploying
make cdk-diff
```

## Fraud Detection Features

### Rule-Based Detection
- Provider billing pattern analysis
- Procedure code validation
- Geographic anomaly detection
- Temporal pattern analysis

### Statistical Detection
- Charge amount outliers (IQR/Z-score methods)
- Frequency anomalies
- Benford's Law analysis for charge amounts

### Duplicate Detection
- Exact match detection
- Fuzzy matching for near-duplicates
- Claim clustering

## Architecture

```mermaid
flowchart TB
    subgraph Input
        A[S3 Raw Data<br/>Claims]
    end

    subgraph Processing
        B[EMR Spark<br/>Processing]
    end

    subgraph Output
        C[S3 Results<br/>Flagged]
    end

    subgraph Catalog
        D[Glue Catalog<br/>Metadata]
    end

    subgraph Analytics
        E[Athena<br/>Queries]
    end

    A --> B
    B --> C
    C --> D
    D --> E
```

## Detection Pipeline

```mermaid
flowchart TD
    A[Raw Claims] --> B[Apply Rules]
    B --> C[Statistical Analysis]
    C --> D[Duplicate Detection]
    D --> E[Score Calculation]
    E --> F[Flagged Claims]

    B --> G[Rule Violations]
    C --> H[Statistical Flags]
    D --> I[Duplicate Flags]

    G --> E
    H --> E
    I --> E
```

## Documentation

Build and serve the documentation site:

```bash
# Serve locally
make docs-serve

# Build static site
make docs
```

## License

MIT
