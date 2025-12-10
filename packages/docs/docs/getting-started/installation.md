# Installation

## Prerequisites

- [uv](https://docs.astral.sh/uv/) - Fast Python package manager
- Java 17+ (for PySpark)
    - You may need to set `JAVA_HOME` environment variable (e.g. `export JAVA_HOME="/Users/$(whoami)/Library/Java/JavaVirtualMachines/corretto-17.0.9/Contents/Home"`)
- Node.js 22+ (for CDK)
- AWS CLI configured with appropriate credentials (for deployment)

### Installing uv

```bash
# macOS / Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Or with Homebrew
brew install uv

# Windows
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
```

## Quick Setup

The fastest way to get started:

```bash
# Clone the repository
git clone https://github.com/caverac/insurance-fraud-detection.git
cd insurance-fraud-detection

# Install everything (Python + Node.js + pre-commit hooks)
make install
```

This single command:

1. Creates a virtual environment and installs all Python dependencies via `uv sync`
2. Installs pre-commit hooks for code quality
3. Installs Node.js dependencies for CDK in `packages/infra`

## Manual Installation

If you prefer to install components individually:

### 1. Python Dependencies

```bash
# uv will create .venv automatically and install all dependencies
uv sync
```

### 2. Pre-commit Hooks

```bash
uv run pre-commit install
```

### 3. Node.js Dependencies (for CDK)

```bash
cd packages/infra
yarn install
```

## Verify Installation

```bash
# Check fraud detection CLI
uv run fraud-detect --help

# Check Spark
uv run python -c "from pyspark.sql import SparkSession; print('PySpark OK')"

# Check CDK
cd packages/infra && yarn cdk --version

# Verify pre-commit hooks
uv run pre-commit run --all-files

# Run tests
uv run pytest
```

## Common uv Commands

```bash
# Sync dependencies (install/update)
uv sync

# Run a command in the virtual environment
uv run <command>

# Add a new dependency
uv add <package>

# Add a dev dependency
uv add --dev <package>

# Update dependencies
uv lock --upgrade
uv sync
```

## CDK Commands

CDK commands are run from the `packages/infra` directory:

```bash
cd packages/infra

# Synthesize CloudFormation templates
yarn synth

# Deploy all stacks
yarn deploy

# Show differences
yarn diff

# Bootstrap (first-time setup)
yarn bootstrap
```

Or use make targets from the project root:

```bash
make cdk-synth
make cdk-deploy
make cdk-diff
make cdk-bootstrap
```

## IDE Configuration

### VS Code

Recommended extensions:

- Python
- Pylance
- Black Formatter
- Pylint
- AWS Toolkit

Settings (`.vscode/settings.json`):

```json
{
  "python.defaultInterpreterPath": ".venv/bin/python",
  "python.analysis.typeCheckingMode": "basic",
  "[python]": {
    "editor.defaultFormatter": "ms-python.black-formatter",
    "editor.formatOnSave": true
  },
  "editor.codeActionsOnSave": {
    "source.organizeImports": "explicit"
  }
}
```

### PyCharm

1. Set Python interpreter to `.venv/bin/python`
2. Mark `packages/*/src` as Sources Root
3. Enable Black as external formatter
4. Enable Pylint inspections

## Troubleshooting

### PySpark Issues

If you encounter Java-related errors:

```bash
# On macOS with Homebrew
brew install openjdk@17
export JAVA_HOME=/opt/homebrew/opt/openjdk@17

# On Ubuntu
sudo apt install openjdk-17-jdk
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

### CDK Bootstrap

First-time CDK deployment requires bootstrapping:

```bash
cd packages/infra
yarn bootstrap
# Or: yarn cdk bootstrap aws://ACCOUNT_ID/REGION
```

### Permission Issues

Ensure your AWS credentials have sufficient permissions for:

- S3 bucket operations
- EMR cluster management
- Glue catalog access
- IAM role creation

### Pre-commit Hook Failures

If pre-commit hooks fail, you can run them manually to see details:

```bash
# Run all hooks
uv run pre-commit run --all-files

# Run specific hook
uv run pre-commit run black --all-files
uv run pre-commit run pylint --all-files
```

### uv Issues

```bash
# Clear uv cache if you have dependency issues
uv cache clean

# Force reinstall all dependencies
rm -rf .venv uv.lock
uv sync
```
