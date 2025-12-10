# Testing Guide

## Overview

The fraud detection project uses pytest for testing, with special support for PySpark testing. Tests are required to maintain 100% code coverage.

## Test Structure

```
packages/
├── fraud_detection/
│   └── tests/
│       ├── conftest.py              # Shared fixtures (Spark session, schemas)
│       └── test_<module_name>.py    # Tests for each module
└── infra/
    └── tests/
        └── test_<stack_name>.py     # CDK stack tests
```

## Running Tests

All test commands use `uv run` to execute within the project's virtual environment.

### All Tests

```bash
# Using make
make test

# Using uv directly
uv run pytest
```

### With Coverage

```bash
# Terminal report (default)
uv run pytest --cov=packages --cov-report=term-missing

# HTML report
uv run pytest --cov=packages --cov-report=html
open htmlcov/index.html
```

### Specific Package

```bash
uv run pytest packages/fraud_detection/tests/
uv run pytest packages/infra/tests/
```

### Specific Test Class or Method

```bash
# Specific class
uv run pytest packages/fraud_detection/tests/test_outliers.py::TestOutlierDetector

# Specific method
uv run pytest packages/fraud_detection/tests/test_outliers.py::TestOutlierDetector::test_zscore_outliers_detected
```

### Filter by Marker

```bash
uv run pytest -m "not slow"
uv run pytest -m integration
```

## Coverage Requirements

The project enforces 100% code coverage. Tests will fail if coverage drops below this threshold.

Configuration in `pyproject.toml`:

```toml
[tool.pytest.ini_options]
addopts = "-v --cov=packages --cov-report=term-missing --cov-fail-under=100"

[tool.coverage.run]
omit = [
    "*/tests/*",
    "*/cli.py",
    "*/jobs/*",
    "*/utils/*",
    # ... other exclusions
]

[tool.coverage.report]
exclude_lines = [
    "pragma: no cover",
    "if TYPE_CHECKING:",
    "if __name__ == .__main__.:",
    "raise ValueError",
]
```

## PySpark Testing

### Session Fixture

A shared Spark session is created once per test session:

```python
# conftest.py
@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("FraudDetectionTests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
```

### Test Data Fixtures

Create reusable test data using fixtures:

```python
@pytest.fixture
def sample_claims(request: pytest.FixtureRequest) -> DataFrame:
    """Create sample claims data for testing."""
    spark_session: SparkSession = request.getfixturevalue("spark")
    schema: StructType = request.getfixturevalue("claims_schema")
    data = [
        ("CLM001", "PAT001", "PRV001", "99213", date(2024, 1, 15), Decimal("100.00"), "CA", "CA"),
        ("CLM002", "PAT002", "PRV001", "99214", date(2024, 1, 16), Decimal("150.00"), "CA", "CA"),
    ]
    return spark_session.createDataFrame(data, schema)
```

### DataFrame Assertions

```python
def test_transform_preserves_rows(spark, sample_claims):
    result = my_transform(sample_claims)
    assert result.count() == sample_claims.count()

def test_output_schema(spark, sample_claims):
    result = my_transform(sample_claims)
    assert "new_column" in result.columns

def test_flag_logic(detector, spark, claims_schema):
    # Test that specific values are flagged correctly
    result = detector.detect_outliers(claims, "charge_amount", "is_outlier")
    outliers = result.filter(result.is_outlier == True).collect()
    assert len(outliers) == 1
    assert outliers[0]["claim_id"] == "CLM_OUTLIER"
```

## CDK Testing

### Template Assertions

```python
from aws_cdk.assertions import Template

def test_data_lake_stack(app, env):
    stack = DataLakeStack(app, "Test", project_name="test", env=env)
    template = Template.from_stack(stack)

    # Assert resource properties
    template.has_resource_properties("AWS::S3::Bucket", {
        "BucketEncryption": {...}
    })

    # Assert resource count
    template.resource_count_is("AWS::Glue::Crawler", 2)
```

## Test Configuration

### pyproject.toml

```toml
[tool.pytest.ini_options]
testpaths = ["packages/fraud_detection/tests", "packages/infra/tests"]
pythonpath = ["."]
addopts = "-v --cov=packages --cov-report=term-missing --cov-fail-under=100"
markers = [
    "slow: marks tests as slow",
    "integration: marks tests as integration tests",
]
```

### conftest.py Best Practices

```python
import pytest

# Session-scoped Spark (created once per test session)
@pytest.fixture(scope="session")
def spark():
    ...

# Function-scoped instances (created for each test)
@pytest.fixture
def detector(spark):
    return FraudDetector(spark)
```

## CI/CD Integration

Tests run automatically on every PR and push to main via GitHub Actions. The CI pipeline includes:

1. **Format Check** - Black and isort
2. **Lint** - Flake8, Pylint, Pydocstyle
3. **Type Check** - Mypy
4. **Tests** - Pytest with coverage enforcement
5. **CDK Synth** - Validates infrastructure code
6. **Docs Build** - Validates documentation builds
