# Testing Guide

## Overview

The fraud detection project uses pytest for testing, with special support for PySpark testing.

## Test Structure

```
packages/
├── fraud_detection/
│   └── tests/
│       ├── __init__.py
│       ├── conftest.py          # Shared fixtures
│       ├── test_billing_patterns.py
│       ├── test_duplicates.py
│       ├── test_outliers.py
│       └── test_benfords.py
└── infra/
    └── tests/
        ├── __init__.py
        └── test_stacks.py
```

## Running Tests

### All Tests

```bash
pytest
```

### With Coverage

```bash
pytest --cov=packages --cov-report=html
open htmlcov/index.html
```

### Verbose Output

```bash
pytest -v
```

### Specific Package

```bash
pytest packages/fraud_detection/tests/
```

### Specific Test Class

```bash
pytest packages/fraud_detection/tests/test_outliers.py::TestOutlierDetector
```

### Specific Test Method

```bash
pytest packages/fraud_detection/tests/test_outliers.py::TestOutlierDetector::test_zscore_outliers_detected
```

### Filter by Marker

```bash
pytest -m "not slow"
pytest -m integration
```

## PySpark Testing

### Session Fixture

A shared Spark session is created for all tests:

```python
# conftest.py
@pytest.fixture(scope="session")
def spark() -> SparkSession:
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

Create reusable test data:

```python
@pytest.fixture
def sample_claims(spark, claims_schema):
    data = [
        ("CLM001", "PAT001", "PRV001", "99213", date(2024, 1, 15), Decimal("100.00"), "CA", "CA"),
        ("CLM002", "PAT002", "PRV001", "99214", date(2024, 1, 16), Decimal("150.00"), "CA", "CA"),
    ]
    return spark.createDataFrame(data, claims_schema)
```

### DataFrame Assertions

Use chispa for DataFrame comparisons:

```python
from chispa import assert_df_equality

def test_transform_preserves_rows(spark, sample_claims):
    result = my_transform(sample_claims)
    assert result.count() == sample_claims.count()

def test_output_schema(spark, sample_claims):
    result = my_transform(sample_claims)
    expected_schema = StructType([...])
    assert result.schema == expected_schema

def test_exact_output(spark, expected_df, actual_df):
    assert_df_equality(expected_df, actual_df)
```

### Testing Patterns

#### Testing Column Addition

```python
def test_adds_flag_column(detector, sample_claims):
    result = detector.detect_outliers(sample_claims, "charge_amount", "is_outlier")
    assert "is_outlier" in result.columns
```

#### Testing Flag Logic

```python
def test_flags_high_values(detector, spark, claims_schema):
    data = [
        ("CLM001", ..., Decimal("100.00")),   # Normal
        ("CLM002", ..., Decimal("10000.00")), # Outlier
    ]
    claims = spark.createDataFrame(data, claims_schema)

    result = detector.detect_outliers(claims, "charge_amount", "is_outlier")

    flags = {row["claim_id"]: row["is_outlier"] for row in result.collect()}
    assert flags["CLM001"] == False
    assert flags["CLM002"] == True
```

#### Testing Aggregations

```python
def test_counts_duplicates_correctly(detector, spark):
    claims = create_claims_with_duplicates(spark, num_duplicates=5)

    result = detector.detect(claims)

    duplicate_count = result.filter(result.is_duplicate == True).count()
    assert duplicate_count == 5
```

## CDK Testing

### Snapshot Testing

```python
from aws_cdk.assertions import Template

def test_data_lake_stack(app, env):
    stack = DataLakeStack(app, "Test", project_name="test", env=env)
    template = Template.from_stack(stack)

    # Assert resource exists
    template.has_resource_properties("AWS::S3::Bucket", {
        "BucketEncryption": {...}
    })

    # Assert resource count
    template.resource_count_is("AWS::Glue::Crawler", 2)
```

### Integration Testing

```python
@pytest.mark.integration
def test_full_deployment():
    """Test actual deployment (requires AWS credentials)."""
    # Deploy
    result = subprocess.run(["cdk", "deploy", "--require-approval", "never"])
    assert result.returncode == 0

    # Verify resources
    s3 = boto3.client("s3")
    buckets = s3.list_buckets()
    assert any("fraud-detection" in b["Name"] for b in buckets["Buckets"])
```

## Mocking

### Mocking External Services

```python
from unittest.mock import Mock, patch

@patch("fraud_detection.utils.external_service.call_api")
def test_with_mocked_api(mock_api, detector, sample_claims):
    mock_api.return_value = {"status": "ok"}
    result = detector.detect(sample_claims)
    assert mock_api.called
```

### Mocking Spark Operations

```python
def test_with_mock_spark(mocker):
    mock_spark = mocker.Mock(spec=SparkSession)
    mock_df = mocker.Mock(spec=DataFrame)
    mock_spark.read.parquet.return_value = mock_df

    # Test code that uses Spark
```

## Performance Testing

### Benchmarking

```python
@pytest.mark.slow
def test_performance_large_dataset(spark, benchmark):
    claims = generate_large_dataset(spark, num_rows=1_000_000)
    detector = FraudDetector(spark)

    result = benchmark(detector.detect, claims)

    assert result.count() == 1_000_000
```

### Memory Profiling

```python
@pytest.mark.memory
def test_memory_usage(spark):
    import tracemalloc

    tracemalloc.start()

    # Run detection
    claims = generate_claims(spark, num_rows=100_000)
    detector = FraudDetector(spark)
    result = detector.detect(claims)
    result.count()  # Force evaluation

    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    assert peak < 1_000_000_000  # Less than 1GB
```

## Test Configuration

### pytest.ini / pyproject.toml

```toml
[tool.pytest.ini_options]
testpaths = ["packages/*/tests"]
pythonpath = ["packages/fraud_detection/src", "packages/infra/src"]
addopts = "-v --cov=packages --cov-report=term-missing"
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

# Module-scoped test data (created once per module)
@pytest.fixture(scope="module")
def reference_data(spark):
    ...

# Function-scoped instances (created for each test)
@pytest.fixture
def detector(spark):
    return FraudDetector(spark)
```

## CI/CD Integration

### GitHub Actions

```yaml
# .github/workflows/test.yml
name: Tests
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - uses: actions/setup-java@v4
        with:
          distribution: 'temurin'
          java-version: '11'
      - run: pip install -e "packages/fraud_detection[dev]"
      - run: pytest --cov --cov-report=xml
      - uses: codecov/codecov-action@v3
```
