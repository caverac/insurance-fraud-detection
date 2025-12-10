# Contributing

## Development Setup

1. Clone the repository
2. Install dependencies with uv

```bash
git clone https://github.com/caveraio/insurance-fraud-detection.git
cd insurance-fraud-detection

# Install all dependencies (Python + Node.js + pre-commit hooks)
make install
```

Or manually:

```bash
uv sync
uv run pre-commit install
cd packages/infra && yarn install
```

## Code Style

### Formatting

We use Black for formatting and isort for import sorting:

```bash
# Format code
make format
# Or manually:
uv run black packages/
uv run isort packages/
```

### Linting

We use Pylint, Flake8, and pydocstyle (NumPy convention):

```bash
# Run all linters
make lint
# Or manually:
uv run flake8 packages/
uv run pylint packages/
uv run pydocstyle packages/
```

### Type Hints

All code should include type hints:

```python
def detect_outliers(
    df: DataFrame,
    column: str,
    threshold: float = 3.0,
) -> DataFrame:
    ...
```

Run type checking:

```bash
make type-check
# Or: uv run mypy packages/
```

### Docstrings

Use NumPy-style docstrings:

```python
def calculate_fraud_score(
    claims: DataFrame,
    weights: dict[str, float],
) -> DataFrame:
    """
    Calculate composite fraud score for each claim.

    Parameters
    ----------
    claims : DataFrame
        DataFrame with fraud flags from detection methods.
    weights : dict[str, float]
        Dictionary mapping flag categories to weights.

    Returns
    -------
    DataFrame
        DataFrame with fraud_score column added.

    Raises
    ------
    ValueError
        If required columns are missing.

    Examples
    --------
    >>> scores = calculate_fraud_score(claims, {"rules": 0.3})
    >>> scores.select("fraud_score").show()
    """
```

## Testing

### Running Tests

```bash
# Run all tests
make test
# Or: uv run pytest

# Run specific package tests
uv run pytest packages/fraud_detection/tests/

# Run specific test
uv run pytest packages/fraud_detection/tests/test_outliers.py::TestOutlierDetector::test_zscore_outliers_detected
```

### Coverage Requirements

The project requires 100% code coverage. Tests will fail if coverage drops below this threshold.

### Writing Tests

Follow these conventions:

```python
import pytest
from pyspark.sql import SparkSession

class TestOutlierDetector:
    """Tests for OutlierDetector."""

    @pytest.fixture
    def detector(self, spark: SparkSession) -> OutlierDetector:
        """Create detector instance."""
        return OutlierDetector(spark, DetectionConfig())

    def test_detects_high_outliers(
        self,
        detector: OutlierDetector,
        sample_claims,
    ) -> None:
        """Test that high outliers are detected."""
        result = detector.detect_zscore_outliers(
            sample_claims, "charge_amount", "is_outlier"
        )
        outliers = result.filter(result.is_outlier).count()
        assert outliers > 0
```

## Pull Request Process

1. Create a feature branch
2. Make your changes
3. Add tests for new functionality
4. Run the test suite
5. Update documentation if needed
6. Submit a pull request

### Branch Naming

- `feature/description` - New features
- `fix/description` - Bug fixes
- `docs/description` - Documentation changes
- `refactor/description` - Code refactoring

### Commit Messages

Use conventional commits:

```
feat: add Benford's Law analysis
fix: correct Z-score calculation for grouped data
docs: update configuration guide
test: add tests for duplicate detection
refactor: simplify outlier detection interface
```

### PR Checklist

- [ ] Tests pass (`uv run pytest`)
- [ ] 100% code coverage maintained
- [ ] Code is formatted (`uv run black packages/ && uv run isort packages/`)
- [ ] Linting passes (`make lint`)
- [ ] Type hints added (`uv run mypy packages/`)
- [ ] Docstrings updated (NumPy style)
- [ ] Documentation updated if needed

## Adding New Detection Methods

### 1. Create the Module

```python
# packages/fraud_detection/src/fraud_detection/rules/<module_name>.py
from pyspark.sql import DataFrame

class MyCustomRules:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config

    def check_my_pattern(self, claims: DataFrame) -> DataFrame:
        """Check for my custom pattern."""
        # Implementation
        return claims
```

### 2. Add Tests

```python
# packages/fraud_detection/tests/test_<module_name>.py
class TestMyCustomRules:
    def test_detects_pattern(self, spark, sample_claims):
        rules = MyCustomRules(spark, DetectionConfig())
        result = rules.check_my_pattern(sample_claims)
        # Assertions
```

### 3. Integrate with Detector

```python
# In FraudDetector.__init__
self.my_rules = MyCustomRules(spark, config)

# In FraudDetector._apply_rules
claims = self.my_rules.check_my_pattern(claims)
```

### 4. Update Documentation

Add documentation for the new method in the appropriate guide.

## Release Process

1. Update version in `pyproject.toml`
2. Update CHANGELOG.md
3. Create release PR
4. After merge, tag the release
5. CI/CD deploys automatically

```bash
# Tag a release
git tag v0.2.0
git push origin v0.2.0
```
