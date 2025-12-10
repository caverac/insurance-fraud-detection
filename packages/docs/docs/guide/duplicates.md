# Duplicate Detection

Duplicate detection identifies claims that have been submitted multiple times, either exactly or with minor modifications.

## Why Duplicates Matter

Duplicate billing is one of the most common forms of healthcare fraud:

- **Accidental duplicates**: System errors, resubmissions
- **Intentional duplicates**: Billing same service multiple times
- **Near-duplicates**: Submitting with slight modifications to evade detection

## Detection Methods

### Exact Duplicate Detection

**Logic**: Identify claims with identical key fields.

**Key fields**:

- `patient_id`
- `provider_id`
- `procedure_code`
- `service_date`
- `charge_amount`

**Implementation**:
```python
from fraud_detection.rules.duplicates import DuplicateDetector

detector = DuplicateDetector(spark, config)
claims = detector.detect(claims)

# Results include:
# - is_exact_duplicate: boolean
# - exact_duplicate_of: original claim_id
```

### Near-Duplicate Detection

**Logic**: Find claims that are highly similar but not identical.

**Similarity calculation**:
```
similarity = (procedure_match * 0.6) + (charge_similarity * 0.4)
```

Where:

- `procedure_match`: 1.0 if same procedure, 0.0 otherwise
- `charge_similarity`: 1 - |charge_diff| / max(charge_a, charge_b)

**Conditions for matching**:

- Same patient and provider
- Within configurable time window (default: 30 days)
- Similarity score ≥ threshold (default: 0.9)

**Configuration**:
```python
config = DetectionConfig(
    duplicate_similarity_threshold=0.9,
    duplicate_time_window_days=30,
)
```

## Output Fields

| Field | Type | Description |
|-------|------|-------------|
| `is_duplicate` | boolean | True if exact or near duplicate |
| `is_exact_duplicate` | boolean | True if exact match |
| `is_near_duplicate` | boolean | True if fuzzy match |
| `duplicate_of` | string | Claim ID of original |
| `exact_duplicate_of` | string | Exact match reference |
| `near_duplicate_of` | string | Fuzzy match reference |

## Example Usage

```python
from pyspark.sql import SparkSession
from fraud_detection.rules.duplicates import DuplicateDetector
from fraud_detection.detector import DetectionConfig

spark = SparkSession.builder.master("local[*]").getOrCreate()

# Load claims
claims = spark.read.parquet("s3://bucket/claims/")

# Configure and run
config = DetectionConfig(
    duplicate_similarity_threshold=0.85,  # More sensitive
    duplicate_time_window_days=45,        # Wider window
)
detector = DuplicateDetector(spark, config)

results = detector.detect(claims)

# View duplicates
duplicates = results.filter(results.is_duplicate == True)
duplicates.select(
    "claim_id",
    "duplicate_of",
    "is_exact_duplicate",
    "is_near_duplicate",
    "charge_amount"
).show()
```

## Handling Duplicate Groups

When multiple duplicates exist, the system:

1. Orders claims by `claim_id` (or submission date if available)
2. Marks first claim as original
3. All subsequent matches reference the first claim

**Example**:
```
CLM001 (original) <- CLM002 (duplicate)
                  <- CLM003 (duplicate)
                  <- CLM004 (duplicate)
```

## Tuning Recommendations

### High Precision

For minimizing false positives:

```python
config = DetectionConfig(
    duplicate_similarity_threshold=0.95,  # Very high similarity
    duplicate_time_window_days=7,         # Tight window
)
```

### High Recall

For catching more potential duplicates:

```python
config = DetectionConfig(
    duplicate_similarity_threshold=0.80,  # Lower threshold
    duplicate_time_window_days=60,        # Wider window
)
```

## Common Scenarios

### Legitimate Resubmissions

Some duplicates are legitimate:

- Claim corrections (different claim ID, same service)
- Recurring services (monthly supplies)
- Split payments

**Mitigation**: Review duplicate flags in context, maintain exception lists.

### Evasion Tactics

Fraudsters may try to evade duplicate detection:

- Slightly varying charge amounts
- Different procedure code with same service
- Submitting through different systems

**Mitigation**: Lower similarity threshold, broader matching criteria.

## Performance Considerations

Near-duplicate detection requires self-join operations which can be expensive.

**Optimizations**:

1. **Partition by patient_id**: Reduces join size
2. **Time window filtering**: Limits comparison scope
3. **Pre-aggregation**: Group by key fields first

```python
# The detector automatically optimizes by:
# - Filtering to same patient/provider pairs
# - Limiting to time window
# - Using window functions where possible
```

## Integration with Fraud Score

Duplicate detection has the highest weight in the fraud score (default: 0.45) because:

1. High precision signal
2. Directly measurable
3. Clear fraud indicator

```python
fraud_score = (
    rule_score * 0.30 +
    statistical_score * 0.25 +
    duplicate_score * 0.45  # Highest weight
)
```
