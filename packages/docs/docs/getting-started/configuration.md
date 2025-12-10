# Configuration

The fraud detection system is highly configurable through the `DetectionConfig` class.

## Configuration Options

### Outlier Detection

| Parameter | Default | Description |
|-----------|---------|-------------|
| `outlier_zscore_threshold` | 3.0 | Z-score threshold for statistical outliers |
| `outlier_iqr_multiplier` | 1.5 | IQR multiplier for box-plot outliers |

### Duplicate Detection

| Parameter | Default | Description |
|-----------|---------|-------------|
| `duplicate_similarity_threshold` | 0.9 | Similarity score threshold (0-1) |
| `duplicate_time_window_days` | 30 | Time window for near-duplicate detection |

### Geographic Rules

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_provider_patient_distance_miles` | 500.0 | Maximum expected distance |

### Billing Pattern Rules

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_daily_procedures_per_provider` | 50 | Daily procedure limit |
| `max_claims_per_patient_per_day` | 5 | Patient daily claim limit |

### Scoring Weights

| Parameter | Default | Description |
|-----------|---------|-------------|
| `weight_rule_violation` | 0.3 | Weight for rule-based flags |
| `weight_statistical_anomaly` | 0.25 | Weight for statistical flags |
| `weight_duplicate` | 0.45 | Weight for duplicate detection |

## Using Configuration

### Python API

```python
from fraud_detection.detector import DetectionConfig, FraudDetector

# Custom configuration
config = DetectionConfig(
    outlier_zscore_threshold=2.5,
    duplicate_similarity_threshold=0.85,
    max_daily_procedures_per_provider=30,
    weight_duplicate=0.5,
)

detector = FraudDetector(spark, config)
results = detector.detect(claims)
```

### Command Line

The CLI currently uses default configuration values:

```bash
uv run fraud-detect run \
    --input ./data \
    --output ./results \
    --format csv \
    --local
```

For custom configurations, use the Python API directly or create a custom job script.

## Tuning Recommendations

### High Precision (Fewer False Positives)

For environments where false positives are costly:

```python
config = DetectionConfig(
    outlier_zscore_threshold=4.0,       # Stricter outlier threshold
    duplicate_similarity_threshold=0.95, # Higher similarity required
    weight_duplicate=0.5,                # Emphasize duplicates
)
```

### High Recall (Catch More Fraud)

For environments where missing fraud is costly:

```python
config = DetectionConfig(
    outlier_zscore_threshold=2.0,        # More sensitive
    duplicate_similarity_threshold=0.8,  # Lower similarity threshold
    max_daily_procedures_per_provider=30, # Stricter limits
)
```

### Healthcare-Specific Tuning

Different specialties have different norms:

```python
# Emergency medicine (higher volume expected)
emergency_config = DetectionConfig(
    max_daily_procedures_per_provider=100,
    max_claims_per_patient_per_day=10,
)

# Physical therapy (multiple visits common)
pt_config = DetectionConfig(
    max_claims_per_patient_per_day=3,
    duplicate_time_window_days=7,
)
```

## Custom Job Script

For production use with custom configuration, create a job script:

```python
# jobs/detect_fraud.py
from pyspark.sql import SparkSession
from fraud_detection.detector import DetectionConfig, FraudDetector

def main():
    spark = SparkSession.builder.appName("FraudDetection").getOrCreate()

    config = DetectionConfig(
        outlier_zscore_threshold=2.5,
        duplicate_similarity_threshold=0.85,
    )

    claims = spark.read.parquet("s3://bucket/claims/")
    detector = FraudDetector(spark, config)
    results = detector.detect(claims)
    results.write.parquet("s3://bucket/results/")

    spark.stop()

if __name__ == "__main__":
    main()
```

Submit to EMR:

```bash
spark-submit jobs/detect_fraud.py
```
