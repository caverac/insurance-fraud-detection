# Quick Start

This guide walks you through running fraud detection on sample data locally.

## Generate Sample Data

First, generate synthetic claims data for testing:

```bash
make sample-data
# Or: uv run fraud-detect generate-sample --output ./sample_data --num-claims 10000 --fraud-rate 0.05
```

This creates a dataset with:

- 10,000 claims
- ~5% intentionally fraudulent patterns
- Realistic provider, patient, and procedure distributions

## Run Fraud Detection

Run the detection pipeline on the sample data:

```bash
make run-local
# Or: uv run fraud-detect run --input ./sample_data --output ./results --format csv --local
```

Options:

- `--local`: Run in local Spark mode (no cluster required)
- `--format`: Input/output format (csv, parquet, json)

## Analyze Results

View a summary of the detection results:

```bash
make analyze
# Or: uv run fraud-detect analyze --results ./results --report summary
```

Example output:

```
=== Fraud Detection Summary ===

Total claims analyzed: 10,000
High risk (>0.7):      312 (3.1%)
Medium risk (0.3-0.7): 847 (8.5%)
Low risk (≤0.3):       8,841 (88.4%)

=== Score Distribution ===

+------+-------+-----+-----+
| mean | stddev| min | max |
+------+-------+-----+-----+
| 0.12 |  0.23 | 0.0 | 0.95|
+------+-------+-----+-----+
```

## View High-Risk Providers

Identify providers with the most suspicious patterns:

```bash
fraud-detect analyze --results ./results --report providers
```

## Using Python API

You can also use the fraud detection library directly:

```python
from pyspark.sql import SparkSession
from fraud_detection import FraudDetector
from fraud_detection.detector import DetectionConfig

# Create Spark session
spark = SparkSession.builder \
    .appName("FraudDetection") \
    .master("local[*]") \
    .getOrCreate()

# Load claims data
claims = spark.read.option("header", "true").csv("./sample_data")

# Configure detection
config = DetectionConfig(
    outlier_zscore_threshold=3.0,
    duplicate_similarity_threshold=0.9,
)

# Run detection
detector = FraudDetector(spark, config)
results = detector.detect(claims)

# View high-risk claims
high_risk = results.filter(results.fraud_score > 0.7)
high_risk.show()
```

## Next Steps

- [Configuration](configuration.md): Customize detection thresholds
- [Rule-Based Detection](../guide/rules.md): Understand the rules engine
- [AWS Deployment](../architecture/aws.md): Deploy to production
