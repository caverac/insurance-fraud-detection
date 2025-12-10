# Statistical Detection

Statistical detection methods identify anomalies without explicit rules by analyzing data distributions.

## Outlier Detection

### Z-Score Method

**Concept**: Measures how many standard deviations a value is from the mean.

**Formula**:

$$
z = \frac{x - \mu}{\sigma}
$$

**Flagged when**: $|z| > \mathrm{threshold}$ (default: 3.0)

**Usage**:
```python
from fraud_detection.statistics.outliers import OutlierDetector

detector = OutlierDetector(spark, config)
claims = detector.detect_zscore_outliers(
    claims,
    column="charge_amount",
    output_column="charge_outlier",
    group_by=["procedure_code"]  # Optional grouping
)
```

**Advantages**:

- Simple and interpretable
- Works well for normally distributed data

**Limitations**:

- Sensitive to extreme outliers (which affect mean/std)
- Assumes normal distribution

### IQR Method

**Concept**: Uses quartiles to define "normal" range, robust to extreme values.

**Formula**:
```
IQR = Q3 - Q1
Lower bound = Q1 - k * IQR
Upper bound = Q3 + k * IQR
```

**Flagged when**: Value outside bounds (k default: 1.5)

**Usage**:
```python
claims = detector.detect_iqr_outliers(
    claims,
    column="charge_amount",
    output_column="charge_iqr_outlier",
)
```

**Advantages**:

- Robust to extreme outliers
- No distribution assumption

**Limitations**:

- Less sensitive than Z-score
- May miss moderate anomalies

## Benford's Law Analysis

### Concept

Benford's Law states that in many naturally occurring datasets, the leading digit follows a specific distribution:

| Digit | Expected Frequency |
|-------|-------------------|
| 1 | 30.1% |
| 2 | 17.6% |
| 3 | 12.5% |
| 4 | 9.7% |
| 5 | 7.9% |
| 6 | 6.7% |
| 7 | 5.8% |
| 8 | 5.1% |
| 9 | 4.6% |

Fraudulent data often violates this distribution because humans tend to:

- Prefer "random-looking" distributions
- Avoid 1s as leading digits
- Favor round numbers

### Usage

```python
from fraud_detection.statistics.benfords import BenfordsLawAnalyzer

analyzer = BenfordsLawAnalyzer(spark)

# Analyze by provider
claims = analyzer.analyze(
    claims,
    column="charge_amount",
    group_by="provider_id",
    threshold=0.15  # Max deviation from expected
)

# Generate detailed report
report = analyzer.get_distribution_report(
    claims,
    column="charge_amount",
    group_by="provider_id"
)
report.show()
```

**Output**:
```
+----------+-----------+--------+-------------------+-------------------+----------+
|first_digit|    count|  total|observed_frequency|expected_frequency| deviation|
+----------+-----------+--------+-------------------+-------------------+----------+
|         1|     3012|  10000|             0.3012|             0.3010|    0.0002|
|         2|     1821|  10000|             0.1821|             0.1760|    0.0061|
...
```

### When Benford's Law Applies

✅ Applies to:

- Financial transactions
- Population data
- Physical measurements
- Geographic data

❌ Does not apply to:

- Assigned numbers (SSN, phone numbers)
- Constrained ranges (percentages, test scores)
- Small datasets

## Provider Billing Analysis

### Concept

Compare each provider's billing patterns against market norms.

```python
claims = detector.detect_provider_outliers(
    claims,
    charge_column="charge_amount",
    provider_column="provider_id",
    procedure_column="procedure_code"
)
```

**Metrics calculated**:

- Provider's average charge per procedure
- Market average per procedure
- Deviation ratio (provider / market)

**Flagged when**: Ratio > 2.0 or < 0.5

### Temporal Analysis

Detect sudden changes in billing patterns over time:

```python
claims = detector.detect_temporal_outliers(
    claims,
    charge_column="charge_amount",
    date_column="service_date"
)
```

**Logic**: Compare weekly average against 4-week rolling average.

**Flagged when**: Current > 3* rolling average

## Combining Statistical Methods

Best practice is to combine multiple methods:

```python
# Apply multiple outlier methods
claims = detector.detect_zscore_outliers(claims, "charge_amount", "zscore_flag")
claims = detector.detect_iqr_outliers(claims, "charge_amount", "iqr_flag")
claims = analyzer.analyze(claims, "charge_amount")

# Flag if multiple methods agree
claims = claims.withColumn(
    "strong_statistical_flag",
    (F.col("zscore_flag") & F.col("iqr_flag")) |
    (F.col("benfords_anomaly") & (F.col("zscore_flag") | F.col("iqr_flag")))
)
```

## Tuning Statistical Thresholds

| Scenario | Z-score | IQR k | Benford Threshold |
|----------|---------|-------|-------------------|
| High precision | 4.0 | 2.0 | 0.20 |
| Balanced | 3.0 | 1.5 | 0.15 |
| High recall | 2.0 | 1.0 | 0.10 |

## Limitations

1. **Requires sufficient data**: Statistical methods need volume to be meaningful
2. **Assumes patterns**: Legitimate outliers will be flagged
3. **Context-blind**: Doesn't understand business reasons for anomalies
4. **Gaming risk**: Sophisticated fraudsters can evade statistical detection

Always combine statistical methods with rule-based and duplicate detection for best results.
