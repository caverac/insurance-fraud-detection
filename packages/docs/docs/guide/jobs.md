# Running Jobs

This guide covers running fraud detection jobs locally and on AWS EMR.

## Local Execution

### Using CLI

```bash
fraud-detect run \
    --input ./data/claims.csv \
    --output ./results \
    --format csv \
    --local
```

### Using Python

```python
from pyspark.sql import SparkSession
from fraud_detection import FraudDetector

spark = SparkSession.builder \
    .appName("FraudDetection") \
    .master("local[*]") \
    .getOrCreate()

claims = spark.read.option("header", "true").csv("./data/claims.csv")
detector = FraudDetector(spark)
results = detector.detect(claims)
results.write.parquet("./results")
```

## AWS EMR Execution

### Spark Submit

```bash
spark-submit \
    --deploy-mode cluster \
    --master yarn \
    --num-executors 10 \
    --executor-memory 4g \
    --executor-cores 2 \
    s3://bucket/scripts/fraud_detection.py \
    --input s3://bucket/claims/ \
    --output s3://bucket/results/
```

### Step Functions Pipeline

The CDK infrastructure includes a Step Functions state machine that:

1. Creates an EMR cluster
2. Runs the fraud detection job
3. Terminates the cluster

**Trigger via AWS Console or CLI**:

```bash
aws stepfunctions start-execution \
    --state-machine-arn arn:aws:states:us-east-1:123456789:stateMachine:fraud-detection-pipeline \
    --input '{}'
```

## Job Configuration

### Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--input` | Input data path | Required |
| `--output` | Output path | Required |
| `--input-format` | csv, parquet, json | parquet |
| `--output-format` | csv, parquet, json | parquet |
| `--zscore-threshold` | Outlier threshold | 3.0 |
| `--iqr-multiplier` | IQR multiplier | 1.5 |
| `--duplicate-threshold` | Similarity threshold | 0.9 |
| `--min-fraud-score` | Minimum score to output | 0.0 |
| `--partition-by` | Output partitions | detection_date |

### Example: High-Sensitivity Run

```bash
fraud-detect run \
    --input s3://bucket/claims/ \
    --output s3://bucket/high-sensitivity-results/ \
    --zscore-threshold 2.0 \
    --duplicate-threshold 0.8 \
    --min-fraud-score 0.2
```

## Scheduling

### AWS EventBridge

Schedule daily runs using EventBridge:

```python
# In CDK
events.Rule(
    self,
    "DailyFraudDetection",
    schedule=events.Schedule.cron(hour="2", minute="0"),
    targets=[
        targets.SfnStateMachine(state_machine)
    ],
)
```

### Airflow DAG

```python
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator

with DAG("fraud_detection", schedule_interval="@daily") as dag:
    detect_fraud = EmrAddStepsOperator(
        task_id="detect_fraud",
        job_flow_id="{{ var.value.emr_cluster_id }}",
        steps=[{
            "Name": "Fraud Detection",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "s3://bucket/scripts/fraud_detection.py",
                    "--input", "s3://bucket/claims/",
                    "--output", "s3://bucket/results/{{ ds }}/",
                ]
            }
        }]
    )
```

## Monitoring

### Job Metrics

The job logs key metrics:

```
INFO: Starting fraud detection job
INFO: Input: s3://bucket/claims/
INFO: Loaded 1,234,567 claims
INFO: Running fraud detection
INFO: Total flagged claims: 45,678
INFO: High risk (score > 0.7): 3,456
INFO: Duplicates detected: 12,345
INFO: Writing results to s3://bucket/results/
INFO: Fraud detection job completed successfully
```

### CloudWatch Metrics

When running on EMR, metrics are sent to CloudWatch:

- `ClaimsProcessed`
- `FlaggedClaims`
- `HighRiskClaims`
- `DuplicatesDetected`
- `ProcessingTimeSeconds`

### Alerting

Set up CloudWatch alarms:

```python
# Alert if high-risk claims exceed threshold
cloudwatch.Alarm(
    self,
    "HighRiskAlert",
    metric=cloudwatch.Metric(
        namespace="FraudDetection",
        metric_name="HighRiskClaims",
    ),
    threshold=1000,
    evaluation_periods=1,
    alarm_actions=[sns_topic],
)
```

## Error Handling

### Retries

The Step Functions workflow includes retry logic:

```python
tasks.EmrAddStep(
    # ...
    result_path="$.step_result",
).add_retry(
    errors=["States.TaskFailed"],
    max_attempts=2,
    backoff_rate=2,
)
```

### Failure Notifications

Failed jobs trigger SNS notifications:

```bash
# Subscribe to failure notifications
aws sns subscribe \
    --topic-arn arn:aws:sns:us-east-1:123456789:fraud-detection-alerts \
    --protocol email \
    --notification-endpoint your@email.com
```

## Performance Tuning

### Spark Configuration

```bash
spark-submit \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.shuffle.partitions=200 \
    --conf spark.default.parallelism=100 \
    --conf spark.executor.memoryOverhead=1g \
    fraud_detection.py
```

### Data Partitioning

Partition input data for better performance:

```
s3://bucket/claims/
  year=2024/
    month=01/
      part-00000.parquet
      part-00001.parquet
    month=02/
      ...
```

### Caching

For iterative analysis, cache intermediate results:

```python
claims = spark.read.parquet("s3://bucket/claims/")
claims = claims.cache()  # Cache for reuse

# Run multiple analyses
result1 = detector.detect(claims)
result2 = analyzer.analyze(claims)
```
