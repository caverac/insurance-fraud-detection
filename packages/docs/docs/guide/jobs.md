# Running Jobs

This guide covers running fraud detection jobs locally and on AWS EMR.

## Local Execution

### Using CLI

```bash
uv run fraud-detect run \
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
| `--input`, `-i` | Input data path | Required |
| `--output`, `-o` | Output path | Required |
| `--format`, `-f` | Data format (csv, parquet, json) | parquet |
| `--local` | Run in local Spark mode | False |

### Custom Configuration

The CLI uses default detection thresholds. For custom thresholds, use the Python API or create a custom job script:

```python
# custom_job.py
from pyspark.sql import SparkSession
from fraud_detection import FraudDetector
from fraud_detection.detector import DetectionConfig

spark = SparkSession.builder.appName("CustomFraudDetection").getOrCreate()

claims = spark.read.parquet("s3://bucket/claims/")

# Custom configuration
config = DetectionConfig(
    outlier_zscore_threshold=2.0,
    duplicate_similarity_threshold=0.8,
)

detector = FraudDetector(spark, config)
results = detector.detect(claims)
results.write.parquet("s3://bucket/results/")
```

Run with spark-submit:

```bash
spark-submit --deploy-mode cluster custom_job.py
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

## Running in AWS

Before running jobs in AWS, you must deploy the infrastructure. See [Deploy All Stacks](../architecture/aws.md#deploy-all-stacks) for deployment instructions.

### 1. Upload Sample Data to S3

```bash
# Generate sample data locally
uv run fraud-detect generate-sample --output /tmp/sample_data --num-claims 1000

# Get the data bucket name from SSM
DATA_BUCKET=$(aws ssm get-parameter \
    --name "/fraud-detection/data-bucket" \
    --query "Parameter.Value" \
    --output text)

# Upload to S3
aws s3 cp /tmp/sample_data/ s3://$DATA_BUCKET/claims/ --recursive
```

### 2. Run the Step Functions Pipeline

```bash
# Get the state machine ARN from SSM
STATE_MACHINE_ARN=$(aws ssm get-parameter \
    --name "/fraud-detection/state-machine-arn" \
    --query "Parameter.Value" \
    --output text)

# Start execution
aws stepfunctions start-execution \
    --state-machine-arn $STATE_MACHINE_ARN \
    --input '{}'
```

Monitor progress in the AWS Console: **Step Functions → State Machines → fraud-detection-pipeline**

### 3. Query Results with Athena

Once the pipeline completes, query results using the saved queries:

```bash
# Get the results bucket from SSM
RESULTS_BUCKET=$(aws ssm get-parameter \
    --name "/fraud-detection/results-bucket" \
    --query "Parameter.Value" \
    --output text)

# Verify results exist
aws s3 ls s3://$RESULTS_BUCKET/flagged/
```

In the AWS Console:

1. Go to **Athena**
2. Select workgroup: `fraud-detection-workgroup`
3. Run the saved queries:
    - **High Risk Providers** - Providers with highest fraud scores
    - **Duplicate Claims** - All detected duplicates
    - **Fraud by Rule Type** - Breakdown by rule violations

Or query via CLI:

```bash
# Get workgroup and database from SSM
WORKGROUP=$(aws ssm get-parameter \
    --name "/fraud-detection/athena-workgroup" \
    --query "Parameter.Value" \
    --output text)

DATABASE=$(aws ssm get-parameter \
    --name "/fraud-detection/glue-database" \
    --query "Parameter.Value" \
    --output text)

aws athena start-query-execution \
    --query-string "SELECT * FROM flagged_claims WHERE fraud_score > 0.5 LIMIT 100" \
    --work-group $WORKGROUP \
    --query-execution-context Database=$DATABASE
```

### SSM Parameters Reference

All stack outputs are stored in SSM Parameter Store under the `/fraud-detection/` prefix:

| Parameter | Description |
|-----------|-------------|
| `/fraud-detection/data-bucket` | Raw claims data bucket |
| `/fraud-detection/results-bucket` | Fraud detection results bucket |
| `/fraud-detection/glue-database` | Glue catalog database name |
| `/fraud-detection/scripts-bucket` | Spark job scripts bucket |
| `/fraud-detection/logs-bucket` | EMR logs bucket |
| `/fraud-detection/state-machine-arn` | Step Functions state machine ARN |
| `/fraud-detection/athena-workgroup` | Athena workgroup name |
| `/fraud-detection/query-results-bucket` | Athena query results bucket |
