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

The recommended way to run jobs on AWS is through the **event-driven pipeline** - simply upload files to S3 and the pipeline triggers automatically. See [Running in AWS](#running-in-aws) for details.

### Manual Spark Submit (Advanced)

If you need to manually run spark-submit on an EMR cluster (e.g., for debugging), SSH into the EMR master node and run:

```bash
# SSH into EMR master node first
spark-submit \
    --deploy-mode cluster \
    --master yarn \
    s3://fraud-detection-scripts-ACCOUNT-REGION/jobs/run_fraud_detection.py \
    --input s3://fraud-detection-data-ACCOUNT-REGION/claims/your-file.csv \
    --output s3://fraud-detection-results-ACCOUNT-REGION/flagged/
```

Note: Replace `ACCOUNT` and `REGION` with your AWS account ID and region. This approach is not recommended for production - use the S3 trigger instead.

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

### Event-Driven Processing

The pipeline is **event-driven** - it automatically triggers when you upload files to the S3 data bucket. When a file is uploaded to the `claims/` prefix, an EventBridge rule triggers the Step Functions pipeline to process that specific file.

### 1. Upload Claims Data to S3

```bash
# Generate sample data locally
uv run fraud-detect generate-sample --output /tmp/sample_data --num-claims 1000

# Get the data bucket name from SSM
DATA_BUCKET=$(aws ssm get-parameter \
    --name "/fraud-detection/data-bucket" \
    --query "Parameter.Value" \
    --output text)

# Upload to S3 - this automatically triggers the pipeline!
aws s3 cp /tmp/sample_data/claims.csv s3://$DATA_BUCKET/claims/
```

The pipeline will automatically start processing the uploaded file. Each file upload triggers a separate pipeline execution.

### 2. Monitor Pipeline Execution

Monitor progress in the AWS Console: **Step Functions → State Machines → fraud-detection-pipeline**

Or check running executions via CLI:

```bash
STATE_MACHINE_ARN=$(aws ssm get-parameter \
    --name "/fraud-detection/state-machine-arn" \
    --query "Parameter.Value" \
    --output text)

# List recent executions
aws stepfunctions list-executions \
    --state-machine-arn $STATE_MACHINE_ARN \
    --max-results 5
```

### Pipeline Flow

1. **S3 Upload** - File uploaded to `s3://{data-bucket}/claims/`
2. **EventBridge** - Detects S3 Object Created event
3. **Step Functions** - Starts pipeline execution with file path
4. **EMR Cluster** - Spins up (~5-10 min), runs fraud detection
5. **Results** - Written to `s3://{results-bucket}/flagged/`
6. **Cleanup** - EMR cluster terminates automatically

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

1. Go to **Athena** → **Query editor**
2. Select workgroup: `fraud-detection-workgroup` (dropdown in top right)
3. Go to the **Saved queries** tab
4. Run one of the pre-built queries:
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

## Troubleshooting

### Athena Queries Return No Results

The results table is partitioned by `detection_date`. When a new day's data is written, Athena won't see it until the partition is registered.

**Solution:** Run this in Athena to discover new partitions:

```sql
MSCK REPAIR TABLE fraud_detection_db.flagged_claims;
```

### Athena Schema Mismatch Error

If you see errors like `Field X's type Y is incompatible with type Z defined in table schema`, the partition metadata may be stale.

**Solution:** Drop and re-add the partition:

```sql
-- Drop the partition with stale schema
ALTER TABLE fraud_detection_db.flagged_claims
DROP PARTITION (detection_date='2025-12-12');

-- Re-discover with correct schema
MSCK REPAIR TABLE fraud_detection_db.flagged_claims;
```

### Step Functions Execution Failed

Check the execution details in the AWS Console:

1. Go to **Step Functions** → **State machines** → `fraud-detection-pipeline`
2. Click on the failed execution
3. Check which step failed and view the error message

For EMR step failures, check the logs:

```bash
LOGS_BUCKET=$(aws ssm get-parameter \
    --name "/fraud-detection/logs-bucket" \
    --query "Parameter.Value" \
    --output text)

# List recent cluster logs
aws s3 ls s3://$LOGS_BUCKET/emr-logs/ --recursive | tail -20

# View step stderr (replace CLUSTER_ID and STEP_ID)
aws s3 cp s3://$LOGS_BUCKET/emr-logs/CLUSTER_ID/steps/STEP_ID/stderr.gz - | gunzip
```

### Pipeline Not Triggering on S3 Upload

Verify the file is uploaded to the correct prefix:

```bash
# Must be under claims/ prefix
aws s3 cp data.csv s3://$DATA_BUCKET/claims/data.csv  # Triggers pipeline
aws s3 cp data.csv s3://$DATA_BUCKET/other/data.csv   # Does NOT trigger
```

Check EventBridge rule exists:

```bash
aws events list-rules --query "Rules[?contains(Name, 'fraud-detection')]"
```
