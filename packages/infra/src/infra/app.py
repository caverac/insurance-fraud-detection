#!/usr/bin/env python3
"""CDK application entry point."""
import aws_cdk as cdk

from infra.stacks.analytics import AnalyticsStack
from infra.stacks.data_lake import DataLakeStack
from infra.stacks.processing import ProcessingStack

app = cdk.App()

env = cdk.Environment(
    account=app.node.try_get_context("account"),
    region=app.node.try_get_context("region") or "us-east-1",
)

project_name = app.node.try_get_context("project_name") or "fraud-detection"

# Data Lake Stack - S3 buckets and Glue catalog
data_lake = DataLakeStack(
    app,
    f"{project_name}-data-lake",
    project_name=project_name,
    env=env,
)

# Processing Stack - EMR for Spark jobs
processing = ProcessingStack(
    app,
    f"{project_name}-processing",
    project_name=project_name,
    data_bucket=data_lake.data_bucket,
    results_bucket=data_lake.results_bucket,
    env=env,
)

# Analytics Stack - Athena and Glue
analytics = AnalyticsStack(
    app,
    f"{project_name}-analytics",
    project_name=project_name,
    data_bucket=data_lake.data_bucket,
    results_bucket=data_lake.results_bucket,
    glue_database=data_lake.glue_database,
    env=env,
)

app.synth()
