"""Tests for CDK stacks."""

import aws_cdk as cdk
from aws_cdk.assertions import Template

from infra.stacks.analytics import AnalyticsStack
from infra.stacks.data_lake import DataLakeStack
from infra.stacks.processing import ProcessingStack


class TestDataLakeStack:
    """Tests for DataLakeStack."""

    def test_creates_data_bucket(self, app: cdk.App, env: cdk.Environment) -> None:
        """Test that data bucket is created with correct properties."""
        stack = DataLakeStack(app, "TestDataLake", project_name="test", env=env)
        template = Template.from_stack(stack)

        template.has_resource_properties(
            "AWS::S3::Bucket",
            {
                "BucketEncryption": {"ServerSideEncryptionConfiguration": [{"ServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]},
                "PublicAccessBlockConfiguration": {
                    "BlockPublicAcls": True,
                    "BlockPublicPolicy": True,
                    "IgnorePublicAcls": True,
                    "RestrictPublicBuckets": True,
                },
                "VersioningConfiguration": {"Status": "Enabled"},
            },
        )

    def test_creates_glue_database(self, app: cdk.App, env: cdk.Environment) -> None:
        """Test that Glue database is created."""
        stack = DataLakeStack(app, "TestDataLake", project_name="test", env=env)
        template = Template.from_stack(stack)

        template.has_resource_properties(
            "AWS::Glue::Database",
            {
                "DatabaseInput": {
                    "Description": "Insurance fraud detection data catalog",
                }
            },
        )

    def test_creates_glue_crawlers(self, app: cdk.App, env: cdk.Environment) -> None:
        """Test that Glue crawlers are created."""
        stack = DataLakeStack(app, "TestDataLake", project_name="test", env=env)
        template = Template.from_stack(stack)

        template.resource_count_is("AWS::Glue::Crawler", 2)


class TestProcessingStack:
    """Tests for ProcessingStack."""

    def test_creates_vpc(self, app: cdk.App, env: cdk.Environment) -> None:
        """Test that VPC is created."""
        data_lake = DataLakeStack(app, "TestDataLake", project_name="test", env=env)
        stack = ProcessingStack(
            app,
            "TestProcessing",
            project_name="test",
            data_bucket=data_lake.data_bucket,
            results_bucket=data_lake.results_bucket,
            env=env,
        )
        template = Template.from_stack(stack)

        template.has_resource_properties("AWS::EC2::VPC", {"EnableDnsHostnames": True})

    def test_creates_state_machine(self, app: cdk.App, env: cdk.Environment) -> None:
        """Test that Step Functions state machine is created."""
        data_lake = DataLakeStack(app, "TestDataLake", project_name="test", env=env)
        stack = ProcessingStack(
            app,
            "TestProcessing",
            project_name="test",
            data_bucket=data_lake.data_bucket,
            results_bucket=data_lake.results_bucket,
            env=env,
        )
        template = Template.from_stack(stack)

        template.resource_count_is("AWS::StepFunctions::StateMachine", 1)


class TestAnalyticsStack:
    """Tests for AnalyticsStack."""

    def test_creates_athena_workgroup(self, app: cdk.App, env: cdk.Environment) -> None:
        """Test that Athena workgroup is created."""
        data_lake = DataLakeStack(app, "TestDataLake", project_name="test", env=env)
        stack = AnalyticsStack(
            app,
            "TestAnalytics",
            project_name="test",
            data_bucket=data_lake.data_bucket,
            results_bucket=data_lake.results_bucket,
            glue_database=data_lake.glue_database,
            env=env,
        )
        template = Template.from_stack(stack)

        template.has_resource_properties(
            "AWS::Athena::WorkGroup",
            {
                "State": "ENABLED",
                "WorkGroupConfiguration": {
                    "EnforceWorkGroupConfiguration": True,
                    "PublishCloudWatchMetricsEnabled": True,
                },
            },
        )

    def test_creates_glue_tables(self, app: cdk.App, env: cdk.Environment) -> None:
        """Test that Glue tables are created."""
        data_lake = DataLakeStack(app, "TestDataLake", project_name="test", env=env)
        stack = AnalyticsStack(
            app,
            "TestAnalytics",
            project_name="test",
            data_bucket=data_lake.data_bucket,
            results_bucket=data_lake.results_bucket,
            glue_database=data_lake.glue_database,
            env=env,
        )
        template = Template.from_stack(stack)

        template.resource_count_is("AWS::Glue::Table", 2)

    def test_creates_named_queries(self, app: cdk.App, env: cdk.Environment) -> None:
        """Test that Athena named queries are created."""
        data_lake = DataLakeStack(app, "TestDataLake", project_name="test", env=env)
        stack = AnalyticsStack(
            app,
            "TestAnalytics",
            project_name="test",
            data_bucket=data_lake.data_bucket,
            results_bucket=data_lake.results_bucket,
            glue_database=data_lake.glue_database,
            env=env,
        )
        template = Template.from_stack(stack)

        template.resource_count_is("AWS::Athena::NamedQuery", 3)
