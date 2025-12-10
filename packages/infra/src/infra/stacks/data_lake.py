"""Data Lake stack with S3 buckets and Glue catalog."""

import aws_cdk as cdk
from aws_cdk import RemovalPolicy
from aws_cdk import aws_glue as glue
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from constructs import Construct


class DataLakeStack(cdk.Stack):
    """Stack for data lake resources including S3 and Glue."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        project_name: str,
        **kwargs: object,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)  # type: ignore[arg-type]

        # Raw data bucket for incoming claims
        self.data_bucket = s3.Bucket(
            self,
            "DataBucket",
            bucket_name=f"{project_name}-data-{self.account}-{self.region}",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            versioned=True,
            removal_policy=RemovalPolicy.RETAIN,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="TransitionToIA",
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INFREQUENT_ACCESS,
                            transition_after=cdk.Duration.days(90),
                        )
                    ],
                ),
                s3.LifecycleRule(
                    id="CleanupOldVersions",
                    noncurrent_version_expiration=cdk.Duration.days(30),
                ),
            ],
        )

        # Results bucket for processed/flagged claims
        self.results_bucket = s3.Bucket(
            self,
            "ResultsBucket",
            bucket_name=f"{project_name}-results-{self.account}-{self.region}",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            versioned=True,
            removal_policy=RemovalPolicy.RETAIN,
        )

        # Glue database for data catalog
        self.glue_database = glue.CfnDatabase(
            self,
            "GlueDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=f"{project_name.replace('-', '_')}_db",
                description="Insurance fraud detection data catalog",
            ),
        )

        # Glue crawler role
        crawler_role = iam.Role(
            self,
            "GlueCrawlerRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),  # type: ignore[arg-type]
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")],
        )

        self.data_bucket.grant_read(crawler_role)
        self.results_bucket.grant_read(crawler_role)

        # Glue crawler for raw claims data
        glue.CfnCrawler(
            self,
            "ClaimsCrawler",
            name=f"{project_name}-claims-crawler",
            role=crawler_role.role_arn,
            database_name=self.glue_database.ref,
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=f"s3://{self.data_bucket.bucket_name}/claims/",
                    )
                ]
            ),
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                update_behavior="UPDATE_IN_DATABASE",
                delete_behavior="LOG",
            ),
        )

        # Glue crawler for results
        glue.CfnCrawler(
            self,
            "ResultsCrawler",
            name=f"{project_name}-results-crawler",
            role=crawler_role.role_arn,
            database_name=self.glue_database.ref,
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=f"s3://{self.results_bucket.bucket_name}/flagged/",
                    )
                ]
            ),
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                update_behavior="UPDATE_IN_DATABASE",
                delete_behavior="LOG",
            ),
        )

        # Outputs
        cdk.CfnOutput(
            self,
            "DataBucketName",
            value=self.data_bucket.bucket_name,
            export_name=f"{project_name}-data-bucket",
        )

        cdk.CfnOutput(
            self,
            "ResultsBucketName",
            value=self.results_bucket.bucket_name,
            export_name=f"{project_name}-results-bucket",
        )

        cdk.CfnOutput(
            self,
            "GlueDatabaseName",
            value=self.glue_database.ref,
            export_name=f"{project_name}-glue-database",
        )
