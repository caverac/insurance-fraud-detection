"""Analytics stack with Athena workgroups and Glue tables."""

import aws_cdk as cdk
from aws_cdk import aws_athena as athena
from aws_cdk import aws_glue as glue
from aws_cdk import aws_s3 as s3
from constructs import Construct


class AnalyticsStack(cdk.Stack):
    """Stack for analytics resources including Athena."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        project_name: str,
        data_bucket: s3.IBucket,
        results_bucket: s3.IBucket,
        glue_database: glue.CfnDatabase,
        **kwargs: object,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)  # type: ignore[arg-type]

        # Athena query results bucket
        query_results_bucket = s3.Bucket(
            self,
            "QueryResultsBucket",
            bucket_name=f"{project_name}-query-results-{self.account}-{self.region}",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            lifecycle_rules=[
                s3.LifecycleRule(
                    expiration=cdk.Duration.days(7),
                )
            ],
        )

        # Athena workgroup
        workgroup = athena.CfnWorkGroup(
            self,
            "AthenaWorkgroup",
            name=f"{project_name}-workgroup",
            description="Workgroup for fraud detection queries",
            state="ENABLED",
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                enforce_work_group_configuration=True,
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{query_results_bucket.bucket_name}/results/",
                    encryption_configuration=athena.CfnWorkGroup.EncryptionConfigurationProperty(
                        encryption_option="SSE_S3",
                    ),
                ),
                publish_cloud_watch_metrics_enabled=True,
                bytes_scanned_cutoff_per_query=10737418240,  # 10 GB limit
            ),
        )

        # Glue table for raw claims
        glue.CfnTable(
            self,
            "ClaimsTable",
            catalog_id=self.account,
            database_name=glue_database.ref,
            table_input=glue.CfnTable.TableInputProperty(
                name="claims",
                description="Raw insurance claims data",
                table_type="EXTERNAL_TABLE",
                parameters={
                    "classification": "parquet",
                    "has_encrypted_data": "false",
                },
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    location=f"s3://{data_bucket.bucket_name}/claims/",
                    input_format=("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"),
                    output_format=("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"),
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library=("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"),
                    ),
                    columns=[
                        glue.CfnTable.ColumnProperty(name="claim_id", type="string"),
                        glue.CfnTable.ColumnProperty(name="patient_id", type="string"),
                        glue.CfnTable.ColumnProperty(name="provider_id", type="string"),
                        glue.CfnTable.ColumnProperty(name="provider_name", type="string"),
                        glue.CfnTable.ColumnProperty(name="procedure_code", type="string"),
                        glue.CfnTable.ColumnProperty(name="diagnosis_code", type="string"),
                        glue.CfnTable.ColumnProperty(name="service_date", type="date"),
                        glue.CfnTable.ColumnProperty(name="submitted_date", type="date"),
                        glue.CfnTable.ColumnProperty(name="charge_amount", type="decimal(10,2)"),
                        glue.CfnTable.ColumnProperty(name="paid_amount", type="decimal(10,2)"),
                        glue.CfnTable.ColumnProperty(name="patient_state", type="string"),
                        glue.CfnTable.ColumnProperty(name="provider_state", type="string"),
                        glue.CfnTable.ColumnProperty(name="place_of_service", type="string"),
                    ],
                ),
                partition_keys=[
                    glue.CfnTable.ColumnProperty(name="year", type="int"),
                    glue.CfnTable.ColumnProperty(name="month", type="int"),
                ],
            ),
        )

        # Glue table for flagged claims
        glue.CfnTable(
            self,
            "FlaggedClaimsTable",
            catalog_id=self.account,
            database_name=glue_database.ref,
            table_input=glue.CfnTable.TableInputProperty(
                name="flagged_claims",
                description="Claims flagged for potential fraud",
                table_type="EXTERNAL_TABLE",
                parameters={
                    "classification": "parquet",
                    "has_encrypted_data": "false",
                },
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    location=f"s3://{results_bucket.bucket_name}/flagged/",
                    input_format=("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"),
                    output_format=("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"),
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library=("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"),
                    ),
                    columns=[
                        glue.CfnTable.ColumnProperty(name="claim_id", type="string"),
                        glue.CfnTable.ColumnProperty(name="patient_id", type="string"),
                        glue.CfnTable.ColumnProperty(name="provider_id", type="string"),
                        glue.CfnTable.ColumnProperty(name="charge_amount", type="decimal(10,2)"),
                        glue.CfnTable.ColumnProperty(name="fraud_score", type="double"),
                        glue.CfnTable.ColumnProperty(name="fraud_reasons", type="array<string>"),
                        glue.CfnTable.ColumnProperty(name="rule_violations", type="array<string>"),
                        glue.CfnTable.ColumnProperty(name="statistical_flags", type="array<string>"),
                        glue.CfnTable.ColumnProperty(name="is_duplicate", type="boolean"),
                        glue.CfnTable.ColumnProperty(name="duplicate_of", type="string"),
                        glue.CfnTable.ColumnProperty(name="processed_at", type="timestamp"),
                    ],
                ),
                partition_keys=[
                    glue.CfnTable.ColumnProperty(name="detection_date", type="date"),
                ],
            ),
        )

        # Named queries for common fraud analysis
        athena.CfnNamedQuery(
            self,
            "HighRiskProvidersQuery",
            database=glue_database.ref,
            work_group=workgroup.name,
            name="High Risk Providers",
            description="Find providers with highest fraud scores",
            query_string=f"""
SELECT
    provider_id,
    COUNT(*) as flagged_claims,
    AVG(fraud_score) as avg_fraud_score,
    SUM(charge_amount) as total_charges
FROM {glue_database.ref}.flagged_claims
WHERE fraud_score > 0.7
GROUP BY provider_id
ORDER BY avg_fraud_score DESC
LIMIT 100
""",
        )

        athena.CfnNamedQuery(
            self,
            "DuplicateClaimsQuery",
            database=glue_database.ref,
            work_group=workgroup.name,
            name="Duplicate Claims",
            description="Find all duplicate claims",
            query_string=f"""
SELECT
    claim_id,
    duplicate_of,
    charge_amount,
    processed_at
FROM {glue_database.ref}.flagged_claims
WHERE is_duplicate = true
ORDER BY processed_at DESC
""",
        )

        athena.CfnNamedQuery(
            self,
            "FraudByRuleQuery",
            database=glue_database.ref,
            work_group=workgroup.name,
            name="Fraud by Rule Type",
            description="Breakdown of fraud flags by rule type",
            query_string=f"""
SELECT
    rule,
    COUNT(*) as count,
    SUM(charge_amount) as total_amount
FROM {glue_database.ref}.flagged_claims
CROSS JOIN UNNEST(rule_violations) AS t(rule)
GROUP BY rule
ORDER BY count DESC
""",
        )

        # Outputs
        cdk.CfnOutput(
            self,
            "AthenaWorkgroupName",
            value=workgroup.name or "",
            export_name=f"{project_name}-athena-workgroup",
        )

        cdk.CfnOutput(
            self,
            "QueryResultsBucketName",
            value=query_results_bucket.bucket_name,
            export_name=f"{project_name}-query-results-bucket",
        )
