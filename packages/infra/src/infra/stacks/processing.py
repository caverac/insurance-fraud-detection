"""Processing stack with EMR for Spark jobs."""

import aws_cdk as cdk
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks
from constructs import Construct


class ProcessingStack(cdk.Stack):
    """Stack for data processing resources including EMR."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        project_name: str,
        data_bucket: s3.IBucket,
        results_bucket: s3.IBucket,
        **kwargs: object,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)  # type: ignore[arg-type]

        # VPC for EMR cluster
        vpc = ec2.Vpc(
            self,
            "ProcessingVpc",
            max_azs=2,
            nat_gateways=1,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="Public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24,
                ),
                ec2.SubnetConfiguration(
                    name="Private",
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                    cidr_mask=24,
                ),
            ],
        )

        # Scripts bucket for Spark jobs
        scripts_bucket = s3.Bucket(
            self,
            "ScriptsBucket",
            bucket_name=f"{project_name}-scripts-{self.account}-{self.region}",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        # Logs bucket for EMR
        logs_bucket = s3.Bucket(
            self,
            "LogsBucket",
            bucket_name=f"{project_name}-logs-{self.account}-{self.region}",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            lifecycle_rules=[
                s3.LifecycleRule(
                    expiration=cdk.Duration.days(30),
                )
            ],
        )

        # EMR service role
        emr_service_role = iam.Role(
            self,
            "EMRServiceRole",
            assumed_by=iam.ServicePrincipal("elasticmapreduce.amazonaws.com"),  # type: ignore[arg-type]
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonEMRServicePolicy_v2")],
        )

        # EMR EC2 instance role
        emr_ec2_role = iam.Role(
            self,
            "EMREC2Role",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),  # type: ignore[arg-type]
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonElasticMapReduceforEC2Role")],
        )

        # Grant bucket access to EMR role
        data_bucket.grant_read(emr_ec2_role)
        results_bucket.grant_read_write(emr_ec2_role)
        scripts_bucket.grant_read(emr_ec2_role)
        logs_bucket.grant_write(emr_ec2_role)

        # Instance profile for EMR EC2 instances
        iam.CfnInstanceProfile(
            self,
            "EMRInstanceProfile",
            roles=[emr_ec2_role.role_name],
            instance_profile_name=f"{project_name}-emr-instance-profile",
        )

        # Security groups
        master_sg = ec2.SecurityGroup(
            self,
            "EMRMasterSG",
            vpc=vpc,
            description="Security group for EMR master",
            allow_all_outbound=True,
        )

        worker_sg = ec2.SecurityGroup(
            self,
            "EMRWorkerSG",
            vpc=vpc,
            description="Security group for EMR workers",
            allow_all_outbound=True,
        )

        # Allow internal communication
        master_sg.add_ingress_rule(worker_sg, ec2.Port.all_traffic())
        worker_sg.add_ingress_rule(master_sg, ec2.Port.all_traffic())
        worker_sg.add_ingress_rule(worker_sg, ec2.Port.all_traffic())

        # Step Functions role for EMR orchestration
        sfn_role = iam.Role(
            self,
            "StepFunctionsRole",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com"),  # type: ignore[arg-type]
        )

        sfn_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "elasticmapreduce:RunJobFlow",
                    "elasticmapreduce:DescribeCluster",
                    "elasticmapreduce:TerminateJobFlows",
                    "elasticmapreduce:AddJobFlowSteps",
                    "elasticmapreduce:DescribeStep",
                    "elasticmapreduce:CancelSteps",
                ],
                resources=["*"],
            )
        )

        sfn_role.add_to_policy(
            iam.PolicyStatement(
                actions=["iam:PassRole"],
                resources=[emr_service_role.role_arn, emr_ec2_role.role_arn],
            )
        )

        # Create EMR cluster using Step Functions
        create_cluster = tasks.EmrCreateCluster(
            self,
            "CreateCluster",
            name=f"{project_name}-cluster",
            release_label="emr-7.0.0",
            service_role=emr_service_role,  # type: ignore[arg-type]
            cluster_role=emr_ec2_role,  # type: ignore[arg-type]
            log_uri=f"s3://{logs_bucket.bucket_name}/emr-logs/",
            instances=tasks.EmrCreateCluster.InstancesConfigProperty(
                ec2_subnet_id=vpc.private_subnets[0].subnet_id,
                emr_managed_master_security_group=master_sg.security_group_id,
                emr_managed_slave_security_group=worker_sg.security_group_id,
                instance_fleets=[
                    tasks.EmrCreateCluster.InstanceFleetConfigProperty(
                        instance_fleet_type=tasks.EmrCreateCluster.InstanceRoleType.MASTER,
                        target_on_demand_capacity=1,
                        instance_type_configs=[
                            tasks.EmrCreateCluster.InstanceTypeConfigProperty(
                                instance_type="m5.xlarge",
                            )
                        ],
                    ),
                    tasks.EmrCreateCluster.InstanceFleetConfigProperty(
                        instance_fleet_type=tasks.EmrCreateCluster.InstanceRoleType.CORE,
                        target_on_demand_capacity=2,
                        instance_type_configs=[
                            tasks.EmrCreateCluster.InstanceTypeConfigProperty(
                                instance_type="m5.xlarge",
                            )
                        ],
                    ),
                ],
            ),
            applications=[
                tasks.EmrCreateCluster.ApplicationConfigProperty(name="Spark"),
                tasks.EmrCreateCluster.ApplicationConfigProperty(name="Hadoop"),
            ],
            configurations=[
                tasks.EmrCreateCluster.ConfigurationProperty(
                    classification="spark-defaults",
                    properties={
                        "spark.sql.adaptive.enabled": "true",
                        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                    },
                ),
            ],
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
        )

        # Add fraud detection step
        fraud_detection_step = tasks.EmrAddStep(
            self,
            "RunFraudDetection",
            cluster_id=sfn.JsonPath.string_at("$.ClusterId"),
            name="FraudDetectionJob",
            action_on_failure=tasks.ActionOnFailure.CONTINUE,
            jar="command-runner.jar",
            args=[
                "spark-submit",
                "--deploy-mode",
                "cluster",
                "--class",
                "org.apache.spark.deploy.SparkSubmit",
                f"s3://{scripts_bucket.bucket_name}/jobs/fraud_detection.py",
                "--input",
                f"s3://{data_bucket.bucket_name}/claims/",
                "--output",
                f"s3://{results_bucket.bucket_name}/flagged/",
            ],
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
        )

        # Terminate cluster
        terminate_cluster = tasks.EmrTerminateCluster(
            self,
            "TerminateCluster",
            cluster_id=sfn.JsonPath.string_at("$.ClusterId"),
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
        )

        # Define state machine
        definition = create_cluster.next(fraud_detection_step).next(terminate_cluster)

        sfn.StateMachine(
            self,
            "FraudDetectionPipeline",
            state_machine_name=f"{project_name}-pipeline",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            role=sfn_role,  # type: ignore[arg-type]
            timeout=cdk.Duration.hours(2),
        )

        # Outputs
        cdk.CfnOutput(
            self,
            "ScriptsBucketName",
            value=scripts_bucket.bucket_name,
            export_name=f"{project_name}-scripts-bucket",
        )

        cdk.CfnOutput(
            self,
            "LogsBucketName",
            value=logs_bucket.bucket_name,
            export_name=f"{project_name}-logs-bucket",
        )
