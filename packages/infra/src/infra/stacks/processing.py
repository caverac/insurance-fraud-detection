"""Processing stack with EMR for Spark jobs."""

import os
import subprocess
import tempfile

import aws_cdk as cdk
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets_events
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_deployment as s3_deploy
from aws_cdk import aws_ssm as ssm
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks
from constructs import Construct


class ProcessingStack(cdk.Stack):
    """Stack for data processing resources including EMR."""

    project_name: str
    scripts_bucket: s3.Bucket
    logs_bucket: s3.Bucket
    state_machine: sfn.StateMachine

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

        self.project_name = project_name

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
        self.scripts_bucket = scripts_bucket

        # Base path for fraud_detection package
        fraud_detection_pkg_path = os.path.abspath(
            os.path.join(
                os.path.dirname(__file__),
                "..",
                "..",
                "..",
                "..",
                "fraud_detection",
            )
        )

        # Deploy job scripts to S3
        jobs_path = os.path.join(fraud_detection_pkg_path, "src", "fraud_detection", "jobs")
        s3_deploy.BucketDeployment(
            self,
            "DeployJobScripts",
            sources=[s3_deploy.Source.asset(jobs_path)],
            destination_bucket=scripts_bucket,
            destination_key_prefix="jobs",
        )

        # Build wheel and deploy to S3
        dist_dir = tempfile.mkdtemp(prefix="fraud_detection_dist_")
        subprocess.run(
            ["uv", "build", "--wheel", "--out-dir", dist_dir],
            cwd=fraud_detection_pkg_path,
            check=True,
            capture_output=True,
        )
        s3_deploy.BucketDeployment(
            self,
            "DeployWheel",
            sources=[s3_deploy.Source.asset(dist_dir)],
            destination_bucket=scripts_bucket,
            destination_key_prefix="dist",
        )

        # Deploy bootstrap script to S3
        bootstrap_path = os.path.join(os.path.dirname(__file__), "..", "..", "..", "scripts")
        s3_deploy.BucketDeployment(
            self,
            "DeployBootstrap",
            sources=[s3_deploy.Source.asset(bootstrap_path)],
            destination_bucket=scripts_bucket,
            destination_key_prefix="scripts",
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
        self.logs_bucket = logs_bucket

        # EMR service role - using the older policy that has broader EC2 permissions
        emr_service_role = iam.Role(
            self,
            "EMRServiceRole",
            assumed_by=iam.ServicePrincipal("elasticmapreduce.amazonaws.com"),  # type: ignore[arg-type]
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonElasticMapReduceRole"),
            ],
        )

        # EMR EC2 instance role - name must match instance profile name for EMR
        emr_ec2_role_name = f"{project_name}-emr-ec2-role"
        emr_ec2_role = iam.Role(
            self,
            "EMREC2Role",
            role_name=emr_ec2_role_name,
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),  # type: ignore[arg-type]
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonElasticMapReduceforEC2Role")],
        )

        # Grant bucket access to EMR role
        data_bucket.grant_read(emr_ec2_role)
        results_bucket.grant_read_write(emr_ec2_role)
        scripts_bucket.grant_read(emr_ec2_role)
        logs_bucket.grant_write(emr_ec2_role)

        # Instance profile for EMR EC2 instances - must have same name as role for CDK EmrCreateCluster
        iam.CfnInstanceProfile(
            self,
            "EMRInstanceProfile",
            roles=[emr_ec2_role.role_name],
            instance_profile_name=emr_ec2_role_name,
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

        # Service access security group - required for private subnets
        service_access_sg = ec2.SecurityGroup(
            self,
            "EMRServiceAccessSG",
            vpc=vpc,
            description="Security group for EMR service access in private subnet",
            allow_all_outbound=True,
        )

        # Allow internal communication
        master_sg.add_ingress_rule(worker_sg, ec2.Port.all_traffic())
        worker_sg.add_ingress_rule(master_sg, ec2.Port.all_traffic())
        worker_sg.add_ingress_rule(worker_sg, ec2.Port.all_traffic())

        # Service access SG rules - allow communication with master/worker
        service_access_sg.add_ingress_rule(master_sg, ec2.Port.tcp(9443), "EMR service access from master")
        master_sg.add_ingress_rule(service_access_sg, ec2.Port.tcp(9443), "EMR service access to master")

        # Grant EMR service role permission to manage the security groups
        emr_service_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "ec2:AuthorizeSecurityGroupEgress",
                    "ec2:AuthorizeSecurityGroupIngress",
                    "ec2:RevokeSecurityGroupEgress",
                    "ec2:RevokeSecurityGroupIngress",
                ],
                resources=[
                    f"arn:aws:ec2:{self.region}:{self.account}:security-group/{master_sg.security_group_id}",
                    f"arn:aws:ec2:{self.region}:{self.account}:security-group/{worker_sg.security_group_id}",
                    f"arn:aws:ec2:{self.region}:{self.account}:security-group/{service_access_sg.security_group_id}",
                ],
            )
        )

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
                resources=[
                    f"arn:aws:elasticmapreduce:{self.region}:{self.account}:cluster/*",
                ],
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
            release_label="emr-7.5.0",
            service_role=emr_service_role,  # type: ignore[arg-type]
            cluster_role=emr_ec2_role,  # type: ignore[arg-type]
            log_uri=f"s3://{logs_bucket.bucket_name}/emr-logs/",
            instances=tasks.EmrCreateCluster.InstancesConfigProperty(
                ec2_subnet_id=vpc.private_subnets[0].subnet_id,
                emr_managed_master_security_group=master_sg.security_group_id,
                emr_managed_slave_security_group=worker_sg.security_group_id,
                service_access_security_group=service_access_sg.security_group_id,
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
                tasks.EmrCreateCluster.ConfigurationProperty(
                    classification="spark-env",
                    configurations=[
                        tasks.EmrCreateCluster.ConfigurationProperty(
                            classification="export",
                            properties={
                                "PYSPARK_PYTHON": "/usr/bin/python3.13",
                                "PYSPARK_DRIVER_PYTHON": "/usr/bin/python3.13",
                            },
                        ),
                    ],
                ),
            ],
            bootstrap_actions=[
                tasks.EmrCreateCluster.BootstrapActionConfigProperty(
                    name="InstallFraudDetectionPackage",
                    script_bootstrap_action=tasks.EmrCreateCluster.ScriptBootstrapActionConfigProperty(
                        path=f"s3://{scripts_bucket.bucket_name}/scripts/bootstrap.sh",
                        args=[scripts_bucket.bucket_name],
                    ),
                ),
            ],
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            result_path="$.Cluster",
        )

        # Add fraud detection step
        # Use result_path to store step output separately, preserving ClusterId for terminate step
        # Input path comes from EventBridge event via $.InputPath
        # Use args_as_json_path to enable dynamic values from state input
        fraud_detection_step = tasks.EmrAddStep(
            self,
            "RunFraudDetection",
            cluster_id=sfn.JsonPath.string_at("$.Cluster.ClusterId"),
            name="FraudDetectionJob",
            action_on_failure=tasks.ActionOnFailure.CONTINUE,
            jar="command-runner.jar",
            args=sfn.JsonPath.list_at("$.SparkArgs"),
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            result_path="$.StepResult",
        )

        # Terminate cluster
        terminate_cluster = tasks.EmrTerminateCluster(
            self,
            "TerminateCluster",
            cluster_id=sfn.JsonPath.string_at("$.Cluster.ClusterId"),
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
        )

        # Pass state to transform EventBridge input into the format we need
        # EventBridge S3 event has: detail.bucket.name and detail.object.key
        # We build the SparkArgs array that will be used by EmrAddStep
        transform_input = sfn.Pass(
            self,
            "TransformInput",
            parameters={
                "InputPath": sfn.JsonPath.format(
                    "s3://{}/{}",
                    sfn.JsonPath.string_at("$.detail.bucket.name"),
                    sfn.JsonPath.string_at("$.detail.object.key"),
                ),
                "SparkArgs": sfn.JsonPath.array(
                    "spark-submit",
                    "--deploy-mode",
                    "cluster",
                    "--class",
                    "org.apache.spark.deploy.SparkSubmit",
                    f"s3://{scripts_bucket.bucket_name}/jobs/run_fraud_detection.py",
                    "--input",
                    sfn.JsonPath.format(
                        "s3://{}/{}",
                        sfn.JsonPath.string_at("$.detail.bucket.name"),
                        sfn.JsonPath.string_at("$.detail.object.key"),
                    ),
                    "--output",
                    f"s3://{results_bucket.bucket_name}/flagged/",
                ),
            },
        )

        # Define state machine with input transformation
        definition = transform_input.next(create_cluster).next(fraud_detection_step).next(terminate_cluster)

        self.state_machine = sfn.StateMachine(
            self,
            "FraudDetectionPipeline",
            state_machine_name=f"{project_name}-pipeline",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            role=sfn_role,  # type: ignore[arg-type]
            timeout=cdk.Duration.hours(2),
        )

        # EventBridge rule to trigger Step Functions on S3 object creation
        # Only triggers for files in the claims/ prefix
        s3_event_rule = events.Rule(
            self,
            "S3ObjectCreatedRule",
            rule_name=f"{project_name}-s3-trigger",
            description="Triggers fraud detection pipeline when new claims data is uploaded",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                detail={
                    "bucket": {"name": events.Match.exact_string(data_bucket.bucket_name)},
                    "object": {"key": events.Match.prefix("claims/")},
                },
            ),
        )

        # Add Step Functions as target for the EventBridge rule
        s3_event_rule.add_target(
            targets_events.SfnStateMachine(
                self.state_machine,
                input=events.RuleTargetInput.from_event_path("$"),
            )
        )

        # Store SSM parameters
        self.store_ssm_parameters()

    def store_ssm_parameters(self) -> None:
        """Store SSM parameters for the stack."""
        ssm.StringParameter(
            self,
            "ScriptsBucketNameSSM",
            parameter_name=f"/{self.project_name}/scripts-bucket",
            string_value=self.scripts_bucket.bucket_name,
        )

        ssm.StringParameter(
            self,
            "LogsBucketNameSSM",
            parameter_name=f"/{self.project_name}/logs-bucket",
            string_value=self.logs_bucket.bucket_name,
        )

        ssm.StringParameter(
            self,
            "StateMachineArnSSM",
            parameter_name=f"/{self.project_name}/state-machine-arn",
            string_value=self.state_machine.state_machine_arn,
        )
