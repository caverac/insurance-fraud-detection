#!/bin/bash
# Cleanup script for fraud-detection AWS resources
# This script handles resources that may fail to delete during CDK destroy
# due to circular dependencies or non-empty resources.

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Check AWS CLI is configured
if ! aws sts get-caller-identity &>/dev/null; then
    log_error "AWS CLI not configured. Please run 'aws configure' or set AWS_PROFILE."
    exit 1
fi

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION=$(aws configure get region || echo "us-east-1")
log_info "Using AWS Account: $ACCOUNT_ID, Region: $REGION"

# 1. Delete Athena workgroup (must be empty first)
log_info "Cleaning up Athena workgroup..."
WORKGROUP="fraud-detection-workgroup"
if aws athena get-work-group --work-group "$WORKGROUP" &>/dev/null; then
    log_info "Deleting Athena workgroup: $WORKGROUP"
    aws athena delete-work-group --work-group "$WORKGROUP" --recursive-delete-option 2>/dev/null || true
    log_info "Athena workgroup deleted"
else
    log_info "Athena workgroup not found, skipping"
fi

# 2. Clean up EMR security groups with circular dependencies
log_info "Cleaning up EMR security groups..."
SG_IDS=$(aws ec2 describe-security-groups \
    --filters "Name=group-name,Values=*fraud-detection*EMR*" \
    --query "SecurityGroups[].GroupId" \
    --output text 2>/dev/null || true)

if [ -n "$SG_IDS" ]; then
    # First, revoke all ingress rules to break circular dependencies
    for sg in $SG_IDS; do
        log_info "Revoking ingress rules for security group: $sg"

        # Get all ingress rules referencing other security groups
        RULES=$(aws ec2 describe-security-group-rules \
            --filters "Name=group-id,Values=$sg" \
            --query "SecurityGroupRules[?!IsEgress && ReferencedGroupInfo.GroupId!=null].SecurityGroupRuleId" \
            --output text 2>/dev/null || true)

        for rule in $RULES; do
            aws ec2 revoke-security-group-ingress \
                --group-id "$sg" \
                --security-group-rule-ids "$rule" 2>/dev/null || true
        done
    done

    # Now delete the security groups
    for sg in $SG_IDS; do
        log_info "Deleting security group: $sg"
        aws ec2 delete-security-group --group-id "$sg" 2>/dev/null || {
            log_warn "Could not delete $sg - may have remaining dependencies"
        }
    done
else
    log_info "No EMR security groups found, skipping"
fi

# 3. Terminate any running EMR clusters
log_info "Checking for running EMR clusters..."
CLUSTER_IDS=$(aws emr list-clusters \
    --active \
    --query "Clusters[?Name=='fraud-detection-cluster'].Id" \
    --output text 2>/dev/null || true)

if [ -n "$CLUSTER_IDS" ]; then
    for cluster in $CLUSTER_IDS; do
        log_info "Terminating EMR cluster: $cluster"
        aws emr terminate-clusters --cluster-ids "$cluster"
    done
    log_info "Waiting for clusters to terminate..."
    sleep 30
else
    log_info "No active EMR clusters found, skipping"
fi

# 4. Stop any running Step Functions executions
log_info "Checking for running Step Functions executions..."
STATE_MACHINE_ARN=$(aws ssm get-parameter \
    --name "/fraud-detection/state-machine-arn" \
    --query "Parameter.Value" \
    --output text 2>/dev/null || true)

if [ -n "$STATE_MACHINE_ARN" ]; then
    RUNNING_EXECUTIONS=$(aws stepfunctions list-executions \
        --state-machine-arn "$STATE_MACHINE_ARN" \
        --status-filter RUNNING \
        --query "executions[].executionArn" \
        --output text 2>/dev/null || true)

    for exec in $RUNNING_EXECUTIONS; do
        log_info "Stopping execution: $exec"
        aws stepfunctions stop-execution --execution-arn "$exec" 2>/dev/null || true
    done
else
    log_info "State machine not found, skipping"
fi

# 5. Empty S3 buckets (CDK handles this with autoDeleteObjects, but just in case)
log_info "Checking S3 buckets..."
for param in "/fraud-detection/data-bucket" "/fraud-detection/results-bucket" "/fraud-detection/scripts-bucket" "/fraud-detection/logs-bucket"; do
    BUCKET=$(aws ssm get-parameter --name "$param" --query "Parameter.Value" --output text 2>/dev/null || true)
    if [ -n "$BUCKET" ]; then
        log_info "Bucket $BUCKET will be emptied by CDK autoDeleteObjects"
    fi
done

log_info "Cleanup complete!"
log_info ""
log_info "Now run CDK destroy:"
log_info "  cd packages/infra && yarn cdk destroy --all"
