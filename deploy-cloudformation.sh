#!/usr/bin/env bash
set -euo pipefail

# --- Usage ---
# One-command AWS deployment for Heimdall.
# Creates CloudFormation stack, sets SSM secrets, and stops RDS to save costs.
#
# ./deploy-cloudformation.sh
#
# All parameters are prompted interactively with sensible defaults.
# ----------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ──────────────────────────────────────────────
# 1. Check prerequisites
# ──────────────────────────────────────────────

if ! command -v aws &>/dev/null; then
  echo "Error: AWS CLI is not installed."
  echo "Install it from https://aws.amazon.com/cli/"
  exit 1
fi

if ! aws sts get-caller-identity &>/dev/null; then
  echo "Error: AWS credentials not configured or expired."
  echo "Run 'aws configure' or set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY."
  exit 1
fi

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

echo "╔════════════════════════════════════════════════════════════╗"
echo "║         Heimdall — One-Command AWS Deployment             ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo
echo "AWS Account: $ACCOUNT_ID"
echo

# ──────────────────────────────────────────────
# 2. Prompt for stack name and region
# ──────────────────────────────────────────────

read -rp "Stack name [heimdall]: " STACK_NAME
STACK_NAME="${STACK_NAME:-heimdall}"

DEFAULT_REGION="${AWS_DEFAULT_REGION:-eu-north-1}"
read -rp "AWS Region [$DEFAULT_REGION]: " AWS_REGION
AWS_REGION="${AWS_REGION:-$DEFAULT_REGION}"
export AWS_DEFAULT_REGION="$AWS_REGION"

# Check if stack already exists
if aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$AWS_REGION" &>/dev/null; then
  echo
  echo "Error: Stack '$STACK_NAME' already exists."
  echo "Use 'aws cloudformation update-stack' to update."
  exit 1
fi

# ──────────────────────────────────────────────
# 3. Auto-detect VPC and subnets
# ──────────────────────────────────────────────

echo
echo "Detecting VPC..."

DEFAULT_VPC_ID=$(aws ec2 describe-vpcs \
  --filters Name=isDefault,Values=true \
  --query "Vpcs[0].VpcId" \
  --output text \
  --region "$AWS_REGION" 2>/dev/null || echo "None")

if [ "$DEFAULT_VPC_ID" != "None" ] && [ -n "$DEFAULT_VPC_ID" ]; then
  echo "Found default VPC: $DEFAULT_VPC_ID"
  VPC_ID="$DEFAULT_VPC_ID"
else
  echo "No default VPC found. Available VPCs:"
  echo
  aws ec2 describe-vpcs \
    --query "Vpcs[*].[VpcId,CidrBlock,Tags[?Key=='Name']|[0].Value||'(unnamed)']" \
    --output table \
    --region "$AWS_REGION"
  echo
  read -rp "Enter VPC ID: " VPC_ID
  if [ -z "$VPC_ID" ]; then
    echo "Error: VPC ID is required."
    exit 1
  fi
fi

# Get subnets in the VPC
SUBNET_JSON=$(aws ec2 describe-subnets \
  --filters "Name=vpc-id,Values=$VPC_ID" \
  --query "Subnets[*].[SubnetId,AvailabilityZone,CidrBlock]" \
  --output json \
  --region "$AWS_REGION")

SUBNET_COUNT=$(echo "$SUBNET_JSON" | python3 -c "import json,sys; print(len(json.load(sys.stdin)))")

if [ "$SUBNET_COUNT" -lt 2 ]; then
  echo "Error: At least 2 subnets are required (for RDS). Found $SUBNET_COUNT in VPC $VPC_ID."
  exit 1
elif [ "$SUBNET_COUNT" -eq 2 ]; then
  SUBNET_IDS=$(echo "$SUBNET_JSON" | python3 -c "
import json, sys
subnets = json.load(sys.stdin)
print(','.join(s[0] for s in subnets))")
  echo "Using both subnets: $SUBNET_IDS"
else
  echo
  echo "Available subnets in $VPC_ID:"
  echo
  echo "$SUBNET_JSON" | python3 -c "
import json, sys
subnets = json.load(sys.stdin)
for i, s in enumerate(subnets, 1):
    print(f'  {i}. {s[0]}  {s[1]}  {s[2]}')"
  echo
  read -rp "Select 2+ subnets (comma-separated numbers, e.g. 1,2): " SUBNET_SELECTION
  SUBNET_IDS=$(echo "$SUBNET_JSON" | python3 -c "
import json, sys
subnets = json.load(sys.stdin)
indices = [int(x.strip()) - 1 for x in '$SUBNET_SELECTION'.split(',')]
print(','.join(subnets[i][0] for i in indices))")
  echo "Selected subnets: $SUBNET_IDS"
fi

# ──────────────────────────────────────────────
# 4. Salesforce configuration
# ──────────────────────────────────────────────

echo
echo "Salesforce Configuration"
echo "────────────────────────"

read -rp "Salesforce Instance URL (e.g. https://mycompany.my.salesforce.com): " SF_INSTANCE_URL
if [ -z "$SF_INSTANCE_URL" ]; then
  echo "Error: Salesforce Instance URL is required."
  exit 1
fi

read -rp "Salesforce Org ID (18-char, e.g. 00Dxxxxxxxxxxxxxxx): " SF_ORG_ID
if [ -z "$SF_ORG_ID" ]; then
  echo "Error: Salesforce Org ID is required."
  exit 1
fi

read -rp "Salesforce Client ID: " SF_CLIENT_ID
if [ -z "$SF_CLIENT_ID" ]; then
  echo "Error: Salesforce Client ID is required."
  exit 1
fi

read -rsp "Salesforce Client Secret (input hidden): " SF_CLIENT_SECRET
echo
if [ -z "$SF_CLIENT_SECRET" ]; then
  echo "Error: Salesforce Client Secret is required."
  exit 1
fi

# ──────────────────────────────────────────────
# 5. DB password
# ──────────────────────────────────────────────

echo
read -rsp "DB password (leave empty to auto-generate): " DB_PASSWORD
echo
if [ -z "$DB_PASSWORD" ]; then
  DB_PASSWORD=$(openssl rand -hex 16)
  echo "Generated random DB password (32 hex chars)."
fi

# ──────────────────────────────────────────────
# Summary and confirmation
# ──────────────────────────────────────────────

echo
echo "╔════════════════════════════════════════════════════════════╗"
echo "║                    Deployment Summary                     ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo
echo "  Stack name:     $STACK_NAME"
echo "  Region:         $AWS_REGION"
echo "  VPC:            $VPC_ID"
echo "  Subnets:        $SUBNET_IDS"
echo "  SF Instance:    $SF_INSTANCE_URL"
echo "  SF Org ID:      $SF_ORG_ID"
echo
read -rp "Proceed with deployment? [Y/n]: " CONFIRM
CONFIRM="${CONFIRM:-Y}"
if [[ ! "$CONFIRM" =~ ^[Yy]$ ]]; then
  echo "Aborted."
  exit 0
fi

# ──────────────────────────────────────────────
# 6. Create SSM parameters (before stack, so ECS tasks can read them)
# ──────────────────────────────────────────────

echo
echo "Creating SSM parameters..."
AWS_REGION="$AWS_REGION" bash "$SCRIPT_DIR/set-secrets.sh" "$STACK_NAME" "$DB_PASSWORD" "$SF_CLIENT_SECRET" --skip-rds

# ──────────────────────────────────────────────
# 7. Deploy CloudFormation stack
# ──────────────────────────────────────────────

echo
echo "Creating CloudFormation stack '$STACK_NAME'..."

# Escape commas for CloudFormation ParameterValue
SUBNET_PARAM=$(echo "$SUBNET_IDS" | sed 's/,/\\,/g')

aws cloudformation create-stack \
  --stack-name "$STACK_NAME" \
  --template-body file://"$SCRIPT_DIR"/cloudformation/heimdall-stack.yml \
  --capabilities CAPABILITY_NAMED_IAM \
  --region "$AWS_REGION" \
  --parameters \
    ParameterKey=VpcId,ParameterValue="$VPC_ID" \
    ParameterKey=SubnetIds,ParameterValue="$SUBNET_PARAM" \
    ParameterKey=SalesforceInstanceUrl,ParameterValue="$SF_INSTANCE_URL" \
    ParameterKey=SalesforceOrgId,ParameterValue="$SF_ORG_ID" \
    ParameterKey=SalesforceClientId,ParameterValue="$SF_CLIENT_ID" \
    ParameterKey=DBPassword,ParameterValue="$DB_PASSWORD"

# ──────────────────────────────────────────────
# 8. Wait for stack creation
# ──────────────────────────────────────────────

echo "Waiting for stack creation to complete (this takes 10-15 minutes)..."

# Show progress dots while waiting
aws cloudformation wait stack-create-complete \
  --stack-name "$STACK_NAME" \
  --region "$AWS_REGION" &
WAIT_PID=$!

while kill -0 $WAIT_PID 2>/dev/null; do
  printf "."
  sleep 10
done
echo

wait $WAIT_PID
WAIT_EXIT=$?

if [ $WAIT_EXIT -ne 0 ]; then
  echo
  echo "Stack creation failed! Recent events:"
  echo
  aws cloudformation describe-stack-events \
    --stack-name "$STACK_NAME" \
    --region "$AWS_REGION" \
    --query "StackEvents[?ResourceStatus=='CREATE_FAILED'].[LogicalResourceId,ResourceStatusReason]" \
    --output table
  exit 1
fi

# ──────────────────────────────────────────────
# 9. Stop RDS to save costs
# ──────────────────────────────────────────────

echo "Stopping RDS instance to save costs..."
aws rds stop-db-instance \
  --db-instance-identifier "${STACK_NAME}-db" \
  --region "$AWS_REGION" >/dev/null 2>&1 || true
echo "RDS stop initiated (takes 5-10 minutes in background)."

# ──────────────────────────────────────────────
# 10. Summary
# ──────────────────────────────────────────────

ECR_REPO="${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${STACK_NAME}-repository"

echo
echo "╔════════════════════════════════════════════════════════════╗"
echo "║              Stack created successfully!                  ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo
echo "  S3 Bucket:  ${STACK_NAME}-backup-${ACCOUNT_ID}"
echo "  ECR Repo:   ${ECR_REPO}"
echo "  RDS:        ${STACK_NAME}-db (stopping — will auto-start on first backup)"
echo
echo "Next step:"
echo "  ./build-and-push.sh ${STACK_NAME} ${ACCOUNT_ID}"
echo
