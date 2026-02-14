#!/bin/bash
# Start an SSM port-forwarding tunnel to the RDS database via the bastion host.
# The bastion instance and RDS are started automatically if stopped.
#
# Usage: ./db-tunnel.sh [stack-name] [-p profile] [shutdown]
# Example: ./db-tunnel.sh heimdall
# Example: ./db-tunnel.sh heimdall -p my-account
# Example: ./db-tunnel.sh heimdall -p my-account shutdown

set -e

STACK_NAME="${1:-heimdall}"
shift || true

# Parse optional flags
AWS_PROFILE_ARG=""
ACTION=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    -p|--profile)
      export AWS_PROFILE="$2"
      AWS_PROFILE_ARG="--profile $2"
      shift 2
      ;;
    *)
      ACTION="$1"
      shift
      ;;
  esac
done

AWS_REGION="${AWS_REGION:-eu-north-1}"

echo "Fetching stack outputs from ${STACK_NAME}..."

OUTPUTS=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --region "$AWS_REGION" \
  --query "Stacks[0].Outputs" \
  --output json)

INSTANCE_ID=$(echo "$OUTPUTS" | python3 -c "
import json, sys
outputs = json.load(sys.stdin)
for o in outputs:
    if o['OutputKey'] == 'BastionInstanceId':
        print(o['OutputValue'])
        break
")

RDS_ENDPOINT=$(echo "$OUTPUTS" | python3 -c "
import json, sys
outputs = json.load(sys.stdin)
for o in outputs:
    if o['OutputKey'] == 'RDSEndpoint':
        print(o['OutputValue'].split(':')[0])
        break
")

if [ -z "$INSTANCE_ID" ] || [ -z "$RDS_ENDPOINT" ]; then
  echo "Failed to get BastionInstanceId or RDSEndpoint from stack outputs."
  exit 1
fi

# Extract RDS instance identifier from the endpoint (hostname = <identifier>.xxx.region.rds.amazonaws.com)
RDS_INSTANCE_ID=$(echo "$RDS_ENDPOINT" | cut -d. -f1)

echo "Bastion:  $INSTANCE_ID"
echo "RDS:      $RDS_ENDPOINT"
echo "RDS ID:   $RDS_INSTANCE_ID"

# Handle shutdown command
if [ "$ACTION" = "shutdown" ]; then
  echo "Stopping bastion instance..."
  aws ec2 stop-instances --instance-ids "$INSTANCE_ID" --region "$AWS_REGION" > /dev/null
  echo "Bastion is stopping."

  echo "Stopping RDS instance..."
  RDS_STATUS=$(aws rds describe-db-instances \
    --db-instance-identifier "$RDS_INSTANCE_ID" \
    --region "$AWS_REGION" \
    --query "DBInstances[0].DBInstanceStatus" \
    --output text 2>/dev/null || echo "unknown")

  if [ "$RDS_STATUS" = "available" ]; then
    aws rds stop-db-instance --db-instance-identifier "$RDS_INSTANCE_ID" --region "$AWS_REGION" > /dev/null
    echo "RDS is stopping."
  else
    echo "RDS is in status '$RDS_STATUS', skipping stop."
  fi
  exit 0
fi

# Start RDS and bastion in parallel if needed
RDS_NEEDS_WAIT=false
BASTION_NEEDS_WAIT=false

# Check RDS status
RDS_STATUS=$(aws rds describe-db-instances \
  --db-instance-identifier "$RDS_INSTANCE_ID" \
  --region "$AWS_REGION" \
  --query "DBInstances[0].DBInstanceStatus" \
  --output text)

if [ "$RDS_STATUS" = "stopped" ]; then
  echo "RDS is stopped, starting it..."
  aws rds start-db-instance --db-instance-identifier "$RDS_INSTANCE_ID" --region "$AWS_REGION" > /dev/null
  RDS_NEEDS_WAIT=true
elif [ "$RDS_STATUS" = "stopping" ]; then
  echo "RDS is stopping, waiting for it to stop before starting..."
  aws rds wait db-instance-stopped --db-instance-identifier "$RDS_INSTANCE_ID" --region "$AWS_REGION"
  echo "RDS stopped, now starting it..."
  aws rds start-db-instance --db-instance-identifier "$RDS_INSTANCE_ID" --region "$AWS_REGION" > /dev/null
  RDS_NEEDS_WAIT=true
elif [ "$RDS_STATUS" = "available" ]; then
  echo "RDS is already available."
else
  echo "RDS is in status: $RDS_STATUS — will wait for available..."
  RDS_NEEDS_WAIT=true
fi

# Check bastion status
STATE=$(aws ec2 describe-instances \
  --instance-ids "$INSTANCE_ID" \
  --region "$AWS_REGION" \
  --query "Reservations[0].Instances[0].State.Name" \
  --output text)

if [ "$STATE" = "stopped" ]; then
  echo "Bastion is stopped, starting it..."
  aws ec2 start-instances --instance-ids "$INSTANCE_ID" --region "$AWS_REGION" > /dev/null
  BASTION_NEEDS_WAIT=true
elif [ "$STATE" = "running" ]; then
  echo "Bastion is already running."
else
  echo "Bastion is in state: $STATE — will wait..."
  BASTION_NEEDS_WAIT=true
fi

# Wait for both in parallel
if [ "$RDS_NEEDS_WAIT" = true ]; then
  echo "Waiting for RDS to become available..."
  aws rds wait db-instance-available \
    --db-instance-identifier "$RDS_INSTANCE_ID" \
    --region "$AWS_REGION" &
  RDS_WAIT_PID=$!
fi

if [ "$BASTION_NEEDS_WAIT" = true ]; then
  echo "Waiting for bastion to be running..."
  aws ec2 wait instance-running --instance-ids "$INSTANCE_ID" --region "$AWS_REGION" &
  BASTION_WAIT_PID=$!
fi

if [ "$RDS_NEEDS_WAIT" = true ]; then
  wait $RDS_WAIT_PID
  echo "RDS is available."
fi

if [ "$BASTION_NEEDS_WAIT" = true ]; then
  wait $BASTION_WAIT_PID
  echo "Bastion is running."
  echo "Waiting for SSM agent to register (30s)..."
  sleep 30
fi

echo ""
echo "Starting tunnel: localhost:5432 -> $RDS_ENDPOINT:5432"
echo "Press Ctrl+C to stop."
echo ""

aws ssm start-session \
  --target "$INSTANCE_ID" \
  --region "$AWS_REGION" \
  --document-name AWS-StartPortForwardingSessionToRemoteHost \
  --parameters "{\"host\":[\"$RDS_ENDPOINT\"],\"portNumber\":[\"5432\"],\"localPortNumber\":[\"5432\"]}"
