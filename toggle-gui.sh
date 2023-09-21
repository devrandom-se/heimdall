#!/usr/bin/env bash
set -euo pipefail

STACK_NAME="${1:-heimdall}"
AWS_REGION="${AWS_REGION:-eu-north-1}"

echo "Looking up stack: $STACK_NAME (region: $AWS_REGION)"

# Get current EnableGui value
CURRENT=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" --region "$AWS_REGION" \
  --query "Stacks[0].Parameters[?ParameterKey=='EnableGui'].ParameterValue" \
  --output text)

if [ "$CURRENT" = "true" ]; then
  NEW="false"
else
  NEW="true"
fi

echo "EnableGui: $CURRENT -> $NEW"

# Build parameter overrides: flip EnableGui, keep everything else
PARAMS=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" --region "$AWS_REGION" \
  --query "Stacks[0].Parameters" \
  --output json \
  | jq -c '[.[] | if .ParameterKey == "EnableGui" then {ParameterKey: .ParameterKey, ParameterValue: "'"$NEW"'"} else {ParameterKey: .ParameterKey, UsePreviousValue: true} end]')

echo "Updating stack..."
aws cloudformation update-stack \
  --stack-name "$STACK_NAME" --region "$AWS_REGION" \
  --use-previous-template \
  --parameters "$PARAMS" \
  --capabilities CAPABILITY_NAMED_IAM

echo "Waiting for stack update to complete (this can take a few minutes)..."
aws cloudformation wait stack-update-complete --stack-name "$STACK_NAME" --region "$AWS_REGION"
echo "Stack update complete."

if [ "$NEW" = "true" ]; then
  ALB_DNS=$(aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" --region "$AWS_REGION" \
    --query "Stacks[0].Outputs[?OutputKey=='ALBDnsName'].OutputValue" \
    --output text)

  DOMAIN=$(aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" --region "$AWS_REGION" \
    --query "Stacks[0].Parameters[?ParameterKey=='GuiDomainName'].ParameterValue" \
    --output text)

  echo ""
  echo "GUI enabled. Update your DNS:"
  echo "  $DOMAIN  CNAME  $ALB_DNS"
else
  echo ""
  echo "GUI disabled. ALB and Cognito resources removed."
fi
