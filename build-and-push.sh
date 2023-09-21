#!/bin/bash
set -e

# --- Usage ---
# ./build-and-push.sh <stack-name> <account-id>
# Example:
# ./build-and-push.sh heimdall 242238425743
# ----------------

STACK_NAME="${1:-heimdall}"
ACCOUNT_ID="$2"

if [ -z "$ACCOUNT_ID" ]; then
  echo "❌ Error: Account ID is required."
  echo "Usage: $0 <stack-name> <account-id>"
  echo "Example: $0 heimdall 242238425743"
  exit 1
fi

AWS_REGION="${AWS_REGION:-eu-north-1}"
PROFILE="${STACK_NAME}-ecr-push-user"
REPO="${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${STACK_NAME}-repository"

echo "╔════════════════════════════════════════════════════════════╗"
echo "║           Heimdall - Build and Push to ECR                ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo
echo "📦 Stack name:      $STACK_NAME"
echo "👤 AWS profile:     $PROFILE"
echo "🏦 AWS account ID:  $ACCOUNT_ID"
echo "🌍 AWS region:      $AWS_REGION"
echo "🪣 ECR repository:  $REPO"
echo

# --- Check if AWS profile exists ---
if ! aws configure list-profiles | grep -q "^${PROFILE}$"; then
  echo "🧩 AWS profile '${PROFILE}' not found."
  echo "   Create access keys for the '${STACK_NAME}-ecr-push-user' IAM user in AWS Console,"
  echo "   then run: aws configure --profile ${PROFILE}"
  exit 1
else
  echo "✅ AWS profile '${PROFILE}' found."
fi

# --- ECR Login ---
echo
echo "🔐 Logging in to ECR..."
aws ecr get-login-password --region "${AWS_REGION}" --profile "${PROFILE}" | \
docker login --username AWS --password-stdin "${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

# --- Docker Build ---
echo
echo "🐳 Building Docker image (linux/amd64 for Fargate)..."
docker buildx build --platform linux/amd64 -t "${STACK_NAME}" .

# --- Tag & Push ---
echo
echo "🏷️ Tagging image..."
docker tag "${STACK_NAME}:latest" "${REPO}:latest"

echo
echo "🚀 Pushing image to ECR..."
docker push "${REPO}:latest"

echo
echo "╔════════════════════════════════════════════════════════════╗"
echo "║                    ✅ Done!                                ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo
echo "Image pushed to:"
echo "   ${REPO}:latest"
echo
echo "Next steps:"
echo "   1. Update Salesforce JWT secret in AWS Secrets Manager"
echo "   2. Run backup manually: aws ecs run-task ..."
echo "   3. Or wait for scheduled backup"
echo
