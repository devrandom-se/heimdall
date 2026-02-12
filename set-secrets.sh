#!/usr/bin/env bash
set -euo pipefail

# --- Usage ---
# Update Heimdall secrets in SSM Parameter Store as SecureString.
# Run this after stack creation to replace the CHANGE_ME placeholders.
#
# ./set-secrets.sh <stack-prefix> <db-password> <salesforce-client-secret>
#
# Example:
#   ./set-secrets.sh heimdall "MyDbP@ssword1234" "3MVG9..."
# ----------------

if [ $# -lt 3 ]; then
  echo "Error: All arguments are required."
  echo "Usage: $0 <stack-prefix> <db-password> <salesforce-client-secret>"
  exit 1
fi

STACK_PREFIX="$1"
DB_PASSWORD="$2"
SF_CLIENT_SECRET="$3"

AWS_REGION="${AWS_REGION:-eu-north-1}"

echo "Stack prefix: $STACK_PREFIX"
echo "Region:       $AWS_REGION"
echo

aws ssm put-parameter \
  --name "/${STACK_PREFIX}/rds/password" \
  --type SecureString \
  --value "${DB_PASSWORD}" \
  --key-id alias/aws/ssm \
  --region "${AWS_REGION}" \
  --overwrite

aws ssm put-parameter \
  --name "/${STACK_PREFIX}/salesforce/client-secret" \
  --type SecureString \
  --value "${SF_CLIENT_SECRET}" \
  --key-id alias/aws/ssm \
  --region "${AWS_REGION}" \
  --overwrite

echo
echo "Done! Parameters updated as SecureString:"
echo "  /${STACK_PREFIX}/rds/password"
echo "  /${STACK_PREFIX}/salesforce/client-secret"
echo
echo "Note: If the RDS instance was already created with the placeholder password,"
echo "you also need to update the RDS master password:"
echo "  aws rds modify-db-instance \\"
echo "    --db-instance-identifier ${STACK_PREFIX}-db \\"
echo "    --master-user-password \"${DB_PASSWORD}\" \\"
echo "    --region ${AWS_REGION}"
