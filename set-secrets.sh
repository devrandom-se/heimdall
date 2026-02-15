#!/usr/bin/env bash
set -euo pipefail

# --- Usage ---
# Update Heimdall secrets in SSM Parameter Store as SecureString.
# Run this after stack creation to replace the CHANGE_ME placeholders.
#
# ./set-secrets.sh <stack-prefix> <db-password> <salesforce-client-secret> [--skip-rds]
#
# Options:
#   --skip-rds    Skip RDS password update (for initial deploy before RDS exists)
#
# Example:
#   ./set-secrets.sh heimdall "MyDbP@ssword1234" "3MVG9..."
# ----------------

if [ $# -lt 3 ]; then
  echo "Error: All arguments are required."
  echo "Usage: $0 <stack-prefix> <db-password> <salesforce-client-secret> [--skip-rds]"
  exit 1
fi

STACK_PREFIX="$1"
DB_PASSWORD="$2"
SF_CLIENT_SECRET="$3"
SKIP_RDS=false

shift 3
while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-rds)
      SKIP_RDS=true
      shift
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

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

# Update RDS master password (unless --skip-rds)
if [ "$SKIP_RDS" = false ]; then
  echo
  DB_INSTANCE="${STACK_PREFIX}-db"
  if aws rds describe-db-instances --db-instance-identifier "$DB_INSTANCE" --region "$AWS_REGION" &>/dev/null; then
    aws rds modify-db-instance \
      --db-instance-identifier "$DB_INSTANCE" \
      --master-user-password "$DB_PASSWORD" \
      --region "$AWS_REGION" >/dev/null
    echo "RDS password updated for $DB_INSTANCE"
  else
    echo "RDS instance $DB_INSTANCE not found, skipping password update."
  fi
fi
