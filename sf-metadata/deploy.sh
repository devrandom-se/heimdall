#!/usr/bin/env bash
set -euo pipefail

# --- Usage ---
# Deploy Heimdall Salesforce metadata to a target org.
# Copies templates to force-app/, injects org-specific values, then deploys.
#
# ./deploy.sh <admin-email> <execution-user> [target-org]
#
# Example:
#   ./deploy.sh admin@example.com backup.user@example.com my-org-alias
# ----------------

# Check prerequisites
for cmd in sf jq sed; do
  if ! command -v "$cmd" &> /dev/null; then
    echo "Error: $cmd is required but not installed."
    exit 1
  fi
done

if [ $# -lt 2 ]; then
  echo "Usage: $0 <admin-email> <execution-user> [target-org]"
  echo ""
  echo "  admin-email     Contact email for the External Client App"
  echo "  execution-user  Salesforce username for client credentials flow"
  echo "  target-org      sf CLI org alias (optional, uses default org if omitted)"
  exit 1
fi

ADMIN_EMAIL="$1"
EXECUTION_USER="$2"
TARGET_ORG=()
if [ -n "${3:-}" ]; then
  TARGET_ORG=(--target-org "$3")
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "Admin email:    $ADMIN_EMAIL"
echo "Execution user: $EXECUTION_USER"
echo ""

# Copy templates to force-app/
echo "Generating force-app/ from templates..."
rm -rf "$SCRIPT_DIR/force-app"
cp -r "$SCRIPT_DIR/template" "$SCRIPT_DIR/force-app"

# Inject org-specific values
sed -i.bak "s|<contactEmail>.*</contactEmail>|<contactEmail>${ADMIN_EMAIL}</contactEmail>|" \
  "$SCRIPT_DIR/force-app/main/default/externalClientApps/Heimdall_Backup.eca-meta.xml"

sed -i.bak "s|<clientCredentialsFlowUser>.*</clientCredentialsFlowUser>|<clientCredentialsFlowUser>${EXECUTION_USER}</clientCredentialsFlowUser>|" \
  "$SCRIPT_DIR/force-app/main/default/extlClntAppOauthPolicies/Heimdall_Backup_oauthPlcy.ecaOauthPlcy-meta.xml"

# Clean up sed backup files
find "$SCRIPT_DIR/force-app" -name "*.bak" -delete

echo "Deploying metadata..."
sf project deploy start --source-dir "$SCRIPT_DIR/force-app" "${TARGET_ORG[@]}" || true

echo ""
echo "Deploy complete!"
echo ""

# Assign permission set to execution user
assign_permset() {
  local USERNAME="$1"
  local USER_ID
  USER_ID=$(sf data query --query "SELECT Id FROM User WHERE Username = '${USERNAME}' LIMIT 1" "${TARGET_ORG[@]}" --json 2>/dev/null | jq -r '.result.records[0].Id // empty')
  if [ -z "$USER_ID" ]; then
    echo "  Could not find user: $USERNAME"
    return 1
  fi
  local PS_ID
  PS_ID=$(sf data query --query "SELECT Id FROM PermissionSet WHERE Name = 'Heimdall_Admin' LIMIT 1" "${TARGET_ORG[@]}" --json 2>/dev/null | jq -r '.result.records[0].Id // empty')
  if [ -z "$PS_ID" ]; then
    echo "  Could not find Heimdall_Admin permission set"
    return 1
  fi
  # Check if already assigned
  local EXISTING
  EXISTING=$(sf data query --query "SELECT Id FROM PermissionSetAssignment WHERE AssigneeId = '${USER_ID}' AND PermissionSetId = '${PS_ID}' LIMIT 1" "${TARGET_ORG[@]}" --json 2>/dev/null | jq -r '.result.records[0].Id // empty')
  if [ -n "$EXISTING" ]; then
    echo "  Already assigned to $USERNAME"
    return 0
  fi
  sf data create record -s PermissionSetAssignment -v "AssigneeId='${USER_ID}' PermissionSetId='${PS_ID}'" "${TARGET_ORG[@]}" --json > /dev/null 2>&1
  echo "  Assigned Heimdall Admin to $USERNAME"
}

# Assign to execution user
echo "Assigning Heimdall Admin permission set..."
assign_permset "$EXECUTION_USER"

# Offer to assign to deploying user
DEPLOYING_USER=$(sf org display "${TARGET_ORG[@]}" --json 2>/dev/null | jq -r '.result.username // empty')
if [ -n "$DEPLOYING_USER" ] && [ "$DEPLOYING_USER" != "$EXECUTION_USER" ]; then
  read -p "Assign Heimdall Admin to $DEPLOYING_USER too? [Y/n] " -n 1 -r
  echo
  if [[ ! $REPLY =~ ^[Nn]$ ]]; then
    assign_permset "$DEPLOYING_USER"
  fi
fi

ORG_ID=$(sf org display "${TARGET_ORG[@]}" --json 2>/dev/null | jq -r '.result.id // empty')
INSTANCE_URL=$(sf org display "${TARGET_ORG[@]}" --json 2>/dev/null | jq -r '.result.instanceUrl // empty')

echo ""
echo "Org ID:       $ORG_ID"
echo "Instance URL: $INSTANCE_URL"
echo ""
echo "Next steps:"
echo "  1. Go to Setup > External Client Apps > Heimdall Backup"
echo "  2. Note the Consumer Key and Consumer Secret"
echo "  3. Run ./deploy-cloudformation.sh to deploy AWS infrastructure"
