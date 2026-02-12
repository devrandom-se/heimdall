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
TARGET_ORG="${3:+--target-org $3}"

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
sf project deploy start --source-dir "$SCRIPT_DIR/force-app" $TARGET_ORG

echo ""
echo "Done! Next steps:"
echo "  1. Go to Setup > External Client Apps > Heimdall Backup"
echo "  2. Note the Consumer Key and Consumer Secret"
echo "  3. Run set-secrets.sh to store the client secret in AWS SSM"
