#!/bin/bash
# Start Heimdall in web mode (restore/browse GUI)
#
# Prerequisites:
#   1. Set environment variables (or create .env file - see below)
#   2. Database must be reachable from your machine. Options:
#      a) SSH tunnel:  ssh -L 5432:heimdall-db.xxx.rds.amazonaws.com:5432 bastion-host
#      b) SSM tunnel:  aws ssm start-session --target <ec2-id> \
#                         --document-name AWS-StartPortForwardingSessionToRemoteHost \
#                         --parameters '{"host":["heimdall-db.xxx.rds.amazonaws.com"],"portNumber":["5432"],"localPortNumber":["5432"]}'
#      c) Run as ECS service with web profile instead of local

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Load .env if it exists
if [ -f "$SCRIPT_DIR/.env" ]; then
    echo "Loading environment from .env"
    set -a
    source "$SCRIPT_DIR/.env"
    set +a
fi

# Required variables check
REQUIRED_VARS=(
    SALESFORCE_BASE_URL
    SALESFORCE_CLIENT_ID
    SALESFORCE_ORG_ID
    AWS_S3_BUCKET_NAME
    POSTGRES_PASSWORD
)

MISSING=()
for var in "${REQUIRED_VARS[@]}"; do
    if [ -z "${!var}" ]; then
        MISSING+=("$var")
    fi
done

if [ ${#MISSING[@]} -gt 0 ]; then
    echo "ERROR: Missing required environment variables:"
    for var in "${MISSING[@]}"; do
        echo "  - $var"
    done
    echo ""
    echo "Create a .env file in the project root:"
    echo ""
    echo "  SALESFORCE_BASE_URL=https://your-instance.my.salesforce.com"
    echo "  SALESFORCE_CLIENT_ID=your-client-id"
    echo "  SALESFORCE_CLIENT_SECRET=your-client-secret"
    echo "  SALESFORCE_ORG_ID=00Dxxxxxxxxxx"
    echo "  AWS_S3_BUCKET_NAME=your-bucket-name"
    echo "  POSTGRES_URL=jdbc:postgresql://localhost:5432/heimdall"
    echo "  POSTGRES_USERNAME=heimdall"
    echo "  POSTGRES_PASSWORD=your-password"
    exit 1
fi

# Default to localhost (assumes tunnel is running)
export POSTGRES_URL="${POSTGRES_URL:-jdbc:postgresql://localhost:5432/heimdall}"
export POSTGRES_USERNAME="${POSTGRES_USERNAME:-heimdall}"

echo "Starting Heimdall Web UI..."
echo "  Database: $POSTGRES_URL"
echo "  Org ID:   $SALESFORCE_ORG_ID"
echo ""
echo "  Open http://localhost:8080 in your browser"
echo ""

cd "$SCRIPT_DIR"
./mvnw spring-boot:run -Dspring-boot.run.profiles=web
