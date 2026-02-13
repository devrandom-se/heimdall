# Heimdall

**Open-source Salesforce backup with version history, archive, and point-in-time restore.**

Heimdall backs up your entire Salesforce org to PostgreSQL and S3 on a schedule, giving you:
- Complete version history for every record
- Deleted records recovery
- Record archiving with optional Salesforce cleanup
- ContentVersion file backup with deduplication
- Web GUI for browsing, searching, and restoring data

## Architecture

```
                          ┌──────────────────────────────────────┐
                          │            Heimdall                  │
                          │                                      │
┌──────────────┐   Bulk   │  ┌────────────┐    ┌─────────────┐  │
│              │   API    │  │   Batch    │    │  Restore    │  │
│  Salesforce  │◄────────►│  │   Mode     │    │  GUI        │  │
│              │          │  │ (scheduled)│    │ (web mode)  │  │
└──────────────┘          │  └─────┬──────┘    └──────┬──────┘  │
                          │        │                   │         │
                          └────────┼───────────────────┼─────────┘
                                   │                   │
                          ┌────────▼───────┐  ┌───────▼────────┐
                          │   PostgreSQL   │  │     AWS S3     │
                          │  (metadata,    │  │  (CSV files,   │
                          │   versions,    │  │   ContentVersion│
                          │   checkpoints) │  │   binaries)    │
                          └────────────────┘  └────────────────┘
```

**Batch Mode** runs as a scheduled ECS Fargate task (or k8s cron job, or standalone). It queries every object in your org via the Bulk API, stores record metadata in PostgreSQL, and uploads full CSV data to S3. ContentVersion files are deduplicated by checksum.

**Web Mode** provides a GUI for browsing backup history, viewing diffs between record versions, recovering deleted records, and downloading files. Uses Salesforce Lightning Design System (SLDS).

**RDS Lifecycle Management** (optional): When running on AWS with an RDS database, Heimdall automatically starts the database before use and stops it after — reducing costs from ~$50/month to a few dollars.

**API Limit Protection** (optional): Monitors Salesforce org API usage and stops backup gracefully when a configurable percentage of the daily limit is reached. Safe across restarts — uses an absolute threshold against the org's actual usage, not a per-run budget. Set `HEIMDALL_API_LIMIT_STOP_AT_PERCENT=50` to stop when the org hits 50% of its daily API limit.

## Features

- **Incremental backups** with checkpoint resume (survives crashes, OOM kills, restarts)
- **ContentVersion deduplication** via content-addressable storage (checksum-based)
- **Dynamic batch sizing** that adapts to API response times (50K-200K records)
- **Record archiving** with configurable age threshold and SOQL expression filters
- **Version history** across monthly backup periods
- **Deleted records browser** with restore capability
- **Field-by-field diff** between any two record versions
- **Related records and files** shown on the unified record page
- **API limit protection** — stops gracefully before exceeding configurable daily API usage threshold
- **S3-compatible storage** (AWS S3, MinIO, Backblaze B2, etc.)
- **Salesforce metadata auto-migration** (creates backup config objects automatically)

## Quick Start

### Prerequisites

- Java 21+
- PostgreSQL 14+
- AWS S3 bucket (or S3-compatible storage)
- Salesforce Connected App with API access

### 1. Clone and Configure

```bash
git clone https://github.com/devrandom-se/heimdall.git
cd heimdall

# Copy and edit configuration
cp .env.example .env
vim .env
```

### 2. Set Up PostgreSQL

```bash
# Create database and user
psql -U postgres -c "CREATE USER heimdall WITH PASSWORD 'your_password';"
psql -U postgres -c "CREATE DATABASE heimdall OWNER heimdall;"
```

Schema is created automatically on first run.

### 3. Run Backup

```bash
export $(cat .env | xargs)
./mvnw spring-boot:run
```

### 4. Run Restore GUI

```bash
./mvnw spring-boot:run -Dspring-boot.run.profiles=web
# Open http://localhost:8080
```

Or use the helper script:
```bash
./start-web.sh
```

## AWS Deployment

### 1. Deploy Salesforce Metadata

Install the Heimdall custom object and External Client App into your Salesforce org:

```bash
cd sf-metadata
sf org login web --alias my-org --set-default
./deploy.sh admin@example.com backup.user@example.com my-org
```

This creates:
- `Heimdall_Backup_Config__c` custom object (backup configuration per object)
- External Client App with OAuth client credentials flow

After deploy, go to **Setup > External Client Apps > Heimdall Backup** and note the **Consumer Key** and **Consumer Secret**.

### 2. Create CloudFormation Stack

The stack name is used as prefix for all resources (e.g. `heimdall` creates `heimdall-db`, `heimdall-cluster`, etc.). Deploy one stack per Salesforce org.

```bash
aws cloudformation create-stack \
  --stack-name heimdall \
  --template-body file://cloudformation/heimdall-stack.yml \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameters \
    ParameterKey=VpcId,ParameterValue=vpc-xxx \
    ParameterKey=SubnetIds,ParameterValue="subnet-aaa\,subnet-bbb" \
    ParameterKey=SalesforceInstanceUrl,ParameterValue=https://mycompany.my.salesforce.com \
    ParameterKey=SalesforceOrgId,ParameterValue=00Dxxxxxxxxxxxxxxx \
    ParameterKey=SalesforceClientId,ParameterValue=3MVG9... \
    ParameterKey=DBPassword,ParameterValue=your-db-password
```

This creates: S3 bucket, RDS PostgreSQL (db.t4g.medium), ECS cluster, ECR repository, EventBridge schedule, and SSM parameters.

> **Tip:** Use the default VPC's public subnets (minimum 2 for RDS). No NAT gateway or VPC endpoints needed — ECS tasks get public IPs for outbound traffic, and RDS is not publicly accessible.

### 3. Set Secrets

The stack creates SSM parameters with placeholder values. Update them with real credentials as SecureString:

```bash
./set-secrets.sh heimdall "your-db-password" "sf-consumer-secret-from-step-1"
```

### 4. Build and Push Docker Image

```bash
./build-and-push.sh
```

### 5. Stop RDS Until First Backup

The database is only needed during backups and GUI sessions. Stop it to save costs until the first scheduled backup starts it automatically:

```bash
aws rds stop-db-instance --db-instance-identifier heimdall-db
```

The backup schedule (default: daily at 18:00 UTC) will auto-start the database, run the backup, and stop it again.

### What it Costs

The stack is designed for minimal cost:
- **S3**: Pennies per GB/month (auto-transitions to Infrequent Access after 30 days)
- **RDS**: On-demand only — auto-starts for backups/GUI, auto-stops when idle (~$3-5/month)
- **ECS Fargate**: Pay per backup run
- **Typical total**: $5-15/month for a mid-size org

### Helper Scripts

| Script | Purpose |
|--------|---------|
| `set-secrets.sh` | Store DB password and SF client secret in SSM as SecureString |
| `sf-metadata/deploy.sh` | Deploy Salesforce metadata (custom object + External Client App) |
| `build-and-push.sh` | Build Docker image and push to ECR |
| `db-tunnel.sh` | SSM tunnel to RDS via bastion (auto-starts both) |
| `toggle-gui.sh` | Enable/disable GUI resources in CloudFormation |
| `start-web.sh` | Run GUI locally (requires tunnel) |

## Configuration

All configuration is via environment variables:

### Required

| Variable | Description |
|----------|-------------|
| `SALESFORCE_BASE_URL` | Salesforce instance URL |
| `SALESFORCE_ORG_ID` | 18-character Organization ID |
| `SALESFORCE_CLIENT_ID` | Connected App Client ID |
| `AWS_S3_BUCKET_NAME` | S3 bucket for backups |
| `POSTGRES_PASSWORD` | Database password |

### Authentication

**Client Credentials (recommended):**
```bash
SALESFORCE_GRANT_TYPE=client_credentials
SALESFORCE_CLIENT_SECRET=your_secret
```

**JWT Bearer Token:**
```bash
SALESFORCE_GRANT_TYPE=urn:ietf:params:oauth:grant-type:jwt-bearer
SALESFORCE_USERNAME=backup.user@yourcompany.com
SALESFORCE_JWT_KEY_FILE=/path/to/private.key
```

### Optional

| Variable | Default | Description |
|----------|---------|-------------|
| `SALESFORCE_API_VERSION` | `v61.0` | Salesforce API version |
| `AWS_S3_REGION` | `eu-north-1` | AWS region |
| `POSTGRES_URL` | `jdbc:postgresql://localhost:5432/heimdall` | JDBC URL |
| `POSTGRES_USERNAME` | `heimdall` | DB username |
| `HEIMDALL_SKIP_FILES_BELOW_KB` | `0` | Skip small ContentVersion files |
| `HEIMDALL_METADATA_AUTO_MIGRATE` | `true` | Auto-create SF metadata objects |
| `RDS_INSTANCE_IDENTIFIER` | _(empty)_ | RDS instance for auto-start/stop |
| `HEIMDALL_API_LIMIT_STOP_AT_PERCENT` | _(empty)_ | Stop backup at N% of daily API limit |

## Docker

```bash
# Build
docker build -t heimdall .

# Run backup
docker run --env-file .env heimdall

# Run GUI
docker run --env-file .env -e SPRING_PROFILES_ACTIVE=web -p 8080:8080 heimdall
```

## Storage Layout

```
OrgId={org_id}/
├── Period={YYMM}/
│   └── Year={YYYY}/Month={MM}/Date={DD}/
│       ├── Account/
│       │   └── Account_20260115_143022.csv
│       ├── Account_deleted/
│       │   └── Account_deleted_20260115_143030.csv
│       └── ...
└── Period=0/
    └── files/
        ├── a3b2/
        │   └── a3b2c4d5e6f7890123456789abcdef01.pdf
        └── b896/
            └── b89618dc1c6772fa6c8b4a37d4214e80.jpg
```

- **Period=YYMM**: Monthly partitions for record CSV data
- **Period=0/files/**: Content-addressable file storage (deduplicated by checksum)

## Roadmap

See [ROADMAP.md](ROADMAP.md) for planned features including pluggable storage backends, retention policies, Salesforce OAuth login, and LWC integration.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and guidelines.

Test coverage is minimal — adding tests is a great way to get familiar with the codebase and make a meaningful first contribution.

## License

**GNU Affero General Public License v3.0 (AGPL-3.0)**

You can use, modify, and distribute this software freely. If you run a modified version as a network service, you must release your changes under the same license.

See [LICENSE](LICENSE) for full terms.

Copyright (c) 2025-2026 Johan Karlsteen
