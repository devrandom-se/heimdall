# Contributing to Heimdall

Thanks for your interest in contributing! Heimdall is an open-source Salesforce backup solution and we welcome contributions of all kinds.

## Getting Started

### Prerequisites

- Java 21+
- Maven 3.9+ (or use the included `./mvnw` wrapper)
- PostgreSQL 14+ (local or remote)
- An S3 bucket or S3-compatible storage (MinIO works great for local dev)
- A Salesforce org with a Connected App

### Development Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/devrandom-ab/heimdall.git
   cd heimdall
   ```

2. **Set up environment**
   ```bash
   cp .env.example .env
   # Edit .env with your Salesforce and AWS credentials
   ```

3. **Set up PostgreSQL**
   ```bash
   psql -U postgres -c "CREATE USER heimdall WITH PASSWORD 'dev_password';"
   psql -U postgres -c "CREATE DATABASE heimdall OWNER heimdall;"
   ```

4. **Build**
   ```bash
   ./mvnw clean compile
   ```

5. **Run backup mode**
   ```bash
   export $(cat .env | xargs)
   ./mvnw spring-boot:run
   ```

6. **Run web mode**
   ```bash
   ./mvnw spring-boot:run -Dspring-boot.run.profiles=web
   # Open http://localhost:8080
   ```

## Project Structure

```
src/main/java/se/devrandom/heimdall/
├── HeimdallApplication.java          # Entry point
├── batchprocessing/                   # Spring Batch job config and processors
│   ├── BatchConfiguration.java        # Job and step definitions
│   ├── ObjectBackupProcessor.java     # Main backup logic per object
│   └── JobCompletionNotificationListener.java
├── config/                            # Spring configuration
│   ├── SalesforceCredentials.java     # OAuth config properties
│   ├── SecurityConfig.java            # Web security
│   └── WebClientConfig.java           # HTTP client for SF API
├── salesforce/                        # Salesforce API integration
│   ├── SalesforceService.java         # Bulk API, SOQL, OAuth
│   ├── SalesforceSchemaManager.java   # Auto-create SF metadata
│   └── objects/                       # Salesforce object models
├── storage/                           # Data storage layer
│   ├── PostgresService.java           # Record metadata and checkpoints
│   ├── S3Service.java                 # CSV and file storage
│   ├── RdsLifecycleService.java       # RDS auto-start/stop
│   └── BackupStatisticsService.java   # Job statistics
├── web/                               # Restore GUI
│   ├── WebController.java             # HTTP endpoints
│   └── RestoreService.java            # Query and restore logic
└── util/
    └── RetryUtil.java                 # Retry with backoff
```

### Key Concepts

- **Profiles**: `backup` (default) runs the batch job and exits. `web` runs the GUI server.
- **Periods**: Backups are grouped by month as YYMM (e.g., 2602 = February 2026). Archive periods are negative (-2602).
- **Checkpoints**: Each object tracks progress via `backup_runs` table, enabling resume after crashes.
- **ContentVersion dedup**: Files are stored by checksum — same file attached to 1000 records is stored once.

## How to Contribute

### Reporting Issues

- Use GitHub Issues for bug reports and feature requests
- Include relevant log output, Salesforce edition, and org size when reporting bugs
- Check existing issues before creating duplicates

### Pull Requests

1. Fork the repository
2. Create a feature branch from `main`
3. Make your changes
4. Ensure `./mvnw clean compile` passes
5. Submit a PR with a clear description of what and why

### Areas Where Help is Wanted

- **Storage backends**: Azure Blob, Google Cloud Storage, local filesystem
- **Testing**: Unit tests, integration tests
- **Documentation**: Setup guides, tutorials, translations
- **Salesforce LWC**: In-app component for viewing backup data
- **Helm chart**: Kubernetes deployment manifests
- **Docker Compose**: Local dev environment with MinIO

## Code Style

- Standard Java conventions
- No Lombok (keep it simple)
- Prefer explicit over magic (no excessive annotations)
- Log meaningful messages at appropriate levels (INFO for milestones, DEBUG for details)
- Handle errors gracefully — a single object failure should not stop the entire backup

## License

By contributing, you agree that your contributions will be licensed under the AGPL-3.0 license.
