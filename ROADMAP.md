# Heimdall Roadmap

## Phase 1: Core Backup (Complete)

- [x] PostgreSQL checkpoint tracking
- [x] Pipeline optimization for Bulk API batches
- [x] Dynamic batch sizing (50K-200K adaptive)
- [x] CSV validation with retry logic
- [x] Resilient error handling (per-object fault isolation)
- [x] ContentVersion deduplication by checksum
- [x] Salesforce metadata auto-migration
- [x] RDS on-demand lifecycle management (auto-start/stop)
- [x] CloudFormation stack for full AWS deployment
- [x] Restore GUI with version history, diffs, related records
- [x] Archive feature with configurable age/expression
- [ ] Stale run detection and concurrent instance prevention
- [ ] Blog post: "From Bash Script to Open Source Backup Solution"

### Stale Run Detection (Planned)

Handle long-running jobs, hung jobs, and overlapping runs without manual intervention.

**Stale run detection:** Instead of a fixed time limit, check `last_checkpoint_modstamp` in `backup_runs`. A job making progress (updating checkpoints) is fine regardless of total runtime. A job that hasn't updated its checkpoint in 60 minutes is considered hung.

**At startup:**
1. Check for RUNNING rows in `backup_runs`
2. If checkpoint is fresh (<60 min) → another instance is active, exit gracefully
3. If checkpoint is stale (>60 min) → mark as FAILED, resume from that checkpoint

**Database-level locking:** `SELECT ... FOR UPDATE` on a lock row to prevent two instances from running simultaneously. Simple and reliable.

## Phase 2: Storage & Retention

### Pluggable Storage Backends

Define a `StorageBackend` interface so each object can target a different backend. Ship with S3, make it easy for contributors to add more.

```
StorageBackend
├── S3StorageBackend (included)
├── MinIOStorageBackend (S3-compatible, minimal work)
├── AzureBlobStorageBackend (community)
├── BackblazeB2StorageBackend (community)
├── GoogleCloudStorageBackend (community)
└── LocalFileStorageBackend (dev/testing)
```

Configurable per object in `Heimdall_Backup_Config__c`:
- Default backend for the org
- Override per object (e.g. ContentVersion → cheap storage, Account → primary)

### Automatic Retention & GDPR Compliance

Automatic cleanup of old backup periods after a configurable grace period.

- `heimdall.retention.max-periods: 12` — delete periods older than 12 months
- Remove database records AND storage files for expired periods
- Cascade to ContentVersion: remove files no longer referenced by any active period
- Audit log of what was deleted and when
- Dry-run mode: preview what would be deleted before executing
- Per-object retention override (some objects may need longer retention)

## Phase 3: Restore GUI & OAuth

### OAuth Login from Salesforce

Users authenticate against the restore GUI using their Salesforce org credentials. No separate user management needed.

- OAuth 2.0 Web Server Flow against the connected Salesforce org
- Permission checks: respect Salesforce profile/permission set for data access
- Session management with token refresh
- Embeddable as iframe in a Salesforce LWC

### Restore & Browse GUI

A web interface for browsing and restoring backed-up data. The core concept is a **unified record page** — one URL per record ID that shows everything Heimdall knows about that record regardless of its current status.

**Unified record page** (`/record/{recordId}`):
- Shows the record's current state, version history across backup periods, and any related child records (deleted or archived)
- If the record still exists in Salesforce: shows backup history and diff against current SF state
- If the record has been deleted from Salesforce: shows last known state and full history
- If the record has been archived: shows archive data with the same interface
- Same UI regardless of record status — no separate "archive viewer" or "deleted records" page

**Search and browse:**
- Search by object type, record ID, name, date range
- Restore individual records or bulk restore to Salesforce
- Preview before restore (diff view: backup vs current Salesforce state)
- Download as CSV/Excel for offline use
- ContentVersion file preview and download

## Phase 4: Archive

### Configuration Model

New fields on `Heimdall_Backup_Config__c` per object:

| Field | Type | Description |
|-------|------|-------------|
| `Archive__c` | Checkbox | Enables archiving for this object |
| `Archive_Age_Days__c` | Number | Archive records with CreatedDate older than X days |
| `Archive_Expression__c` | Text | Advanced SOQL filter for fine-grained control |
| `Archive_Action__c` | Picklist | `Archive Only` or `Archive and Delete` |

**Date basis: CreatedDate only.** LastModifiedDate would cause race conditions — a comment added to an old Case would knock it out of the archive window. CreatedDate is immutable and predictable.

**Archive_Expression__c** allows compound filters beyond age, e.g.:
- `RecordType.DeveloperName = 'Standard Case'` — only archive standard cases
- `Status = 'Closed' AND RecordType.DeveloperName = 'Standard Case'` — closed standard cases only

When both `Archive_Age_Days__c` and `Archive_Expression__c` are set, they combine with AND logic: records must be older than the age threshold AND match the expression.

### Archive Job Flow

Archiving is integrated into the backup job and runs after the regular backup completes. Not a separate scheduled job.

**For each object where `Archive__c = true`:**

1. **Identify candidates** — Query Salesforce for records matching the age threshold and optional expression filter
2. **Write to archive period** — Store records in `Period=Archive-YYMM` (where YYMM is when the archiving runs, not when the record was created)
3. **Archive linked ContentVersion** — For archived records, follow ContentDocumentLink to find and archive associated files (see ContentVersion section below)
4. **Delete from Salesforce** (if `Archive_Action__c = 'Archive and Delete'`) — Only after verification (see verification flow below)

### Archive-and-Delete Verification Flow

When `Archive_Action__c = 'Archive and Delete'`, Heimdall must verify the archive is complete and intact before deleting anything from Salesforce:

1. **Write archive data** to storage
2. **Read back and verify** — confirm record count and checksums match
3. **Delete from Salesforce** — bulk delete via Salesforce API
4. **Log deletions** — audit trail of what was deleted, when, and from which archive period

If verification fails, the delete step is skipped and the failure is logged. Records remain in Salesforce until the next successful archive run.

### Deferred Delete for Archive Only

When `Archive_Action__c = 'Archive Only'`, records stay in Salesforce after archiving. For admins who want to eventually clean up Salesforce storage but prefer manual verification first:

- **Archived records list** — GUI provides a filterable list of records that have been archived but still exist in Salesforce, per object and archive period
- **Manual verification** — Admin reviews the archived data in the GUI, confirms it's intact
- **Manual delete** — Admin triggers deletion of verified records from Salesforce via the GUI, either individually or in bulk
- **Audit trail** — Same deletion logging as the automatic flow

### ContentDocumentLink Archiving

**Strategy: Archive ContentDocumentLink records alongside parent objects using a subquery.** After archiving records for an object, Heimdall runs a second Bulk API query for `ContentDocumentLink` using a subquery that reuses the exact same archive criteria as the parent object. This preserves file-to-record relationships in the archive without a separate ID collection step.

Flow:
1. Parent object archive completes (e.g., Case records matching age + expression criteria)
2. Query `ContentDocumentLink WHERE LinkedEntityId IN (SELECT Id FROM Case WHERE <same archive criteria>)` using the Bulk API
3. Store ContentDocumentLink CSV in the same `Archive-YYMM` period — only metadata (no file binary download)
4. Track progress independently via `backup_runs` with `object_name = 'ContentDocumentLink:{ParentObject}'`
5. Checkpoint resets automatically if the parent's archive expression changes

This enables future features: showing related files for archived records, detecting orphaned ContentDocuments, and cascading archive cleanup. The regular ContentVersion backup already handles file binary storage — CDL archiving adds the link metadata.

Future: ContentVersion binary archiving (download files linked only to archived records) and orphan detection (ContentDocuments with no remaining links).

### Archive Storage Model

**Period format: `Archive-YYMM`** — dated by when the archiving was performed, not when the records were created.

- Same storage backends as regular backups (S3, etc.)
- Optional per-object backend override for archives (e.g. S3 Standard for backups, S3 Glacier or a cheaper provider for long-term archives)
- Each archive run produces a new dated period, enabling granular retention
- GDPR-compatible: archive periods can be cleaned up by the retention system just like backup periods
- Separate retention policy for archive periods (e.g. keep archives for 7 years, backups for 12 months)

### Archive in the GUI

Archiving extends the unified record page from Phase 3 — no separate "archive viewer" needed.

- Archived and deleted child records appear naturally on the unified record page alongside backup history
- Visual indicator distinguishes record status: active (backed up), archived, deleted
- Restore from archive back to Salesforce with one click

### Salesforce LWC Integration

An LWC component that brings Heimdall data directly into the Salesforce UI:

- **Record page link** — Drop onto any record page layout. Shows a link to the Heimdall unified record page for that record ID, opening in a new tab or as a subtab
- **Inline iframe for child records** — Embeddable iframe showing deleted and archived related records (e.g. archived EmailMessages on a Case page, archived Cases on an Account page)
- **Metadata-driven visibility** — LWC reads `Heimdall_Backup_Config__c` to auto-hide on object pages where archiving is not configured. Zero admin configuration needed after initial placement
- **OAuth passthrough** — Uses the Phase 3 OAuth flow so users don't need to log in separately

## Phase 5: Growth & Community

### Features for Adoption

- **Multi-org support**: back up multiple Salesforce orgs to the same Heimdall instance
- **Configurable period format**: period is a date string pattern (e.g. `YYMM` for monthly, `YYww` for weekly, `YYQn` for quarterly). The period format implicitly controls backup frequency — a weekly format means a new full backup every week. Inherited from the original bash script design
- **Scheduled backups**: cron-based scheduling to trigger backup runs automatically
- **Email/Slack notifications**: backup status reports, failure alerts
- **Backup health dashboard**: overview of all objects, last successful backup, storage usage
- **Terraform/CloudFormation templates**: one-click deployment to AWS
- **Docker Compose**: easy local setup for evaluation
- **Helm chart**: Kubernetes deployment

### Developer Experience

- Comprehensive API documentation
- Contributing guide with architecture overview
- Storage backend plugin tutorial ("Add a new backend in 100 lines")
- Example LWC package installable from AppExchange or GitHub

### Potential Differentiators vs. Commercial Solutions

- **Open source & free**: no per-user or per-record pricing
- **BYOS (Bring Your Own Storage)**: full control over where data lives
- **Archive with in-Salesforce access**: most competitors require a separate portal
- **GDPR-first retention**: automatic, auditable data lifecycle management
- **Self-hosted**: data never leaves your infrastructure
