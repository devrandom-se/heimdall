# Heimdall Roadmap

Heimdall backs up Salesforce orgs to storage you own (S3 + PostgreSQL) and lets you browse, diff and restore what it backed up. The goal is the best open-source Salesforce backup: one whose restores you can trust, and whose failures are loud.

The roadmap is organised by theme on three horizons. **Now** is active work, **Next** is committed direction, **Later** is intent. Order is the promise; there are no dates. Design notes for the larger items are at the end.

Last reviewed: 2026-09-02, after a full code review at commit `316c0ad`.

## Shipped

- Bulk API 2.0 backup with checkpoints on (SystemModstamp, Id), adaptive batch sizing, CSV validation against `Sforce-NumberOfRecords`, per-object fault isolation
- ContentVersion file backup with checksum deduplication
- Monthly periods (`YYMM`), full copy per period; S3 CSV per batch plus a PostgreSQL index of every record version
- Archive: Status-driven per-object config, age and expression filters, ContentDocumentLink archiving into `Archive-YYMM` periods
- Retention: delete backup periods older than N months, preview (dry-run) in the GUI, opt-in monthly cleanup in the batch job
- Restore GUI: record page with version history, diffs, related records, deleted-record browsing, field-level restore to a sandbox with validation
- Demo mode with seeded data
- Salesforce metadata auto-migration of `Heimdall_Backup_Config__c`
- RDS on-demand lifecycle (start before the job, stop after)
- One-command AWS deployment: ECS Fargate, RDS, S3, optional GUI behind ALB + Cognito, optional bastion
- Testcontainers integration tests for storage, retention and restore

## Now: reliability and trust

The 2026 storage-full incident showed that Heimdall backs up well but cannot yet prove that it did, and does not tell anyone when it did not. These items close that gap. Everything here is backward compatible with an existing installation unless marked otherwise.

### Never lose data silently

- Fail the object when the S3 upload or the PostgreSQL insert fails, and never advance the checkpoint past a failed batch
- Validate each Bulk API chunk before appending it to the period CSV, so a retried chunk cannot land twice
- Make the step threads safe: immutable date formatting, a concurrent map for config state
- Honest `backup_runs.status`: `FAILED` on exception, `PARTIAL` when stopped by the API limit or a failed batch, `ABANDONED` for stale `RUNNING` rows found at startup (stale-run detection, see design note)
- Exit code 2 for "completed with errors", so the scheduler and the alerting can see partial failure
- Surface HTTP errors from Bulk API result downloads with status and body instead of `null`

### Retention that always runs

- Run the retention step before the backup step, so a failing backup can never block cleanup
- Catch-up gate: clean up when at least a month has passed since the last cleanup, instead of on an exact day of month
- `VACUUM` after retention deletes, inside the job, before RDS is stopped
- Compute the period once per job, so a run across month-end midnight does not split into two periods
- Cover what retention misses today: archive periods (separate, opt-in retention), ContentVersion files no retained row references, S3 objects that have no `csvfiles` row. All opt-in, all with dry-run

### Loud failure

- CloudFormation: SNS topic with an alert e-mail; alarms for a failed task, no successful run in 26 hours, low RDS free storage, and schedule invocation failures
- Publish a run summary (objects backed up, failed, skipped, row counts, free storage) as CloudWatch metrics and to the alert topic
- Stop RDS after every job when an instance identifier is configured, not only when Heimdall started it; a flag keeps it running for installations that share the instance with the GUI

### Deployment hardening

- RDS: storage autoscaling cap and deletion protection; S3 bucket retained on stack deletion; log groups with retention; retry policy on the schedule target
- Immutable image tags (git SHA) with an `ImageTag` stack parameter, so every night's image is known and rollback is one parameter change
- Container: heap sized from the task (`MaxRAMPercentage`), non-root user, `exec` entrypoint so SIGTERM reaches the JVM
- RDS encryption at rest. **Requires a new instance**; bundled with the storage migration under Next

### GUI security

- CSRF protection on, with tokens sent by the fetch calls
- Escape record data correctly in the restore form (DOM APIs, no inline event handlers)
- Validate object names and record IDs at the controller boundary before they reach SOQL
- Refresh the sandbox session on 401 and apply timeouts to the sandbox client
- One demo-mode switch with an explicit guard on every mutating endpoint

### Engineering hygiene

- Commit the Maven wrapper (README's `./mvnw` fails on a fresh clone today)
- GitHub Actions: build, unit tests, Testcontainers integration tests, Docker build, cfn-lint
- Dependabot; a supported Spring Boot line; bump `org.json` (CVE-2023-5072) and the AWS SDK
- Tag releases and keep a CHANGELOG with upgrade notes

## Next: foundations

### Storage model

- Schema migrations with Flyway, baselined on existing installations; one DDL owner for both the batch and the web profile
- Partition `objects` by period, so retention becomes `DROP PARTITION`: no mass DELETE, no VACUUM dependency, and per-partition indexes vanish with their period. Existing rows stay in a legacy partition until retention ages them out. **Migration release**
- Replace the trigram index for related-record lookup with a `refs text[]` column and a GIN array index, populated for new periods only
- Index review: drop the PK-prefix duplicate, add the browsing index `(org_id, object_name, id, period DESC)`, switch to keyset pagination
- Incremental object statistics instead of nightly full scans
- One connection pool shared by batch and web; batched inserts rewritten on the wire

### Prove the backup

- Reconciliation per run: `Sforce-NumberOfRecords` versus rows in the CSV and in `objects`
- Rebuild the PostgreSQL index from S3 (`--reindex`), so S3 alone is a sufficient disaster-recovery source
- "How to verify a backup" and a restore-drill runbook in the docs
- S3 Object Lock and cross-region replication as options

### Code structure

- Split `SalesforceService` into authenticator, REST client, Bulk query client, CSV support, config repository, batch-size tuner and ContentVersion backup
- One `SalesforceAuthenticator` shared by the backup client and the sandbox-restore client
- Persist ContentVersion download failures and retry them at the next run; key the checkpoint on (SystemModstamp, Id)
- Abort unconsumed Bulk API jobs; dedicated executor for the look-ahead query
- Pipeline tests with WireMock (Salesforce) and LocalStack (S3)
- Either a partitioned Spring Batch step (one partition per object) or plain executors

### Restore quality

- Idempotent restore: check `restore_log` for an existing sandbox ID before creating
- Restore audit log with actor, outcome and error
- One S3 Select per restore instead of one per version per request

## Later: reach

- OAuth login via Salesforce, with roles (viewer, restorer, admin). See design note
- Pluggable storage backends. See design note
- Multi-org: one task per org against a shared database (`org_id` is already in every key and table)
- Archive-and-delete verification flow and deferred delete from the GUI. See design note
- Salesforce LWC integration. See design note
- Notifications (e-mail, Slack) and a backup-health dashboard
- Helm chart; Docker Compose with MinIO for an end-to-end local run
- Configurable period format (weekly, quarterly). See design note
- Storage model beyond full copy per period: base plus incrementals, or dedup on record version
- Blog post: "From Bash Script to Open Source Backup Solution"

## Compatibility and upgrades

Heimdall runs in installations we do not know about, so:

- Schema changes are additive and idempotent (`CREATE ... IF NOT EXISTS`, `ADD COLUMN IF NOT EXISTS`) until Flyway lands; after that every release ships its migrations
- Behaviour that deletes data (archive retention, orphan sweeps) is opt-in, defaults to off, and has a dry-run
- Changes that need a new database instance or a table rewrite (partitioning, RDS encryption, `version` to BIGINT) go into one migration release with a runbook, never into a regular release
- Every release lists its upgrade steps in `CHANGELOG.md`

## Design notes

### Stale run detection

Instead of a fixed time limit, use `last_checkpoint_modstamp` in `backup_runs`. A job that keeps updating its checkpoint is fine regardless of total runtime; one that has not updated it in 60 minutes is hung.

At startup:

1. Look for `RUNNING` rows in `backup_runs`
2. Fresh checkpoint (under 60 minutes): another instance is active, exit gracefully
3. Stale checkpoint: mark the row `ABANDONED` and resume from its checkpoint

A `SELECT ... FOR UPDATE` on a lock row prevents two instances from running at once.

### Pluggable storage backends

Two interfaces, `BlobStore` (CSV and file bytes) and `MetadataStore` (the record index), so each can be swapped independently. Ship S3 and PostgreSQL; make MinIO (S3-compatible) and a local filesystem store the first alternatives, since they also give an end-to-end local run without AWS.

```
BlobStore
├── S3BlobStore (included)
├── MinIO (S3-compatible, configuration only)
├── LocalFileBlobStore (dev, testing, small installs)
└── Azure Blob / Backblaze B2 / Google Cloud Storage (community)
```

Configurable per object in `Heimdall_Backup_Config__c`: a default backend for the org and an optional override per object (for example ContentVersion to cheap storage).

### OAuth login from Salesforce

Users authenticate against the GUI with their Salesforce org credentials, so Heimdall needs no user management of its own.

- OAuth 2.0 web server flow against the connected org
- Roles mapped from Salesforce permission sets: viewer, restorer, admin
- Session management with token refresh
- Embeddable as an iframe in a Salesforce LWC

### Archive-and-delete verification and deferred delete

When `Archive_Action__c = 'Archive and Delete'`, Heimdall must verify the archive before deleting anything from Salesforce:

1. Write the archive data to storage
2. Read it back and verify record count and checksums
3. Delete from Salesforce with the Bulk API
4. Log every deletion: what, when, from which archive period

If verification fails, the delete is skipped and logged; records stay in Salesforce until the next successful archive run.

For `Archive Only`, records stay in Salesforce. The GUI offers a filterable list of archived records that still exist in Salesforce, so an admin can review the archived data, then delete verified records individually or in bulk, with the same audit trail.

Future: ContentVersion binary archiving for files linked only to archived records, and orphan detection for ContentDocuments with no remaining links.

### Salesforce LWC integration

An LWC component that brings Heimdall data into the Salesforce UI:

- A record-page link to the Heimdall record page for the current record, in a new tab or subtab
- An inline iframe showing deleted and archived related records (archived EmailMessages on a Case, archived Cases on an Account)
- Reads `Heimdall_Backup_Config__c` to hide itself on objects where archiving is not configured
- Uses the OAuth flow above, so no separate login

### Configurable period format

The period is a date pattern (`YYMM` monthly, `YYww` weekly, `YYQn` quarterly). The format implicitly sets backup frequency: a weekly format means a new full backup every week. Inherited from the original bash script; today only `YYMM` is supported and the value is stored as a signed integer (negative for archive periods), so a format change is a storage-model change.
