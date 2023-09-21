-- Database initialization script for Heimdall
-- Run this script to create the necessary tables in PostgreSQL
-- Based on the original "One Backup to Rule Them All" schema

-- Table 1: CSV Files tracking
CREATE TABLE IF NOT EXISTS public.csvfiles (
    csv_id INTEGER GENERATED ALWAYS AS IDENTITY,
    period INTEGER NOT NULL,
    s3path TEXT NOT NULL,
    CONSTRAINT csvfiles_pk PRIMARY KEY (csv_id)
);

CREATE INDEX IF NOT EXISTS csvfiles_period_index ON csvfiles (period);

-- Table 2: Objects metadata storage
CREATE TABLE IF NOT EXISTS public.objects (
    id TEXT NOT NULL,
    period INTEGER NOT NULL,
    version INTEGER NOT NULL,
    metadata JSONB,
    org_id TEXT NOT NULL,
    csv_id INTEGER,
    is_deleted BOOLEAN DEFAULT FALSE,
    object_name TEXT,
    name TEXT NOT NULL,
    CONSTRAINT objects_pk PRIMARY KEY (org_id, period, id, version)
);

CREATE INDEX IF NOT EXISTS objects_org_id_id_index ON public.objects (org_id, id);
CREATE INDEX IF NOT EXISTS objects_org_id_object_name_is_deleted_index ON public.objects (org_id, object_name, is_deleted);
CREATE INDEX IF NOT EXISTS objects_org_id_period_index ON objects (org_id, period);

-- Table 3: Backup runs tracking (replaces Salesforce Backup_Run__c for checkpoint management)
CREATE TABLE IF NOT EXISTS public.backup_runs (
    run_id BIGSERIAL PRIMARY KEY,
    object_name TEXT NOT NULL,
    org_id TEXT NOT NULL,
    period INTEGER NOT NULL,
    status TEXT NOT NULL,
    query_all BOOLEAN DEFAULT FALSE,
    started_at TIMESTAMP NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP,
    last_checkpoint_id TEXT,
    last_checkpoint_modstamp TIMESTAMP,
    records_processed INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    bytes_transferred BIGINT DEFAULT 0,
    error_message TEXT,
    archive_expression TEXT
);

CREATE INDEX IF NOT EXISTS backup_runs_org_object_index ON backup_runs (org_id, object_name, period);
CREATE INDEX IF NOT EXISTS backup_runs_status_index ON backup_runs (status);

-- Functional indexes for ContentDocument delete safety checks
-- Find all ContentDocumentLinks pointing to a given ContentDocumentId
CREATE INDEX IF NOT EXISTS objects_cdl_content_doc_id
    ON objects ((metadata->>'ContentDocumentId'))
    WHERE object_name = 'ContentDocumentLink';

-- Find all ContentVersions belonging to a given ContentDocumentId
CREATE INDEX IF NOT EXISTS objects_cv_content_doc_id
    ON objects ((metadata->>'ContentDocumentId'))
    WHERE object_name = 'ContentVersion';

-- ContentDocumentLink by LinkedEntityId (find files for a record)
CREATE INDEX IF NOT EXISTS objects_cdl_linked_entity_id
    ON objects ((metadata->>'LinkedEntityId'))
    WHERE object_name = 'ContentDocumentLink';

-- pg_trgm for fast LIKE '%recordId%' search across metadata (excluding content objects handled separately)
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX IF NOT EXISTS objects_metadata_trgm_no_content_idx
    ON objects USING GIN ((metadata::text) gin_trgm_ops)
    WHERE metadata IS NOT NULL
      AND object_name NOT IN ('ContentVersion', 'ContentDocumentLink', 'ContentDocument');

-- Grant necessary permissions (adjust username as needed)
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO heimdall;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO heimdall;
