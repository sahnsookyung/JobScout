# Migration Scripts

This directory contains database migration scripts for schema changes.

## Manual Migration (No Alembic)

Since this project doesn't use Alembic, run these SQL commands directly on your database:

### Add notified column to job_match table

```sql
-- Add the notified column
ALTER TABLE job_match ADD COLUMN notified BOOLEAN DEFAULT FALSE;

-- Create index for efficient queries
CREATE INDEX idx_job_match_notified ON job_match (notified);

-- Update existing rows (optional, defaults will handle new rows)
UPDATE job_match SET notified = FALSE WHERE notified IS NULL;
```

### Verify migration

```sql
-- Check column exists
SELECT column_name FROM information_schema.columns 
WHERE table_name = 'job_match' AND column_name = 'notified';

-- Check index exists
SELECT indexname FROM pg_indexes 
WHERE tablename = 'job_match' AND indexname = 'idx_job_match_notified';
```

## Automated Migration Script

For automated deployments, use the migration script from the project root directory:

```bash
python migrations/migrate_add_notified.py
```
