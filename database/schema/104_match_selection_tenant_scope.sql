ALTER TABLE match_selection_run
ADD COLUMN IF NOT EXISTS tenant_id UUID;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'match_selection_run_tenant_id_fkey'
          AND conrelid = 'match_selection_run'::regclass
    ) THEN
        ALTER TABLE match_selection_run
        ADD CONSTRAINT match_selection_run_tenant_id_fkey
        FOREIGN KEY (tenant_id) REFERENCES tenant(id) ON DELETE CASCADE;
    END IF;
END $$;

DROP INDEX IF EXISTS idx_match_selection_run_current;

CREATE UNIQUE INDEX IF NOT EXISTS idx_match_selection_run_current_tenant
ON match_selection_run (owner_id, resume_fingerprint, tenant_id)
WHERE tenant_id IS NOT NULL AND is_current AND lifecycle_status = 'committed';

CREATE UNIQUE INDEX IF NOT EXISTS idx_match_selection_run_current_global
ON match_selection_run (owner_id, resume_fingerprint)
WHERE tenant_id IS NULL AND is_current AND lifecycle_status = 'committed';
