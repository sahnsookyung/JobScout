ALTER TABLE users
ADD COLUMN IF NOT EXISTS last_login_at TIMESTAMPTZ;

ALTER TABLE users
ADD COLUMN IF NOT EXISTS data_expires_at TIMESTAMPTZ;

ALTER TABLE users
ADD COLUMN IF NOT EXISTS deletion_started_at TIMESTAMPTZ;

ALTER TABLE users
ADD COLUMN IF NOT EXISTS is_platform_admin BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE users
ADD COLUMN IF NOT EXISTS retention_exempt BOOLEAN NOT NULL DEFAULT FALSE;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'ck_users_platform_admin_retention_exempt'
          AND conrelid = 'users'::regclass
    ) THEN
        ALTER TABLE users
        ADD CONSTRAINT ck_users_platform_admin_retention_exempt
        CHECK (NOT is_platform_admin OR retention_exempt);
    END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_users_single_platform_admin
ON users (is_platform_admin)
WHERE is_platform_admin;

CREATE INDEX IF NOT EXISTS idx_users_ephemeral_expiry
ON users (data_expires_at)
WHERE NOT retention_exempt
  AND deletion_started_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_users_deletion_started
ON users (deletion_started_at)
WHERE NOT retention_exempt;

ALTER TABLE candidate_preferences
ADD COLUMN IF NOT EXISTS result_policy JSONB;

ALTER TABLE candidate_preferences
ADD COLUMN IF NOT EXISTS ranking_config JSONB;

ALTER TABLE job_match
ADD COLUMN IF NOT EXISTS owner_id UUID;

ALTER TABLE job_match
ADD COLUMN IF NOT EXISTS tenant_id UUID;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM job_match match
        JOIN structured_resume resume
          ON resume.resume_fingerprint = match.resume_fingerprint
        GROUP BY match.id
        HAVING count(DISTINCT resume.owner_id) <> 1
    ) THEN
        RAISE EXCEPTION
            'Cannot assign job_match owner safely: a resume fingerprint maps to multiple owners';
    END IF;
END $$;

WITH fingerprint_owner AS (
    SELECT resume_fingerprint, min(owner_id::text)::uuid AS owner_id
    FROM structured_resume
    GROUP BY resume_fingerprint
    HAVING count(DISTINCT owner_id) = 1
)
UPDATE job_match match
SET owner_id = mapping.owner_id
FROM fingerprint_owner mapping
WHERE match.owner_id IS NULL
  AND mapping.resume_fingerprint = match.resume_fingerprint;

UPDATE job_match match
SET tenant_id = job.tenant_id
FROM job_post job
WHERE match.tenant_id IS NULL
  AND job.id = match.job_post_id;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM job_match WHERE owner_id IS NULL) THEN
        RAISE EXCEPTION
            'Cannot enable owner isolation: at least one job_match has no unambiguous owner';
    END IF;
END $$;

ALTER TABLE job_match
ALTER COLUMN owner_id SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'job_match_owner_id_fkey'
          AND conrelid = 'job_match'::regclass
    ) THEN
        ALTER TABLE job_match
        ADD CONSTRAINT job_match_owner_id_fkey
        FOREIGN KEY (owner_id) REFERENCES users(id) ON DELETE CASCADE;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'job_match_tenant_id_fkey'
          AND conrelid = 'job_match'::regclass
    ) THEN
        ALTER TABLE job_match
        ADD CONSTRAINT job_match_tenant_id_fkey
        FOREIGN KEY (tenant_id) REFERENCES tenant(id) ON DELETE CASCADE;
    END IF;
END $$;

ALTER TABLE job_match
DROP CONSTRAINT IF EXISTS uq_job_match_job_resume;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'uq_job_match_owner_job_resume'
          AND conrelid = 'job_match'::regclass
    ) THEN
        ALTER TABLE job_match
        ADD CONSTRAINT uq_job_match_owner_job_resume
        UNIQUE (owner_id, job_post_id, resume_fingerprint);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_job_match_owner_job
ON job_match (owner_id, job_post_id);

CREATE INDEX IF NOT EXISTS idx_job_match_tenant_owner
ON job_match (tenant_id, owner_id);

DO $$
DECLARE
    constraint_name TEXT;
BEGIN
    SELECT fk_constraint.conname
    INTO constraint_name
    FROM pg_constraint fk_constraint
    JOIN pg_attribute attribute
      ON attribute.attrelid = fk_constraint.conrelid
     AND attribute.attnum = ANY (fk_constraint.conkey)
    WHERE fk_constraint.contype = 'f'
      AND fk_constraint.conrelid = 'pipeline_run'::regclass
      AND attribute.attname = 'owner_id'
    LIMIT 1;

    IF constraint_name IS NOT NULL THEN
        EXECUTE format(
            'ALTER TABLE pipeline_run DROP CONSTRAINT %I',
            constraint_name
        );
    END IF;

    ALTER TABLE pipeline_run
    ADD CONSTRAINT pipeline_run_owner_id_fkey
    FOREIGN KEY (owner_id) REFERENCES users(id) ON DELETE CASCADE;
END $$;
