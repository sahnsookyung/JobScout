ALTER TABLE job_post
ADD COLUMN IF NOT EXISTS description_source TEXT DEFAULT 'unknown' NOT NULL;

ALTER TABLE job_post
ADD COLUMN IF NOT EXISTS description_completeness TEXT DEFAULT 'unknown' NOT NULL;

ALTER TABLE job_post
ADD COLUMN IF NOT EXISTS description_warning_code TEXT;

ALTER TABLE job_post
ADD COLUMN IF NOT EXISTS description_hash TEXT;

ALTER TABLE job_post
ADD COLUMN IF NOT EXISTS description_recovery_status TEXT DEFAULT 'not_needed' NOT NULL;

ALTER TABLE job_post
ADD COLUMN IF NOT EXISTS description_recovery_reason TEXT;

ALTER TABLE job_post
ADD COLUMN IF NOT EXISTS description_recovery_attempts INTEGER DEFAULT 0 NOT NULL;

ALTER TABLE job_post
ADD COLUMN IF NOT EXISTS description_recovery_last_attempt_at TIMESTAMPTZ;

ALTER TABLE job_post
ADD COLUMN IF NOT EXISTS description_recovery_next_retry_at TIMESTAMPTZ;

ALTER TABLE job_post
ADD COLUMN IF NOT EXISTS description_recovery_last_error TEXT;

ALTER TABLE job_post
ADD COLUMN IF NOT EXISTS description_recovery_run_id TEXT;

ALTER TABLE candidate_preferences
ADD COLUMN IF NOT EXISTS preference_rerank_top_n INTEGER;

ALTER TABLE job_match
ALTER COLUMN preference_score TYPE NUMERIC(5, 2);

ALTER TABLE match_selection_item
ALTER COLUMN preference_score_at_selection TYPE NUMERIC(5, 2);

CREATE TABLE IF NOT EXISTS job_offerings_profile (
    job_post_id UUID PRIMARY KEY
        REFERENCES job_post(id) ON DELETE CASCADE,
    profile_json JSONB DEFAULT '{}'::jsonb NOT NULL,
    profile_schema_version INTEGER DEFAULT 1 NOT NULL,
    source_description_hash TEXT,
    extraction_provider TEXT,
    extraction_model TEXT,
    confidence NUMERIC(5, 4),
    created_at TIMESTAMPTZ DEFAULT timezone('UTC', now()) NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT timezone('UTC', now()) NOT NULL
);

ALTER TABLE llm_match_evaluation
ADD COLUMN IF NOT EXISTS analysis JSONB DEFAULT '{}'::jsonb NOT NULL;

CREATE INDEX IF NOT EXISTS idx_job_post_description_hash
ON job_post (description_hash);

CREATE INDEX IF NOT EXISTS idx_job_post_description_recovery_scan
ON job_post (
    tenant_id,
    status,
    description_recovery_status,
    description_recovery_next_retry_at,
    first_seen_at
);

CREATE INDEX IF NOT EXISTS idx_job_post_missing_description
ON job_post (tenant_id, status, extraction_status, first_seen_at);

CREATE INDEX IF NOT EXISTS idx_jop_source_hash
ON job_offerings_profile (source_description_hash);

CREATE INDEX IF NOT EXISTS idx_jop_schema_version
ON job_offerings_profile (profile_schema_version);

CREATE INDEX IF NOT EXISTS idx_llm_eval_owner_match_created
ON llm_match_evaluation (owner_id, job_match_id, created_at);

CREATE INDEX IF NOT EXISTS idx_llm_eval_backlog_status_created
ON llm_match_evaluation (status, created_at)
WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_llm_eval_retryable_failed_created
ON llm_match_evaluation (created_at)
WHERE deleted_at IS NULL
  AND status = 'failed'
  AND retryable = TRUE;

CREATE INDEX IF NOT EXISTS idx_msi_run_tier_rank_id
ON match_selection_item (selection_run_id, selection_tier, rank_position, id);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'msi_selection_tier_chk'
          AND conrelid = 'match_selection_item'::regclass
    ) THEN
        ALTER TABLE match_selection_item
        ADD CONSTRAINT msi_selection_tier_chk
        CHECK (selection_tier = ANY (ARRAY['primary'::text, 'excluded'::text]));
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'msi_excluded_reason_chk'
          AND conrelid = 'match_selection_item'::regclass
    ) THEN
        ALTER TABLE match_selection_item
        ADD CONSTRAINT msi_excluded_reason_chk
        CHECK (
            (
                selection_tier = 'primary'::text
                AND excluded_reason IS NULL
            )
            OR (
                selection_tier = 'excluded'::text
                AND excluded_reason IS NOT NULL
            )
        );
    END IF;
END $$;
