-- Additive: existing vectors and matches remain explicitly in the legacy space.
-- Deploy compatible readers before starting a resumable data backfill.
ALTER TABLE job_post ADD COLUMN IF NOT EXISTS embedding_space TEXT NOT NULL DEFAULT 'legacy';
ALTER TABLE job_requirement_unit_embedding ADD COLUMN IF NOT EXISTS embedding_space TEXT NOT NULL DEFAULT 'legacy';
ALTER TABLE resume_section_embedding ADD COLUMN IF NOT EXISTS embedding_space TEXT NOT NULL DEFAULT 'legacy';
ALTER TABLE resume_section_embedding ADD COLUMN IF NOT EXISTS query_embedding vector(1024);
ALTER TABLE resume_evidence_unit_embedding ADD COLUMN IF NOT EXISTS embedding_space TEXT NOT NULL DEFAULT 'legacy';
ALTER TABLE job_match ADD COLUMN IF NOT EXISTS embedding_space TEXT NOT NULL DEFAULT 'legacy';
