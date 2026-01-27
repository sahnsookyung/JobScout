# JobSearch+Tailor — Postgres Schema + Embeddings + Atomic Units (v0.1)

## Overview
This schema supports a multiuser SaaS (tenant isolation) while still working for self-host (single tenant) deployments. For SaaS isolation, PostgreSQL Row Level Security (RLS) is a common pattern: store a `tenant_id` on tenant-scoped tables and enforce access via RLS policies. [web:107]  
The ingestion data you showed (e.g., `site`, `job_url`, `title`, `company`, `location`, `date_posted`, `description`, `skills`, salary fields) maps cleanly into “canonical job” + “source sightings” + “content blob” tables. [file:248]

## Embedding model decision
Use **bge-base-en-v1.5** for embeddings; it outputs a **768-dimensional** vector, which is a good balance of quality and cost for semantic matching. [web:349][web:306]  
In Postgres, store vectors using `pgvector` as `vector(768)` and create an HNSW index with `vector_cosine_ops` for fast approximate nearest-neighbor search. [web:357][web:354]

## Postgres schema (DDL)
> Assumptions:
> - You run `CREATE EXTENSION vector;` once per DB.
> - You use **canonical jobs** (deduped) + **source sightings** (board-specific URL/metadata).
> - You generate **job_requirement_units** (JRUs) from the raw description, then embed those units for matching.
> - You store user-owned entities with `tenant_id` and/or `user_id`.

```sql
-- Extensions
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS vector;

-- =========================
-- Tenancy + users
-- =========================
CREATE TABLE tenant (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE app_user (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL REFERENCES tenant(id) ON DELETE CASCADE,
  email citext NOT NULL,
  settings jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, email)
);

-- Optional (recommended): RLS enablement per tenant-scoped table.
-- See AWS guidance on tenant isolation via RLS.[1]
-- ALTER TABLE app_user ENABLE ROW LEVEL SECURITY;
-- CREATE POLICY tenant_isolation_app_user ON app_user
--   USING (tenant_id = current_setting('app.tenant_id')::uuid);

-- =========================
-- Global (or tenant-scoped) job corpus
-- =========================
-- Canonical job (deduped across boards)
CREATE TABLE job_post (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),

  -- If you want GLOBAL shared job corpus in SaaS:
  --   keep tenant_id NULL and enforce that only public fields are stored here.
  -- If you want PER-TENANT job corpus:
  --   set tenant_id NOT NULL and apply RLS.
  tenant_id uuid NULL REFERENCES tenant(id) ON DELETE CASCADE,

  title text NOT NULL,
  company text NOT NULL,
  location_text text,
  is_remote boolean,
  job_type text,
  job_level text,

  currency text,
  salary_min numeric,
  salary_max numeric,
  salary_interval text,

  -- Canonical dedupe key you compute (see "fingerprinting notes" below).
  canonical_fingerprint text NOT NULL,
  fingerprint_version int NOT NULL DEFAULT 1,

  first_seen_at timestamptz NOT NULL DEFAULT now(),
  last_seen_at  timestamptz NOT NULL DEFAULT now(),

  -- Lifecycle state: keep history; avoid hard deletes.
  status text NOT NULL DEFAULT 'active', -- active|expired|unknown

  UNIQUE (tenant_id, fingerprint_version, canonical_fingerprint)
);

CREATE INDEX idx_job_post_last_seen ON job_post(last_seen_at);
CREATE INDEX idx_job_post_company ON job_post(company);
CREATE INDEX idx_job_post_remote ON job_post(is_remote);
CREATE INDEX idx_job_post_tenant ON job_post(tenant_id);

-- Each board-specific sighting/URL of a canonical job
-- (Your CSV has site, job_url, job_url_direct, date_posted, etc.)[2]
CREATE TABLE job_post_source (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  job_post_id uuid NOT NULL REFERENCES job_post(id) ON DELETE CASCADE,

  site text NOT NULL,               -- CSV: site[2]
  job_url text NOT NULL,            -- CSV: job_url[2]
  job_url_direct text,              -- CSV: job_url_direct[2]
  source_job_id text,               -- if extractable per board
  date_posted date,                 -- CSV: date_posted[2]

  first_seen_at timestamptz NOT NULL DEFAULT now(),
  last_seen_at  timestamptz NOT NULL DEFAULT now(),
  is_active boolean NOT NULL DEFAULT true,

  UNIQUE (site, job_url)
);

CREATE INDEX idx_job_post_source_job ON job_post_source(job_post_id);
CREATE INDEX idx_job_post_source_seen ON job_post_source(last_seen_at);

-- Heavy/raw content (1:1 with canonical job)
-- (Your CSV has description, skills, emails, company_* fields.)[2]
CREATE TABLE job_post_content (
  job_post_id uuid PRIMARY KEY REFERENCES job_post(id) ON DELETE CASCADE,
  description text,                 -- CSV: description[2]
  skills_raw text,                  -- CSV: skills[2]
  emails text,                      -- CSV: emails[2]
  company_industry text,
  company_url text,
  company_logo text,
  company_url_direct text,
  company_addresses text,
  company_num_employees text,
  company_revenue text,
  company_description text,
  experience_range text,
  company_rating numeric,
  company_reviews_count int,
  vacancy_count int,
  work_from_home_type text,

  -- Keep raw ingest payload for debugging / reprocessing
  raw_payload jsonb NOT NULL DEFAULT '{}'::jsonb
);

-- =========================
-- Job requirement units (atomic chunks) + embeddings
-- =========================
-- JRUs are created by your parsing worker (LLM/heuristics) from job_post_content.description.[2]
CREATE TABLE job_requirement_unit (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  job_post_id uuid NOT NULL REFERENCES job_post(id) ON DELETE CASCADE,

  -- required|preferred|responsibility|constraint|benefit
  req_type text NOT NULL,

  -- One atomic proposition / bullet / subpoint
  text text NOT NULL,

  -- Normalized tags for scoring and filtering (skills, tools, years, etc.)
  tags jsonb NOT NULL DEFAULT '{}'::jsonb,

  ordinal int,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_jru_job ON job_requirement_unit(job_post_id);

-- Separate table keeps your relational row small and makes re-embedding easier.
-- (Fixed dimension: bge-base-en-v1.5 => 768 dims.)[3][4]
CREATE TABLE job_requirement_unit_embedding (
  job_requirement_unit_id uuid PRIMARY KEY
    REFERENCES job_requirement_unit(id) ON DELETE CASCADE,
  embedding vector(768) NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- HNSW index for cosine similarity search in pgvector.[5][6]
CREATE INDEX jru_embedding_hnsw
  ON job_requirement_unit_embedding
  USING hnsw (embedding vector_cosine_ops);

-- =========================
-- Resume (user-scoped) + embeddings
-- =========================
CREATE TABLE resume_master (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id uuid NOT NULL REFERENCES app_user(id) ON DELETE CASCADE,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE resume_version (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  resume_master_id uuid NOT NULL REFERENCES resume_master(id) ON DELETE CASCADE,
  version int NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (resume_master_id, version)
);

-- Atomic resume claims (REUs)
CREATE TABLE resume_evidence_unit (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  resume_version_id uuid NOT NULL REFERENCES resume_version(id) ON DELETE CASCADE,
  text text NOT NULL,
  tags jsonb NOT NULL DEFAULT '{}'::jsonb,
  provenance jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_reu_version ON resume_evidence_unit(resume_version_id);

CREATE TABLE resume_evidence_unit_embedding (
  resume_evidence_unit_id uuid PRIMARY KEY
    REFERENCES resume_evidence_unit(id) ON DELETE CASCADE,
  embedding vector(768) NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX reu_embedding_hnsw
  ON resume_evidence_unit_embedding
  USING hnsw (embedding vector_cosine_ops);

-- =========================
-- Matching + explainability + workflow
-- =========================
-- Match is a relationship; never store it on job_post because it is per-user + per-resume-version.
CREATE TABLE job_match (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id uuid NOT NULL REFERENCES app_user(id) ON DELETE CASCADE,
  resume_version_id uuid NOT NULL REFERENCES resume_version(id) ON DELETE CASCADE,
  job_post_id uuid NOT NULL REFERENCES job_post(id) ON DELETE CASCADE,

  overall_score numeric NOT NULL,
  score_breakdown jsonb NOT NULL DEFAULT '{}'::jsonb, -- coverage + penalties
  computed_at timestamptz NOT NULL DEFAULT now(),

  UNIQUE (user_id, resume_version_id, job_post_id)
);

CREATE INDEX idx_job_match_user_score ON job_match(user_id, overall_score DESC);

-- Evidence mapping: which REU supports which JRU (with similarity)
CREATE TABLE job_requirement_match (
  job_match_id uuid NOT NULL REFERENCES job_match(id) ON DELETE CASCADE,
  job_requirement_unit_id uuid NOT NULL REFERENCES job_requirement_unit(id) ON DELETE CASCADE,
  resume_evidence_unit_id uuid NULL REFERENCES resume_evidence_unit(id) ON DELETE SET NULL,
  similarity numeric,
  covered boolean NOT NULL DEFAULT false,
  PRIMARY KEY (job_match_id, job_requirement_unit_id, resume_evidence_unit_id)
);

-- Tracker is the user's application pipeline
CREATE TABLE job_tracker (
  user_id uuid NOT NULL REFERENCES app_user(id) ON DELETE CASCADE,
  job_post_id uuid NOT NULL REFERENCES job_post(id) ON DELETE CASCADE,
  status text NOT NULL DEFAULT 'new', -- New, Shortlisted, Tailoring, Applied, etc.
  status_history jsonb NOT NULL DEFAULT '[]'::jsonb,
  notes text,
  reminder_at timestamptz,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, job_post_id)
);

-- Generated tailored artifacts (resume/cover letter/email drafts)
CREATE TABLE tailored_variant (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id uuid NOT NULL REFERENCES app_user(id) ON DELETE CASCADE,
  job_post_id uuid NOT NULL REFERENCES job_post(id) ON DELETE CASCADE,
  resume_version_id uuid NOT NULL REFERENCES resume_version(id) ON DELETE CASCADE,

  doc_type text NOT NULL,           -- resume|cover_letter|followup_email
  content_md text NOT NULL,
  provenance_map jsonb NOT NULL DEFAULT '{}'::jsonb,
  diff jsonb NOT NULL DEFAULT '{}'::jsonb,
  approval_state text NOT NULL DEFAULT 'pending',

  created_at timestamptz NOT NULL DEFAULT now()
);

-- Notifications
CREATE TABLE notification (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id uuid NOT NULL REFERENCES app_user(id) ON DELETE CASCADE,
  type text NOT NULL,
  payload jsonb NOT NULL,
  send_state text NOT NULL DEFAULT 'queued',
  created_at timestamptz NOT NULL DEFAULT now()
);
