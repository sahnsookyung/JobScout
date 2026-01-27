# Appendix A — Filtering & Matching Design (Vector Retrieval + Rule Scoring)

## A1. Purpose
This appendix specifies the semantic filtering and scoring pipeline used to rank job posts for a user based on the user’s Master Resume, and to provide evidence-backed reasoning for each score.

## A2. Key concepts & terminology
- **Resume Evidence Unit (REU):** Atomic, provenance-trackable claim extracted from the Master Resume (e.g., “Built X using Y; improved Z by N%”).
- **Job Requirement Unit (JRU):** Atomic requirement extracted from a job description (e.g., “3+ years Python,” “experience with AWS,” “build ETL pipelines”).
- **Vector Retrieval:** Embed REUs/JRUs into a shared vector space and retrieve nearest neighbors by similarity (broad sieve).

Semantic search is implemented as “embed corpus entries, embed query, retrieve closest embeddings,” which tends to handle synonyms and paraphrases better than keyword-only matching. [page:2]

For short-requirement → longer-evidence matching, the system should treat retrieval as asymmetric semantic search and use an embedding setup appropriate for that pattern. [page:2]

## A3. Pipeline overview
1. **Job ingestion**  
   The system ingests new JobPosts via JobSpy or jobspy-api and stores raw and normalized JD data.  
   If using jobspy-api, the system calls `GET /api/v1/search_jobs` and authenticates with the `x-api-key` header. [page:1]

2. **Normalize job descriptions (JD → JRUs)**  
   The system extracts JRUs including required skills, preferred skills, responsibilities, seniority signals, location/remote constraints, and compensation signals.

3. **Normalize resumes (Master Resume → REUs)**  
   The system extracts REUs and preserves provenance links back to the original resume text and resume version.

4. **Embedding**  
   The system generates embeddings for JRUs and REUs using the configured embedding model and stores them (plus metadata) for retrieval.

5. **Stage 1: Broad filtering via vector retrieval**  
   Given a JobPost (JRUs) and a Resume version (REUs), retrieve top-K candidate REUs for each JRU using similarity search, producing (JRU → candidate REUs with similarity scores).

6. **Stage 2: Deterministic scoring + penalties (fine sieve)**  
   Compute a final JobMatch score as a weighted function of (a) required coverage, (b) preferred coverage, and (c) explicit penalties (below).

7. **Evidence gating for “no made-up claims”**  
   A requirement can only be marked “covered” if at least one REU is attached with similarity ≥ threshold and passes policy checks (e.g., recency rules).

8. **Output**  
   Persist JobMatch with requirement→evidence links and missing items, then surface results in the Inbox and Notifications.

## A4. Scoring model (recommended v1)

### A4.1 Coverage
- RequiredCoverage = (# required JRUs covered) / (total required JRUs)
- PreferredCoverage = (# preferred JRUs covered) / (total preferred JRUs)

### A4.2 Penalties (explicit, not learned)
Penalties are computed from normalized tags and rule checks, independent of embedding similarity.

Recommended penalty classes:
- Missing must-have skills/requirements: subtract P_missing_required per missing required JRU.
- Location/remote mismatch: subtract P_location_mismatch or mark job as “Low-signal for this user” depending on user preference strictness.
- Seniority mismatch: subtract P_seniority_mismatch when JD seniority signals conflict with user’s target level.
- Compensation mismatch (if present): subtract P_comp_mismatch when salary range is below user’s minimum (if user enabled a hard constraint).
- Duplicate/repost dampening: apply P_duplicate to near-duplicate jobs to avoid repeated review.

### A4.3 Final score (example)
Score = 100 * (w_req * RequiredCoverage + w_pref * PreferredCoverage) - Σ(Penalties)

Where w_req > w_pref and all weights/penalties are configurable per user and/or globally.

## A5. Configuration & explainability requirements (new FRs)
- FR-Apx1: The system shall persist the top-K retrieved evidence candidates per JRU (including similarity scores) to enable audit and debugging.
- FR-Apx2: The system shall expose the scoring breakdown per JobMatch: RequiredCoverage, PreferredCoverage, each penalty applied, and final score.
- FR-Apx3: The system shall support configurable thresholds per JRU type (e.g., higher threshold for “required skill” than for “preferred responsibility”).
- FR-Apx4: The system shall support “hard constraints” (e.g., remote-only) that can override score and force a job label (e.g., “Excluded by constraint”) without deletion.
- FR-Apx5: The system shall ensure each “covered” JRU displayed to the user includes at least one linked REU as evidence; otherwise it must be displayed as missing/unsupported and must block auto-claiming in generation.

## A6. Data model additions (optional but recommended)
- JobRequirementUnit: id, job_id, text, type(required/preferred/responsibility/constraint), normalized_tags, embedding, created_at
- ResumeEvidenceUnit: id, resume_version, text, normalized_tags (skills, tools, domain), embedding, provenance_pointer, created_at
- RetrievalHit: job_requirement_unit_id, resume_evidence_unit_id, similarity_score, retrieved_at

(These can be materialized tables or derived and stored inside JobMatch.requirement_matches depending on scale/performance needs.)

## A7. Service interfaces (optional additions)
- `POST /v1/match/score`  
  Inputs: resume_master_version, job_id (or JD text), scoring_profile_id, thresholds  
  Outputs: JobMatch (score + breakdown + requirement→evidence links)

- `POST /v1/retrieval/debug`  
  Inputs: job_requirement_unit_id, resume_master_version, top_k  
  Outputs: ranked evidence candidates with similarity scores

## A8. Acceptance tests (examples)
- AT-Apx1: Given a JD with 10 required JRUs and a resume that supports 7 with evidence above threshold, RequiredCoverage shall equal 0.7 and exactly 3 required JRUs shall appear in missing_items.
- AT-Apx2: If a job violates a user hard constraint (e.g., “remote-only”), the job shall not be deleted but must be labeled “Excluded by constraint” with an explanation.
- AT-Apx3: If “evidence-required” is enabled, export/generation shall be blocked for any tailored statement that lacks a mapped REU.
