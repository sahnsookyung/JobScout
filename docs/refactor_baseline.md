# Matcher Refactor Baseline - 2025-02-02

## Test Results

### Unit Tests (m="not db")
- Total: 153 tests
- Passed: 148 tests
- Failed: 2 tests (known broken tests in test_multi_embedding_matching.py)
- Skipped: 3 tests (DB tests when DB not available)
- Duration: ~50 seconds

### Known Failures
1. `test_multi_embedding_matching.py::test_02_requirement_section_similarity`
   - Error: `AttributeError: Mock object has no attribute 'db'`
   - Root cause: Test calls non-existent `calculate_requirement_similarity_with_resume_sections` method
   - Action: Replace with valid tests in Phase 3

2. `test_multi_embedding_matching.py::test_03_experience_mismatch_penalty`
   - Error: Same as above
   - Root cause: Test calls non-existent method
   - Action: Replace with valid tests in Phase 3

## Code Metrics (Before Refactor)

### core/matcher_service.py
- Total lines: 995
- Methods: 15 public methods
- Dataclasses: 5 (ResumeEvidenceUnit, StructuredResumeProfile, RequirementMatchResult, PreferencesAlignmentScore, JobMatchPreliminary)
- Mock service: 1 (MockMatcherService as subclass - anti-pattern)

### Key Behaviors to Preserve

1. **Two-stage matching pipeline:**
   - Stage 1: Retrieve top-K candidates using resume-level embedding
   - Stage 2: Compute requirement-level matching on retrieved candidates

2. **Resume embedding generation (current):**
   - In match_resume_to_job: Uses `evidence_units[:5]` slice
   - In match_resume_two_stage: Uses `evidence_units[:10]` slice
   - Concatenates text and calls `ai.generate_embedding(resume_text)`

3. **Preference weights (hard-coded):**
   - Location: 0.35
   - Company size: 0.15
   - Industry: 0.25
   - Role: 0.25

4. **Similarity threshold:** Default 0.5

5. **Requirement matching:**
   - For each requirement: Find best evidence by cosine similarity
   - Check if similarity >= threshold
   - Classify as matched/missing
   - Duplicated in two places (lines 715-748, 875-908)

## Target Improvements

### Structural Refactor
1. Extract dataclasses to separate module
2. Extract similarity calculator to utility
3. Extract preference matcher to focused module
4. Extract requirement matcher (single source of truth)
5. Extract years extractor to separate module
6. Extract resume profiler to separate module
7. Create Stage-1 embedding builder (support text + pooled REU modes)

### Algorithmic Improvements (NEW)
1. **Stage-1 resume embedding via pooled REU embeddings:**
   - Replace text concatenation with weighted mean of curated REU embeddings
   - Robust to resume ordering
   - Avoids very long text inputs to embedding model
   - Curated subset: Summary (weight=3.0), Skills (2.0), Experience (1.5), Projects (0.5), Education (0.0)

2. **Config-driven weights:**
   - Extract preference weights to config
   - Extract section weights to config
   - Mode selection: "text" vs "pooled_reu"

3. **Feature flag:**
   - `stage1_embedding.mode` allows A/B testing and rollback

## Success Criteria

### Code Structure
- MatcherService reduced to ~200 lines
- No duplicate requirement matching logic
- Mock service not a subclass (Protocol-based)
- Section weights configurable
- Preference weights configurable

### Algorithmic
- Stage-1 uses pooled REU embeddings by default
- Pooled embeddings are L2 normalized
- Curated subset is deterministic (section-based)
- No JD consultation in subset selection

### Testing
- All existing tests pass
- New tests for pooled mode
- Critical behavior tests (pooling, normalization, selection, no extra embedding calls)
