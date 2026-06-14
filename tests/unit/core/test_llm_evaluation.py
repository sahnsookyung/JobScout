"""Tests for match-level LLM evaluation caching."""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from sqlalchemy.exc import IntegrityError

from core.llm_evaluation import (
    LlmJudgeConflictError,
    LlmJudgeQuotaExceededError,
    LlmJudgeUnavailableError,
    MatchLlmEvaluationService,
    evaluation_public_dict,
)
from core.resume_evidence_selection import select_relevant_resume_evidence_units
from database.models import (
    LLM_EVALUATION_DELETED,
    LLM_EVALUATION_FAILED,
    LLM_EVALUATION_PENDING,
    LLM_EVALUATION_RUNNING,
    LLM_EVALUATION_SUCCEEDED,
    LlmMatchEvaluation,
)


def _config(
    *,
    judge_enabled=True,
    llm_enabled=True,
    base_url="https://llm.local",
    model="judge-model",
    max_per_owner_per_day=25,
    reuse_ttl_days=90,
):
    return SimpleNamespace(
        matching=SimpleNamespace(
            llm_judge=SimpleNamespace(
                enabled=judge_enabled,
                runtime=SimpleNamespace(
                    base_url=base_url,
                    model=model,
                    provider="openai_compatible",
                    api_key="judge-key" if llm_enabled else None,
                    api_secret=None,
                    headers=None,
                    structured_output_mode="auto",
                    temperature=0.0,
                    timeout_seconds=20,
                    max_input_tokens=4000,
                ),
                max_per_run=10,
                max_per_owner_per_day=max_per_owner_per_day,
                reuse_ttl_days=reuse_ttl_days,
                prompt_version="match-judge-v1",
                schema_version="1",
            ),
        )
    )


def _service(config=None, db=None):
    return MatchLlmEvaluationService(db or Mock(), config=config or _config())


def _match(*, tenant_id=None):
    return SimpleNamespace(
        id="match-1",
        resume_fingerprint="resume-fp",
        job_post_id="job-1",
        job_post=SimpleNamespace(tenant_id=tenant_id),
    )


def _evaluation(status=LLM_EVALUATION_SUCCEEDED, completed_at=None):
    now = datetime.now(timezone.utc)
    return SimpleNamespace(
        id="eval-1",
        owner_id="owner-1",
        tenant_id=None,
        job_post_id="job-1",
        job_match_id="match-1",
        resume_fingerprint="resume-fp",
        provider="openai",
        model="judge-model",
        prompt_version="match-judge-v1",
        schema_version="1",
        judge_config_hash="config-hash",
        evidence_hash="evidence-hash",
        input_hash="input-hash",
        status=status,
        llm_score=82.5,
        confidence=0.88,
        verdict="good",
        summary="Good fit.",
        reason_codes=["skills_match"],
        requirement_verdicts=[{"requirement_id": "req-1", "verdict": "strong"}],
        error_code=None,
        retryable=False,
        created_at=now,
        started_at=now,
        completed_at=completed_at or now,
        deleted_at=None,
    )


def _hashes():
    return {
        "judge_config_hash": "config-hash",
        "evidence_hash": "evidence-hash",
        "input_hash": "input-hash",
    }


class _QueryChain:
    def __init__(self, rows):
        self.rows = rows

    def options(self, *args, **kwargs):
        return self

    def filter(self, *args, **kwargs):
        return self

    def order_by(self, *args, **kwargs):
        return self

    def all(self):
        return self.rows


class _ScalarResult:
    def __init__(self, *, value=None, rows=None):
        self.value = value
        self.rows = rows or []

    def scalar_one_or_none(self):
        return self.value

    def scalars(self):
        return SimpleNamespace(all=lambda: self.rows)


class _CapturingProvider:
    def __init__(self):
        self.calls = []

    def extract_structured_data(self, text, schema_spec, system_prompt=None, user_message=None):
        self.calls.append(
            {
                "text": text,
                "schema_spec": schema_spec,
                "system_prompt": system_prompt,
                "user_message": user_message,
            }
        )
        return {
            "score": 78.5,
            "confidence": 0.72,
            "verdict": "good",
            "summary": "The candidate is a plausible fit.",
            "reason_codes": ["transferable_skills"],
            "requirement_verdicts": [],
            "transferable_strengths": ["Java experience transfers to Kotlin reasonably well."],
            "gaps": ["No direct Kotlin production evidence."],
            "ranking_rationale": "Strong adjacent JVM evidence with a remaining direct-language gap.",
        }


def _wire_minimal_generation(service, *, match=None, existing=None, created=None):
    service._get_match_for_owner = Mock(return_value=match or _match())
    service.build_judge_input = Mock(
        return_value=SimpleNamespace(
            provider_payload={"safe": "payload"},
            hashes=_hashes(),
            truncation={"truncated": False, "fields": {}},
        )
    )
    service._find_active_cache = Mock(return_value=existing)
    service._check_daily_quota = Mock()
    service._create_pending_evaluation = Mock(
        return_value=created or _evaluation(status=LLM_EVALUATION_PENDING)
    )
    service._run_provider = Mock()


def test_unavailable_provider_blocks_generation_before_db_work():
    service = _service(_config(judge_enabled=False))

    with pytest.raises(LlmJudgeUnavailableError):
        service.generate_for_match("match-1", owner_id="owner-1")

def test_unavailable_provider_requires_credentials_for_remote_endpoint():
    service = _service(_config(llm_enabled=False))

    assert service.is_available() is False
    assert service.availability_status() == (False, "credentials_missing")

    with pytest.raises(LlmJudgeUnavailableError) as exc_info:
        service.generate_for_match("match-1", owner_id="owner-1")
    assert exc_info.value.reason == "credentials_missing"
    assert "CEREBRAS_API_KEY" in str(exc_info.value)


def test_generate_reuses_fresh_successful_cache_without_quota():
    service = _service()
    existing = _evaluation(status=LLM_EVALUATION_SUCCEEDED)
    _wire_minimal_generation(service, existing=existing)

    result = service.generate_for_match("match-1", owner_id="owner-1")

    assert result.reused is True
    assert result.evaluation is existing
    service._check_daily_quota.assert_not_called()
    service._create_pending_evaluation.assert_not_called()


def test_failed_cache_is_tombstoned_and_retried():
    db = Mock()
    service = _service(db=db)
    existing = _evaluation(status=LLM_EVALUATION_FAILED)
    created = _evaluation(status=LLM_EVALUATION_PENDING)
    _wire_minimal_generation(service, existing=existing, created=created)

    result = service.generate_for_match("match-1", owner_id="owner-1")

    assert result.reused is False
    assert result.evaluation is created
    assert existing.status == LLM_EVALUATION_DELETED
    assert existing.summary is None
    service._check_daily_quota.assert_called_once_with("owner-1")
    service._run_provider.assert_called_once_with(
        created,
        {"safe": "payload"},
        truncation={"truncated": False, "fields": {}},
    )
    db.commit.assert_called_once()


def test_run_provider_embeds_payload_in_user_message_for_openai_provider_path():
    db = Mock()
    provider = _CapturingProvider()
    service = MatchLlmEvaluationService(db, config=_config(), llm_provider=provider)
    evaluation = _evaluation(status=LLM_EVALUATION_PENDING)
    payload = {
        "job": {"description": "FULL JD SENTINEL"},
        "resume_evidence_units": [{"source_text": "RESUME EVIDENCE SENTINEL"}],
    }

    service._run_provider(
        evaluation,
        payload,
        truncation={"truncated": False, "fields": {}},
    )

    assert evaluation.status == LLM_EVALUATION_SUCCEEDED
    assert provider.calls
    call = provider.calls[0]
    assert "FULL JD SENTINEL" in call["text"]
    assert "RESUME EVIDENCE SENTINEL" in call["text"]
    assert "FULL JD SENTINEL" in call["user_message"]
    assert "RESUME EVIDENCE SENTINEL" in call["user_message"]
    assert "<JUDGE_INPUT_JSON>" in call["user_message"]
    db.flush.assert_called()


def test_generate_infers_tenant_from_match_job_when_not_explicit():
    db = Mock()
    service = _service(db=db)
    created = _evaluation(status=LLM_EVALUATION_PENDING)
    _wire_minimal_generation(
        service,
        match=_match(tenant_id="tenant-from-job"),
        existing=None,
        created=created,
    )

    result = service.generate_for_match("match-1", owner_id="owner-1", tenant_id=None)

    assert result.evaluation is created
    service._find_active_cache.assert_called_once()
    assert service._find_active_cache.call_args.kwargs["tenant_id"] == "tenant-from-job"
    assert service._create_pending_evaluation.call_args.kwargs["tenant_id"] == "tenant-from-job"


def test_quota_is_checked_before_tombstoning_expired_cache():
    db = Mock()
    service = _service(_config(reuse_ttl_days=1), db=db)
    existing = _evaluation(
        status=LLM_EVALUATION_SUCCEEDED,
        completed_at=datetime.now(timezone.utc) - timedelta(days=2),
    )
    _wire_minimal_generation(service, existing=existing)
    service._check_daily_quota.side_effect = LlmJudgeQuotaExceededError("quota")

    with pytest.raises(LlmJudgeQuotaExceededError):
        service.generate_for_match("match-1", owner_id="owner-1")

    assert existing.status == LLM_EVALUATION_SUCCEEDED
    assert existing.deleted_at is None
    db.flush.assert_not_called()


def test_generate_reuses_pending_cache_without_force():
    service = _service()
    existing = _evaluation(status=LLM_EVALUATION_PENDING)
    _wire_minimal_generation(service, existing=existing)

    result = service.generate_for_match("match-1", owner_id="owner-1")

    assert result.reused is True
    assert result.evaluation is existing
    service._check_daily_quota.assert_not_called()


def test_generate_force_conflicts_with_running_cache():
    service = _service()
    existing = _evaluation(status=LLM_EVALUATION_RUNNING)
    _wire_minimal_generation(service, existing=existing)

    with pytest.raises(LlmJudgeConflictError):
        service.generate_for_match("match-1", owner_id="owner-1", force=True)


def test_generate_reuses_cache_after_unique_constraint_race():
    db = Mock()
    db.flush.side_effect = IntegrityError("insert", {}, Exception("duplicate"))
    service = _service(db=db)
    created = _evaluation(status=LLM_EVALUATION_PENDING)
    raced = _evaluation(status=LLM_EVALUATION_RUNNING)
    _wire_minimal_generation(service, existing=None, created=created)
    service._find_active_cache.side_effect = [None, raced]

    result = service.generate_for_match("match-1", owner_id="owner-1")

    assert result.reused is True
    assert result.evaluation is raced
    db.rollback.assert_called_once()
    service._run_provider.assert_not_called()


def test_check_daily_quota_counts_tombstoned_attempts():
    db = Mock()
    db.scalar.return_value = 25
    service = _service(_config(max_per_owner_per_day=25), db=db)

    with pytest.raises(LlmJudgeQuotaExceededError):
        service._check_daily_quota("owner-1")

    rendered_query = str(db.scalar.call_args.args[0])
    assert "deleted_at" not in rendered_query


def test_check_daily_quota_allows_below_limit():
    db = Mock()
    db.scalar.return_value = 24
    service = _service(_config(max_per_owner_per_day=25), db=db)

    service._check_daily_quota("owner-1")

    assert db.scalar.called


def test_tombstone_clears_generated_explanation_fields():
    service = _service()
    evaluation = _evaluation()

    service._tombstone(evaluation)

    assert evaluation.status == LLM_EVALUATION_DELETED
    assert evaluation.deleted_at is not None
    assert evaluation.llm_score is None
    assert evaluation.confidence is None
    assert evaluation.verdict is None
    assert evaluation.summary is None
    assert evaluation.reason_codes == []
    assert evaluation.requirement_verdicts == []
    assert evaluation.error_code is None
    assert evaluation.retryable is False


def test_public_serialization_excludes_cache_hashes_and_raw_inputs():
    public = evaluation_public_dict(_evaluation())

    assert public["status"] == LLM_EVALUATION_SUCCEEDED
    assert public["llm_score"] == 82.5
    assert "judge_config_hash" not in public
    assert "evidence_hash" not in public
    assert "input_hash" not in public
    assert "owner_id" not in public
    assert "tenant_id" not in public


def test_public_serialization_handles_missing_optional_values():
    evaluation = _evaluation()
    evaluation.llm_score = None
    evaluation.confidence = None
    evaluation.reason_codes = {"unsafe": "shape"}
    evaluation.requirement_verdicts = {"unsafe": "shape"}
    evaluation.started_at = None

    public = evaluation_public_dict(evaluation)

    assert public["llm_score"] is None
    assert public["confidence"] is None
    assert public["reason_codes"] == []
    assert public["requirement_verdicts"] == []
    assert public["started_at"] is None


def test_list_for_match_filters_global_tenant_scope():
    db = Mock()
    db.execute.return_value.scalars.return_value.all.return_value = []
    service = _service(db=db)
    service._get_match_for_owner = Mock(return_value=_match())

    service.list_for_match("match-1", owner_id="owner-1", tenant_id=None)

    rendered_query = str(db.execute.call_args.args[0])
    assert "tenant_id IS NULL" in rendered_query


def test_list_for_match_filters_specific_tenant_scope():
    db = Mock()
    db.execute.return_value.scalars.return_value.all.return_value = []
    service = _service(db=db)
    service._get_match_for_owner = Mock(return_value=_match())

    service.list_for_match("match-1", owner_id="owner-1", tenant_id="tenant-1")

    rendered_query = str(db.execute.call_args.args[0])
    assert "tenant_id = :tenant_id_1" in rendered_query


def test_list_for_match_infers_tenant_from_match_job():
    db = Mock()
    db.execute.return_value.scalars.return_value.all.return_value = []
    service = _service(db=db)
    service._get_match_for_owner = Mock(return_value=_match(tenant_id="tenant-from-job"))

    service.list_for_match("match-1", owner_id="owner-1", tenant_id=None)

    rendered_query = str(db.execute.call_args.args[0])
    assert "tenant_id = :tenant_id_1" in rendered_query


def test_delete_evaluation_tombstones_matching_owner_scoped_row():
    db = Mock()
    service = _service(db=db)
    evaluation = _evaluation()
    service._get_match_for_owner = Mock(return_value=_match())
    service._get_evaluation_for_owner = Mock(return_value=evaluation)

    service.delete_evaluation("match-1", "eval-1", owner_id="owner-1")

    assert evaluation.status == LLM_EVALUATION_DELETED
    db.commit.assert_called_once()


def test_delete_evaluation_hides_mismatched_match_id():
    db = Mock()
    service = _service(db=db)
    evaluation = _evaluation()
    evaluation.job_match_id = "different-match"
    service._get_match_for_owner = Mock(return_value=_match())
    service._get_evaluation_for_owner = Mock(return_value=evaluation)

    with pytest.raises(LookupError):
        service.delete_evaluation("match-1", "eval-1", owner_id="owner-1")

    db.commit.assert_not_called()


def test_get_match_for_owner_raises_when_no_visible_match():
    db = Mock()
    db.execute.return_value = _ScalarResult(value=None)
    service = _service(db=db)

    with pytest.raises(LookupError):
        service._get_match_for_owner("match-1", owner_id="owner-1", tenant_id="tenant-1")


def test_get_evaluation_for_owner_returns_visible_tenant_row():
    evaluation = _evaluation()
    db = Mock()
    db.execute.return_value = _ScalarResult(value=evaluation)
    service = _service(db=db)

    assert service._get_evaluation_for_owner(
        "eval-1",
        owner_id="owner-1",
        tenant_id="tenant-1",
    ) is evaluation


def test_get_evaluation_for_owner_raises_for_missing_row():
    db = Mock()
    db.execute.return_value = _ScalarResult(value=None)
    service = _service(db=db)

    with pytest.raises(LookupError):
        service._get_evaluation_for_owner("eval-1", owner_id="owner-1", tenant_id=None)


def test_find_active_cache_filters_global_scope():
    evaluation = _evaluation()
    db = Mock()
    db.execute.return_value = _ScalarResult(value=evaluation)
    service = _service(db=db)

    result = service._find_active_cache(
        owner_id="owner-1",
        tenant_id=None,
        resume_fingerprint="resume-fp",
        job_post_id="job-1",
        judge_config_hash="config-hash",
        evidence_hash="evidence-hash",
    )

    assert result is evaluation
    assert "tenant_id IS NULL" in str(db.execute.call_args.args[0])


def test_build_hash_payload_excludes_prior_deterministic_outputs():
    requirement = SimpleNamespace(text="T" * 1000, req_type="required", tags={}, id="req-1")
    match_requirement = SimpleNamespace(
        job_requirement_unit_id="req-1",
        requirement=requirement,
        req_type="required",
        evidence_text="E" * 1000,
        evidence_section="Experience",
        similarity_score="0.91",
        evidence_score="not-a-number",
        is_covered=True,
        created_at=datetime.now(timezone.utc),
        id="row-1",
    )
    db = Mock()
    db.query.return_value = _QueryChain([match_requirement])
    service = _service(db=db)
    match = SimpleNamespace(
        id="match-1",
        resume_fingerprint="resume-fp",
        job_post_id="job-1",
        job_post=SimpleNamespace(
            title="Senior Backend Engineer",
            company="Acme",
            location_text="Tokyo",
            is_remote=True,
            content_hash="content-hash",
            description_hash="description-hash",
            canonical_job_summary=None,
            description="D" * 17000,
            description_source="external_seed",
            description_completeness="full",
            description_warning_code=None,
            tenant_id=None,
        ),
        fit_score="81.25",
        preference_score=None,
        required_coverage=0.8,
        preferred_requirement_coverage=0.5,
        penalties=0,
        fit_components={"fit_confidence": 0.9, "secret": "drop"},
        preference_components={"preference_mode_used": "semantic_rerank", "secret": "drop"},
    )

    payload, hashes = service._build_hash_payload(match)

    assert payload["job"]["description"] == "D" * 17000
    assert payload["job"]["description_metadata"]["completeness"] == "full"
    assert payload["job"]["description_metadata"]["truncated_for_prompt"] is False
    assert payload["requirements"]["required"][0]["text"] == "T" * 1000
    assert "prior_deterministic_scores" not in payload
    assert "requirement_matches" not in payload
    serialized = str(payload)
    assert "81.25" not in serialized
    assert "semantic_rerank" not in serialized
    assert "E" * 100 not in serialized
    assert set(hashes) == {"judge_config_hash", "evidence_hash", "input_hash"}


def test_resume_summary_includes_full_nested_profile_skills_and_projects():
    service = _service()
    skills = [{"name": f"Skill {index}"} for index in range(30)]
    skills.append({"name": "TypeScript"})
    long_project_detail = "Built TypeScript Web Components with typed frontend state. " * 20
    resume = SimpleNamespace(
        extracted_data={
            "profile": {
                "summary": {"text": "Full-stack engineer."},
                "skills": {
                    "all": [{"name": "React.js"}, *skills],
                },
                "projects": [
                    {
                        "name": "Portfolio",
                        "description": long_project_detail,
                        "highlights": [long_project_detail],
                    }
                ],
            }
        },
        total_experience_years=3.5,
    )
    truncation = {"truncated": False, "fields": {}}

    payload = service._serialize_resume_summary(resume, truncation)

    assert "TypeScript" in str(payload)
    assert payload["profile"]["skills"]["all"][-1]["name"] == "TypeScript"
    assert payload["profile"]["projects"][0]["description"] == long_project_detail.strip()
    assert payload["total_experience_years"] == 3.5
    assert truncation == {"truncated": False, "fields": {}}


def test_judge_input_prioritizes_late_requirement_relevant_resume_evidence():
    config = _config()
    config.matching.llm_judge.evidence_units_max_count = 3
    service = _service(config)
    requirement = SimpleNamespace(
        text="Frontend development with TypeScript and React",
        req_type="required",
        tags={"technologies": ["TypeScript", "React"]},
        min_years=None,
        years_context=None,
    )
    generic_units = [
        SimpleNamespace(
            source_text=f"Generic backend evidence {index}",
            source_section="Experience",
            tags={},
            years_value=None,
            years_context=None,
            is_total_years_claim=False,
        )
        for index in range(12)
    ]
    type_script_unit = SimpleNamespace(
        source_text="Built TypeScript Web Components and React overlays.",
        source_section="Projects",
        tags={"technologies": ["TypeScript", "React"]},
        years_value=None,
        years_context=None,
        is_total_years_claim=False,
    )
    service._load_job_for_match = Mock(
        return_value=SimpleNamespace(
            title="Frontend Engineer",
            company="Acme",
            location_text="Tokyo",
            is_remote=False,
            description="Build product UI with TypeScript.",
            description_source="external_seed",
            description_completeness="full",
            description_warning_code=None,
            content_hash="content-hash",
            description_hash="description-hash",
        )
    )
    service._load_match_requirements = Mock(return_value=[])
    service._load_job_requirements = Mock(return_value=[requirement])
    service._load_resume = Mock(return_value=None)
    service._load_resume_evidence_units = Mock(return_value=[*generic_units, type_script_unit])

    judge_input = service.build_judge_input(_match(), owner_id="owner-1")

    serialized_evidence = str(judge_input.provider_payload["resume_evidence_units"])
    assert "TypeScript Web Components" in serialized_evidence
    assert len(judge_input.provider_payload["resume_evidence_units"]) == 3


def test_evidence_selector_promotes_explicit_requirement_term_hits():
    requirement = SimpleNamespace(text="TypeScript", tags={})
    units = [
        SimpleNamespace(
            source_text=f"Generic service evidence {index}",
            source_section="Experience",
            tags={},
            years_context=None,
        )
        for index in range(8)
    ]
    units.append(
        SimpleNamespace(
            source_text="Interactive Portfolio Website using TypeScript and Web Components.",
            source_section="Projects",
            tags={"technologies": ["TypeScript"]},
            years_context=None,
        )
    )

    selected = select_relevant_resume_evidence_units(
        units,
        [requirement],
        max_count=2,
    )

    assert "TypeScript" in str(selected)


def test_create_pending_evaluation_sets_cache_identity_fields():
    db = Mock()
    service = _service(db=db)
    match = _match(tenant_id="tenant-1")

    evaluation = service._create_pending_evaluation(
        match=match,
        owner_id="owner-1",
        tenant_id="tenant-1",
        hashes=_hashes(),
    )

    assert evaluation.status == LLM_EVALUATION_PENDING
    assert evaluation.resume_fingerprint == "resume-fp"
    assert evaluation.judge_config_hash == "config-hash"
    db.add.assert_called_once_with(evaluation)


def test_run_provider_persists_successful_structured_response():
    db = Mock()
    provider = Mock()
    provider.extract_structured_data.return_value = {
        "score": 87.345,
        "confidence": 0.87654,
        "verdict": "good",
        "summary": "Strong evidence." * 100,
        "reason_codes": ["Skills Match", "DROP-THIS!"],
        "requirement_verdicts": [
            {"requirement_id": "req-1", "verdict": "strong", "reason": "Covered"}
        ],
    }
    service = MatchLlmEvaluationService(db, config=_config(), llm_provider=provider)
    evaluation = _evaluation(status=LLM_EVALUATION_PENDING)

    service._run_provider(evaluation, {"safe": "payload"})

    assert evaluation.status == LLM_EVALUATION_SUCCEEDED
    assert evaluation.llm_score == 87.34
    assert evaluation.confidence == 0.8765
    assert evaluation.summary.endswith("...")
    assert evaluation.reason_codes == ["skills_match", "dropthis"]
    assert evaluation.requirement_verdicts[0]["requirement_id"] == "req-1"
    assert evaluation.error_code is None
    assert evaluation.retryable is False
    assert db.flush.call_count >= 2


def test_run_provider_marks_failure_retryable_without_raw_payload():
    db = Mock()
    provider = Mock()
    provider.extract_structured_data.side_effect = RuntimeError("provider down")
    service = MatchLlmEvaluationService(db, config=_config(), llm_provider=provider)
    evaluation = _evaluation(status=LLM_EVALUATION_PENDING)

    service._run_provider(evaluation, {"safe": "payload"})

    assert evaluation.status == LLM_EVALUATION_FAILED
    assert evaluation.llm_score is None
    assert evaluation.summary is None
    assert evaluation.error_code == "llm_judge_failed"
    assert evaluation.retryable is True
    assert db.flush.call_count >= 2


def test_run_provider_marks_oversized_provider_request_with_specific_error():
    db = Mock()
    provider = Mock()
    provider_error = RuntimeError("Request too large for model token limit")
    provider_error.status_code = 413
    provider.extract_structured_data.side_effect = provider_error
    service = MatchLlmEvaluationService(db, config=_config(), llm_provider=provider)
    evaluation = _evaluation(status=LLM_EVALUATION_PENDING)

    service._run_provider(evaluation, {"safe": "payload"})

    assert evaluation.status == LLM_EVALUATION_FAILED
    assert evaluation.error_code == "llm_judge_input_too_large"
    assert evaluation.retryable is True


def test_provider_is_built_lazily_and_cached():
    service = _service()
    provider = Mock()

    with pytest.MonkeyPatch.context() as monkeypatch:
        build = Mock(return_value=provider)
        runtime = Mock(return_value="runtime-config")
        monkeypatch.setattr("core.llm_evaluation.build_llm_provider", build)
        monkeypatch.setattr("core.llm_evaluation.runtime_llm_config_from_match_judge", runtime)

        assert service._provider() is provider
        assert service._provider() is provider

    runtime.assert_called_once_with(service.llm_config)
    build.assert_called_once_with("runtime-config")


def test_evaluate_selection_run_returns_zero_when_unavailable_or_disabled():
    service = _service(_config(judge_enabled=False))

    assert service.evaluate_selection_run(
        "selection-1",
        owner_id="owner-1",
        top_n=5,
    ) == {"attempted": 0, "reused": 0, "created": 0, "failed": 0}


def test_evaluate_selection_run_counts_reuse_create_failure_and_quota_stop():
    db = Mock()
    matches = [
        SimpleNamespace(id="match-1"),
        SimpleNamespace(id="match-2"),
        SimpleNamespace(id="match-3"),
        None,
        SimpleNamespace(id="match-4"),
    ]
    db.execute.return_value.scalars.return_value.all.return_value = [
        SimpleNamespace(job_match=match) for match in matches
    ]
    service = _service(db=db)
    service.generate_for_match = Mock(
        side_effect=[
            SimpleNamespace(reused=True),
            SimpleNamespace(reused=False),
            RuntimeError("transient"),
            LlmJudgeQuotaExceededError("quota"),
        ]
    )

    stats = service.evaluate_selection_run(
        "selection-1",
        owner_id="owner-1",
        tenant_id="tenant-1",
        top_n=99,
    )

    assert stats == {"attempted": 4, "reused": 1, "created": 1, "failed": 1}
    assert service.generate_for_match.call_count == 4


def test_model_has_partial_unique_active_cache_indexes():
    indexes = {index.name: index for index in LlmMatchEvaluation.__table__.indexes}

    tenant_index = indexes["uq_llm_eval_active_tenant_cache"]
    global_index = indexes["uq_llm_eval_active_global_cache"]

    assert tenant_index.unique is True
    assert global_index.unique is True
    assert tenant_index.dialect_options["postgresql"]["where"] is not None
    assert global_index.dialect_options["postgresql"]["where"] is not None
