"""Tests for match-level LLM evaluation caching."""

from decimal import Decimal
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
    normalize_llm_score,
)
from core.llm.provider_chain import LLMProviderCandidate, LLMProviderChain
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
                    max_input_tokens=24000,
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
        analysis={},
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

    def extract_resume_data(self, text):
        raise NotImplementedError

    def extract_requirements_data(self, text):
        raise NotImplementedError

    def generate_embedding(self, text):
        raise NotImplementedError


class _FailingProvider(_CapturingProvider):
    def __init__(self, error):
        super().__init__()
        self.error = error

    def extract_structured_data(self, text, schema_spec, system_prompt=None, user_message=None):
        self.calls.append({"text": text})
        raise self.error


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


def test_provider_chain_availability_uses_configured_entries_without_legacy_key():
    config = _config(llm_enabled=False, base_url=None, model=None)
    config.matching.llm_judge.runtime.providers = [
        SimpleNamespace(
            name="groq",
            provider="groq",
            base_url="https://api.groq.com/openai/v1",
            api_key="groq-key",
            api_secret=None,
            headers=None,
            model="groq-model",
            structured_output_mode="auto",
            timeout_seconds=20,
            max_input_tokens=12000,
            api_key_env="GROQ_API_KEY",
        )
    ]
    service = _service(config)

    assert service.availability_status() == (True, "available")
    payload = service._judge_config_payload()
    assert payload["providers"][0]["name"] == "groq"
    assert payload["max_input_tokens"] == 12000
    assert "groq-key" not in str(payload)


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


def test_start_for_match_creates_pending_without_running_provider():
    db = Mock()
    service = _service(db=db)
    created = _evaluation(status=LLM_EVALUATION_PENDING)
    _wire_minimal_generation(service, existing=None, created=created)

    result = service.start_for_match("match-1", owner_id="owner-1")

    assert result.reused is False
    assert result.should_run is True
    assert result.evaluation is created
    assert result.provider_payload == {"safe": "payload"}
    assert result.truncation == {"truncated": False, "fields": {}}
    service._run_provider.assert_not_called()
    db.commit.assert_called_once()


def test_start_for_match_reuses_running_cache_without_queueing():
    service = _service()
    existing = _evaluation(status=LLM_EVALUATION_RUNNING)
    _wire_minimal_generation(service, existing=existing)

    result = service.start_for_match("match-1", owner_id="owner-1")

    assert result.reused is True
    assert result.should_run is False
    assert result.evaluation is existing
    service._check_daily_quota.assert_not_called()


def test_run_pending_evaluation_loads_row_and_runs_provider():
    db = Mock()
    evaluation = _evaluation(status=LLM_EVALUATION_PENDING)
    db.get.return_value = evaluation
    service = _service(db=db)
    service._run_provider = Mock()

    result = service.run_pending_evaluation(
        "00000000-0000-4000-8000-000000000301",
        {"safe": "payload"},
        truncation={"truncated": False, "fields": {}},
    )

    assert result is evaluation
    service._run_provider.assert_called_once_with(
        evaluation,
        {"safe": "payload"},
        truncation={"truncated": False, "fields": {}},
    )
    db.commit.assert_called_once()


def test_run_pending_evaluation_skips_invalid_missing_deleted_and_terminal_rows():
    service = _service(db=Mock())

    assert service.run_pending_evaluation("not-a-uuid", {"safe": "payload"}) is None

    db = Mock()
    db.get.return_value = None
    service = _service(db=db)
    assert service.run_pending_evaluation(
        "00000000-0000-4000-8000-000000000302",
        {"safe": "payload"},
    ) is None

    deleted = _evaluation(status=LLM_EVALUATION_PENDING)
    deleted.deleted_at = datetime.now(timezone.utc)
    db = Mock()
    db.get.return_value = deleted
    service = _service(db=db)
    assert service.run_pending_evaluation(
        "00000000-0000-4000-8000-000000000303",
        {"safe": "payload"},
    ) is deleted

    terminal = _evaluation(status=LLM_EVALUATION_SUCCEEDED)
    db = Mock()
    db.get.return_value = terminal
    service = _service(db=db)
    assert service.run_pending_evaluation(
        "00000000-0000-4000-8000-000000000304",
        {"safe": "payload"},
    ) is terminal


def test_resume_pending_evaluation_covers_guard_and_rebuild_paths():
    service = _service(db=Mock())
    assert service.resume_pending_evaluation("not-a-uuid") is None

    db = Mock()
    db.get.return_value = None
    service = _service(db=db)
    assert service.resume_pending_evaluation("00000000-0000-4000-8000-000000000305") is None

    deleted = _evaluation(status=LLM_EVALUATION_PENDING)
    deleted.deleted_at = datetime.now(timezone.utc)
    db = Mock()
    db.get.return_value = deleted
    service = _service(db=db)
    assert service.resume_pending_evaluation("00000000-0000-4000-8000-000000000306") is deleted

    terminal_failed = _evaluation(status=LLM_EVALUATION_FAILED)
    terminal_failed.retryable = False
    db = Mock()
    db.get.return_value = terminal_failed
    service = _service(db=db)
    assert service.resume_pending_evaluation("00000000-0000-4000-8000-000000000307") is terminal_failed

    succeeded = _evaluation(status=LLM_EVALUATION_SUCCEEDED)
    db = Mock()
    db.get.return_value = succeeded
    service = _service(db=db)
    assert service.resume_pending_evaluation("00000000-0000-4000-8000-000000000308") is succeeded

    missing_match = _evaluation(status=LLM_EVALUATION_PENDING)
    missing_match.job_match_id = None
    db = Mock()
    db.get.return_value = missing_match
    db.execute.return_value = _ScalarResult(value=None)
    service = _service(db=db)
    assert service.resume_pending_evaluation("00000000-0000-4000-8000-000000000309") is missing_match
    assert missing_match.status == LLM_EVALUATION_FAILED
    assert missing_match.error_code == "match_not_found"
    assert missing_match.retryable is False
    db.commit.assert_called_once()

    retryable = _evaluation(status=LLM_EVALUATION_FAILED)
    retryable.retryable = True
    match = _match()

    def get_model(model, _id):
        return retryable if model is LlmMatchEvaluation else match

    db = Mock()
    db.get.side_effect = get_model
    service = _service(db=db)
    judge_input = SimpleNamespace(
        provider_payload={"safe": "payload"},
        truncation={"truncated": False},
    )
    service.build_judge_input = Mock(return_value=judge_input)
    service.run_pending_evaluation = Mock(return_value=retryable)

    assert service.resume_pending_evaluation("00000000-0000-4000-8000-000000000310") is retryable
    assert retryable.status == LLM_EVALUATION_PENDING
    assert retryable.retryable is False
    assert retryable.error_code is None
    assert retryable.completed_at is None
    db.flush.assert_called_once()
    service.run_pending_evaluation.assert_called_once_with(
        retryable.id,
        {"safe": "payload"},
        truncation={"truncated": False},
    )

def test_retry_evaluation_resets_retryable_failed_row_without_tombstoning():
    db = Mock()
    service = _service(db=db)
    match = _match()
    failed = _evaluation(status=LLM_EVALUATION_FAILED)
    failed.retryable = True
    failed.error_code = "llm_judge_provider_timeout"
    failed.analysis = {"queue": {"enqueue_reason": "auto_top_n"}}
    service._get_match_for_owner = Mock(return_value=match)
    service._get_evaluation_for_owner = Mock(return_value=failed)

    result = service.retry_evaluation(
        match.id,
        failed.id,
        owner_id="owner-1",
    )

    assert result.evaluation is failed
    assert result.should_run is True
    assert failed.status == LLM_EVALUATION_PENDING
    assert failed.retryable is False
    assert failed.error_code is None
    assert failed.analysis["enqueue_reason"] == "retry_now"
    assert failed.analysis["queue"]["queue_state"] == "pending"
    db.commit.assert_called_once()

def test_retry_evaluation_rejects_terminal_failed_row():
    service = _service(db=Mock())
    terminal = _evaluation(status=LLM_EVALUATION_FAILED)
    terminal.retryable = False
    service._get_match_for_owner = Mock(return_value=_match())
    service._get_evaluation_for_owner = Mock(return_value=terminal)

    with pytest.raises(LlmJudgeConflictError):
        service.retry_evaluation("match-1", terminal.id, owner_id="owner-1")


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


def test_public_serialization_normalizes_existing_fractional_llm_scores():
    evaluation = _evaluation()
    evaluation.llm_score = Decimal("0.92")
    evaluation.verdict = "strong"

    public = evaluation_public_dict(evaluation)

    assert public["llm_score"] == 92.0


def test_public_serialization_includes_lifecycle_fields_without_raw_queue_payloads():
    evaluation = _evaluation(status=LLM_EVALUATION_FAILED)
    evaluation.retryable = True
    evaluation.analysis = {
        "enqueue_reason": "auto_top_n",
        "queue_job_id": "llm-evaluation-eval-1",
        "queue": {
            "enqueue_reason": "auto_top_n",
            "queue_job_id": "llm-evaluation-eval-1",
            "queue_state": "deferred",
            "next_retry_at": "2026-07-06T12:00:00+00:00",
            "retry_after_seconds": 120,
            "provider_status_message": "Provider temporarily paused.",
        },
        "provider_payload": "secret",
    }

    public = evaluation_public_dict(evaluation)

    assert public["queued_reason"] == "auto_top_n"
    assert public["queue_job_id"] == "llm-evaluation-eval-1"
    assert public["queue_state"] == "deferred"
    assert public["next_retry_at"] == "2026-07-06T12:00:00+00:00"
    assert public["retry_after_seconds"] == 120
    assert public["provider_status_message"] == "Provider temporarily paused."
    assert "provider_payload" not in public["analysis"]


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


def test_judge_input_compacts_to_runtime_token_budget():
    config = _config()
    config.matching.llm_judge.runtime.max_input_tokens = 2000
    service = _service(config)
    requirement = SimpleNamespace(
        text="Build TypeScript and React frontend experiences. " + ("Requirement detail. " * 200),
        req_type="required",
        tags={"technologies": ["TypeScript", "React"]},
        min_years=None,
        years_context=None,
    )
    resume = SimpleNamespace(
        extracted_data={
            "profile": {
                "summary": "Frontend engineer with TypeScript, React, testing, and UI systems. " * 400,
                "projects": [
                    {
                        "name": "Portfolio",
                        "description": "Built TypeScript interfaces and React dashboards. " * 300,
                    }
                ],
            }
        },
        total_experience_years=3.5,
    )
    evidence_unit = SimpleNamespace(
        source_text="Built TypeScript Web Components and React overlays. " * 300,
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
            description="Full job description mentioning TypeScript and React. " * 500,
            description_source="external_seed",
            description_completeness="full",
            description_warning_code=None,
            content_hash="content-hash",
            description_hash="description-hash",
        )
    )
    service._load_match_requirements = Mock(return_value=[])
    service._load_job_requirements = Mock(return_value=[requirement])
    service._load_resume = Mock(return_value=resume)
    service._load_resume_evidence_units = Mock(return_value=[evidence_unit])

    judge_input = service.build_judge_input(_match(), owner_id="owner-1")
    token_budget = judge_input.provider_payload["input_metadata"]["token_budget"]

    assert service._estimate_judge_prompt_tokens(judge_input.provider_payload) <= 2000
    assert judge_input.truncation["truncated"] is True
    assert judge_input.truncation["fields"]["llm_judge.prompt_token_budget"]["truncated"] is True
    assert token_budget["max_input_tokens"] == 2000
    assert token_budget["compacted"] is True
    assert token_budget["within_budget"] is True
    assert any(
        field.get("reason") == "runtime_token_budget"
        for field in judge_input.truncation["fields"].values()
        if isinstance(field, dict)
    )


def test_judge_config_payload_includes_runtime_token_budget():
    config = _config()
    config.matching.llm_judge.runtime.max_input_tokens = 12345
    service = _service(config)

    payload = service._judge_config_payload()

    assert payload["max_input_tokens"] == 12345


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


def test_run_provider_normalizes_fractional_score_and_orders_requirement_verdicts():
    db = Mock()
    provider = Mock()
    provider.extract_structured_data.return_value = {
        "score": 0.92,
        "confidence": 0.95,
        "verdict": "strong",
        "summary": "Strong evidence.",
        "reason_codes": [],
        "requirement_verdicts": [
            {"requirement_id": "req_3", "verdict": "partial", "reason": "Third"},
            {"requirement_id": "req_1", "verdict": "strong", "reason": "First"},
            {"requirement_id": "req_2", "verdict": "missing", "reason": "Second"},
        ],
    }
    service = MatchLlmEvaluationService(db, config=_config(), llm_provider=provider)
    evaluation = _evaluation(status=LLM_EVALUATION_PENDING)

    service._run_provider(evaluation, {"safe": "payload"})

    assert evaluation.status == LLM_EVALUATION_SUCCEEDED
    assert evaluation.llm_score == 92.0
    assert [
        item["requirement_id"] for item in evaluation.requirement_verdicts
    ] == ["req_1", "req_2", "req_3"]


def test_normalize_llm_score_handles_percent_fraction_and_ten_point_scales():
    assert normalize_llm_score(87.345, "good") == 87.34
    assert normalize_llm_score(0.95, "strong") == 95.0
    assert normalize_llm_score(9.5, "strong") == 95.0
    assert normalize_llm_score(1.0, "mismatch") == 1.0
    assert normalize_llm_score(3.0, "mismatch") == 3.0


def test_run_provider_marks_unknown_failure_terminal_without_raw_payload():
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
    assert evaluation.retryable is False
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
    assert evaluation.retryable is False


def test_run_provider_marks_token_quota_failure_with_specific_error():
    db = Mock()
    provider = Mock()
    provider_error = RuntimeError("Tokens per minute limit exceeded")
    provider_error.status_code = 429
    provider.extract_structured_data.side_effect = provider_error
    service = MatchLlmEvaluationService(db, config=_config(), llm_provider=provider)
    evaluation = _evaluation(status=LLM_EVALUATION_PENDING)

    service._run_provider(evaluation, {"safe": "payload"})

    assert evaluation.status == LLM_EVALUATION_FAILED
    assert evaluation.error_code == "llm_judge_token_quota_exceeded"
    assert evaluation.retryable is True


def test_run_provider_records_successful_fallback_provider_attempts():
    db = Mock()
    timeout = TimeoutError("timed out")
    primary = _FailingProvider(timeout)
    fallback = _CapturingProvider()
    provider = LLMProviderChain(
        [
            LLMProviderCandidate(
                name="nvidia",
                provider_name="nvidia",
                model="nvidia-model",
                provider=primary,
            ),
            LLMProviderCandidate(
                name="groq",
                provider_name="groq",
                model="groq-model",
                provider=fallback,
            ),
        ]
    )
    service = MatchLlmEvaluationService(db, config=_config(), llm_provider=provider)
    evaluation = _evaluation(status=LLM_EVALUATION_PENDING)

    service._run_provider(evaluation, {"safe": "payload"})

    assert evaluation.status == LLM_EVALUATION_SUCCEEDED
    assert evaluation.provider == "groq"
    assert evaluation.model == "groq-model"
    attempts = evaluation.analysis["provider_attempts"]
    assert [attempt["status"] for attempt in attempts] == ["failed", "succeeded"]
    assert attempts[0]["error_category"] == "timeout"
    assert "payload" not in str(attempts)


def test_run_provider_does_not_retry_terminal_provider_chain_failure():
    db = Mock()
    auth_error = RuntimeError("invalid api key")
    auth_error.status_code = 401
    primary = _FailingProvider(auth_error)
    fallback = _CapturingProvider()
    provider = LLMProviderChain(
        [
            LLMProviderCandidate(
                name="nvidia",
                provider_name="nvidia",
                model="nvidia-model",
                provider=primary,
            ),
            LLMProviderCandidate(
                name="groq",
                provider_name="groq",
                model="groq-model",
                provider=fallback,
            ),
        ]
    )
    service = MatchLlmEvaluationService(db, config=_config(), llm_provider=provider)
    evaluation = _evaluation(status=LLM_EVALUATION_PENDING)

    service._run_provider(evaluation, {"safe": "payload"})

    assert evaluation.status == LLM_EVALUATION_FAILED
    assert evaluation.error_code == "llm_judge_invalid_credentials"
    assert evaluation.retryable is False
    assert len(evaluation.analysis["provider_attempts"]) == 1
    assert fallback.calls == []


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
    ) == {"attempted": 0, "reused": 0, "created": 0, "enqueued": 0, "failed": 0}


def test_evaluate_selection_run_counts_reuse_create_enqueue_failure_and_quota_stop(monkeypatch):
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
    service.start_for_match = Mock(
        side_effect=[
            SimpleNamespace(reused=True, should_run=False),
            SimpleNamespace(
                reused=False,
                should_run=True,
                evaluation=SimpleNamespace(id="evaluation-2"),
                provider_payload={"job": "payload"},
                truncation={"truncated": False},
            ),
            RuntimeError("transient"),
            LlmJudgeQuotaExceededError("quota"),
        ]
    )
    enqueue = Mock()
    monkeypatch.setattr("core.llm_evaluation_queue.enqueue_llm_evaluation", enqueue)

    stats = service.evaluate_selection_run(
        "selection-1",
        owner_id="owner-1",
        tenant_id="tenant-1",
        top_n=99,
    )

    assert stats == {"attempted": 4, "reused": 1, "created": 1, "enqueued": 1, "failed": 1}
    assert service.start_for_match.call_count == 4
    enqueue.assert_called_once_with(
        "evaluation-2",
        provider_payload={"job": "payload"},
        truncation={"truncated": False},
        enqueue_reason="auto_top_n",
    )


def test_model_has_partial_unique_active_cache_indexes():
    indexes = {index.name: index for index in LlmMatchEvaluation.__table__.indexes}

    tenant_index = indexes["uq_llm_eval_active_tenant_cache"]
    global_index = indexes["uq_llm_eval_active_global_cache"]

    assert tenant_index.unique is True
    assert global_index.unique is True
    assert tenant_index.dialect_options["postgresql"]["where"] is not None
    assert global_index.dialect_options["postgresql"]["where"] is not None


class _Query:
    def __init__(self, *, rows=None, first=None, get=None, fail=False):
        self.rows = rows or []
        self.first_value = first
        self.get_value = get
        self.fail = fail

    def filter(self, *args, **kwargs):
        if self.fail:
            raise RuntimeError("query failed")
        return self

    def options(self, *args, **kwargs):
        return self

    def order_by(self, *args, **kwargs):
        return self

    def limit(self, *args, **kwargs):
        return self

    def all(self):
        return self.rows

    def first(self):
        return self.first_value

    def get(self, _id):
        return self.get_value


def _truncation():
    return {"truncated": False, "fields": {}}


def test_load_helpers_cover_query_success_and_fallback_paths():
    job = SimpleNamespace(id="job-1")
    service = _service(db=Mock())

    assert service._load_job_for_match(SimpleNamespace(job_post=job)) is job
    assert service._load_job_for_match(SimpleNamespace(job_post_id=None, job_id=None)) is None

    service.db.query.return_value = _Query(get=job)
    assert service._load_job_for_match(SimpleNamespace(job_post_id="job-1")) is job

    service.db.query.return_value = _Query(fail=True)
    assert service._load_job_for_match(SimpleNamespace(job_post_id="job-1")) is None

    requirements = [
        SimpleNamespace(id="b", ordinal=None),
        SimpleNamespace(id="a", ordinal=2),
        SimpleNamespace(id="c", ordinal=1),
    ]
    sorted_requirements = service._load_job_requirements(
        SimpleNamespace(id="job-1", requirements=requirements)
    )
    assert [item.id for item in sorted_requirements] == ["c", "a", "b"]
    assert service._load_job_requirements(None) == []
    assert service._load_job_requirements(SimpleNamespace(id=None, requirements=[])) == []

    service.db.query.return_value = _Query(rows=[SimpleNamespace(id="req-1")])
    assert len(service._load_job_requirements(SimpleNamespace(id="job-1", requirements=[]))) == 1
    assert len(service._load_match_requirements(SimpleNamespace(id="match-1"))) == 1

    service.db.query.return_value = _Query(first=SimpleNamespace(id="resume-1"))
    assert service._load_resume("owner-1", "fp").id == "resume-1"
    assert service._load_resume(None, "fp") is None
    assert service._load_resume("owner-1", None) is None

    service.db.query.return_value = _Query(rows=[SimpleNamespace(id="ev-1")])
    assert len(service._load_resume_evidence_units("owner-1", "fp")) == 1
    assert service._load_resume_evidence_units(None, "fp") == []

    service.db.query.return_value = _Query(fail=True)
    assert service._load_job_requirements(SimpleNamespace(id="job-1", requirements=[])) == []
    assert service._load_match_requirements(SimpleNamespace(id="match-1")) == []
    assert service._load_resume("owner-1", "fp") is None
    assert service._load_resume_evidence_units("owner-1", "fp") == []


def test_judge_input_serializers_record_truncation_metadata():
    service = _service()
    service._judge_limit = Mock(
        side_effect=lambda name, default: {
            "requirements_max_count": 1,
            "requirement_text_max_chars": 8,
            "resume_summary_max_chars": 20,
            "evidence_units_max_count": 1,
            "evidence_unit_max_chars": 8,
        }.get(name, default)
    )

    truncation = _truncation()
    grouped = service._serialize_requirements(
        [
            SimpleNamespace(
                req_type="required",
                text="x" * 20,
                tags={"k": ["a", object(), 3]},
                min_years=Decimal("2.5"),
                years_context="context",
            ),
            SimpleNamespace(req_type="unknown", text="ignored"),
        ],
        truncation,
    )

    assert truncation["truncated"] is True
    assert truncation["fields"]["requirements"]["original_count"] == 2
    assert grouped["required"][0]["text"].endswith("...")
    assert grouped["required"][0]["tags"] == {"k": ["a", 3]}
    assert grouped["required"][0]["min_years"] == 2.5

    resume = SimpleNamespace(
        extracted_data={
            "skills": ["Python", object()],
            "embedding": "secret",
            "nested": {"resume_fingerprint": "secret", "ok": "\x00 kept "},
        },
        total_experience_years=Decimal("7.5"),
    )
    summary = service._serialize_resume_summary(resume, _truncation())
    assert "summary_text" in summary
    public_summary = service._public_resume_summary(resume.extracted_data)
    assert "embedding" not in public_summary
    assert public_summary["nested"] == {"ok": "kept"}
    assert service._serialize_resume_summary(None, _truncation()) == {}

    evidence_truncation = _truncation()
    evidence = service._serialize_resume_evidence_units(
        [
            SimpleNamespace(
                source_section="experience",
                source_text="y" * 20,
                tags={"years": [1, object(), "two"]},
                years_value="3",
                years_context="context",
                is_total_years_claim=True,
            ),
            SimpleNamespace(source_section="ignored", source_text="ignored"),
        ],
        evidence_truncation,
    )
    assert len(evidence) == 1
    assert evidence_truncation["fields"]["resume_evidence_units"]["original_count"] == 2
    assert evidence[0]["source_text"].endswith("...")


def test_json_and_scalar_helpers_cover_edge_cases():
    service = _service()
    truncation = _truncation()

    sentinel = object()
    assert service._public_resume_value([" ok ", sentinel])[0] == "ok"
    assert service._public_resume_value({"embedding": "secret", "keep": sentinel}) == {
        "keep": str(sentinel)
    }
    capped = service._cap_json_payload({"text": "z" * 50}, 12, truncation, "payload")
    assert capped["summary_text"].endswith("...")
    assert truncation["fields"]["payload"]["included_chars"] == 12
    assert service._cap_json_payload({"ok": True}, 1_000, _truncation(), "payload") == {"ok": True}
    assert service._safe_json_object(None) == {}
    assert service._safe_json_object({"nested": {"ignored": True}, "ok": True}) == {"ok": True}
    assert service._truncate("abcdef", 3) == "abc"
    assert service._float(Decimal("1.25")) == 1.25
    assert service._float("not-a-number") is None


def test_reuse_error_classification_and_public_dict_edges():
    service = _service()

    assert service._is_reusable(_evaluation(status=LLM_EVALUATION_PENDING)) is False
    assert service._is_reusable(_evaluation(completed_at=None)) is True

    too_large = RuntimeError("request too large")
    too_large.status_code = 400
    assert service._provider_error_code(too_large) == "llm_judge_input_too_large"
    quota = RuntimeError("token_quota_exceeded")
    assert service._provider_error_code(quota) == "llm_judge_token_quota_exceeded"
    assert service._hash_json({"b": 2, "a": 1}) == service._hash_json({"a": 1, "b": 2})
    assert service._safe_reason_codes(["Skills Match", "bad-code!", ""]) == [
        "skills_match",
        "badcode",
    ]

    evaluation = _evaluation()
    evaluation.analysis = {
        "provider_payload": "secret",
        "notes": ["x" * 2100 for _ in range(2)],
        "nested": {"raw_payload": "secret", "visible": "ok"},
    }
    public = evaluation_public_dict(evaluation)
    assert "provider_payload" not in public["analysis"]
    assert public["analysis"]["nested"] == {"visible": "ok"}
    assert public["analysis"]["notes"][0].endswith("...")


def test_evaluation_effectiveness_covers_ignored_stale_and_current_metadata():
    service = _service()
    match = _match()

    deleted = _evaluation()
    deleted.deleted_at = datetime.now(timezone.utc)
    assert service.evaluation_effectiveness(
        match,
        deleted,
        owner_id="owner-1",
    )["ignored_for_rerank_reason"] == "deleted"

    missing_score = _evaluation()
    missing_score.llm_score = None
    assert service.evaluation_effectiveness(
        match,
        missing_score,
        owner_id="owner-1",
    )["ignored_for_rerank_reason"] == "missing_llm_score"

    stale_job = _evaluation()
    stale_match = _match()
    stale_match.job_content_hash = "old-hash"
    service._load_job_for_match = Mock(return_value=SimpleNamespace(content_hash="new-hash"))
    assert service.evaluation_effectiveness(
        stale_match,
        stale_job,
        owner_id="owner-1",
    )["ignored_for_rerank_reason"] == "stale_job_content"

    service._load_job_for_match = Mock(return_value=SimpleNamespace(content_hash="content-hash"))
    service.build_judge_input = Mock(side_effect=RuntimeError("cannot rebuild"))
    assert service.evaluation_effectiveness(
        _match(),
        _evaluation(),
        owner_id="owner-1",
    )["ignored_for_rerank_reason"] == "current_input_unavailable"

    stale_input = _evaluation()
    service.build_judge_input = Mock(
        return_value=SimpleNamespace(
            hashes={
                "judge_config_hash": stale_input.judge_config_hash,
                "input_hash": stale_input.input_hash,
                "evidence_hash": "changed-evidence",
            },
            truncation={"truncated": True},
        )
    )
    stale_result = service.evaluation_effectiveness(
        _match(),
        stale_input,
        owner_id="owner-1",
    )
    assert stale_result["ignored_for_rerank_reason"] == "stale_evidence_hash"
    assert stale_result["input_truncation"] == {"truncated": True}

    current = _evaluation()
    service.build_judge_input = Mock(
        return_value=SimpleNamespace(
            hashes={
                "judge_config_hash": current.judge_config_hash,
                "input_hash": current.input_hash,
                "evidence_hash": current.evidence_hash,
            },
            truncation={"truncated": False},
        )
    )
    current_result = service.evaluation_effectiveness(
        _match(),
        current,
        owner_id="owner-1",
    )
    assert current_result == {
        "effective_for_rerank": True,
        "ignored_for_rerank_reason": None,
        "stale_status": "current",
        "input_truncation": {"truncated": False},
    }
