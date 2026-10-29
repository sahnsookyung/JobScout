"""Contract tests for asymmetric embedding requests and safe generation changes."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from core.config_loader import LlmConfig
from core.embedding_contract import embedding_space_for_config
from core.llm.openai_service import OpenAIService
from core.llm.provider_factory import runtime_llm_config_from_etl
from core.matcher.requirement_matcher import RequirementMatcher
from core.scorer.persistence import save_match_to_db
from etl.resume.profiler import ResumeProfiler
from tests.unit.core.scorer.test_persistence import make_dto, make_repo
from tests.unit.etl.test_resume_profiler import make_resume_schema


def config(**overrides):
    return LlmConfig(embedding_input_type_enabled=True,
                     embedding_base_url="https://integrate.api.nvidia.com/v1",
                     embedding_model="nvidia/nemotron-3.5-embed-8b", **overrides)


@pytest.fixture
def service():
    with patch("core.llm.openai_service.OpenAI"):
        provider = OpenAIService(api_key="test", embedding_api_key="embedding-test",
            embedding_base_url="https://integrate.api.nvidia.com/v1",
            model_config={"embedding_input_type_enabled": True, "embedding_dimensions": 1024},
            retry_max_attempts=1)
    provider._embedding_rate_limiter = MagicMock()
    provider.embedding_client.embeddings.create.return_value = SimpleNamespace(
        data=[SimpleNamespace(index=0, embedding=[1.0]*1024)], usage=None)
    return provider


@pytest.mark.parametrize("method,role", [("generate_embedding", "passage"), ("generate_query_embedding", "query")])
def test_roles_dimensions_and_shared_rate_limit(service, method, role):
    assert len(getattr(service, method)("Synthetic example")) == 1024
    kwargs = service.embedding_client.embeddings.create.call_args.kwargs
    assert kwargs["extra_body"] == {"input_type": role, "truncate": "NONE"}
    assert kwargs["dimensions"] == 1024
    assert kwargs["encoding_format"] == "float"
    assert service._embedding_rate_limiter.wait_for_slot.call_args.kwargs == {
        "provider_name": "nvidia-extraction", "requests_per_minute": 10, "max_wait_seconds": 65}


def test_response_indices_preserve_source_association(service):
    service.embedding_client.embeddings.create.return_value.data = [
        SimpleNamespace(index=1, embedding=[2.0]*1024), SimpleNamespace(index=0, embedding=[1.0]*1024)]
    assert service.generate_query_embeddings_batch(["first", "second"]) == [[1.0]*1024, [2.0]*1024]


@pytest.mark.parametrize("rows", [[], [SimpleNamespace(index=1, embedding=[1.0]*1024)],
    [SimpleNamespace(index=0, embedding=[0.0]*1024)], [SimpleNamespace(index=0, embedding=[1.0]*512)],
    [SimpleNamespace(index=0, embedding=[float('nan')]*1024)],
    [SimpleNamespace(index=0, embedding=[float('inf')]*1024)]])
def test_invalid_responses_fail_before_persistence(service, rows):
    service.embedding_client.embeddings.create.return_value.data = rows
    with pytest.raises(ValueError):
        service.generate_query_embeddings_batch(["example"])


def test_batch_boundaries_are_bounded_and_keep_roles(service):
    service.embedding_client.embeddings.create.side_effect = lambda **kw: SimpleNamespace(
        data=[SimpleNamespace(index=i, embedding=[1.0]*1024) for i in range(len(kw['input']))], usage=None)
    assert len(service.generate_query_embeddings_batch(["example"]*65)) == 65
    calls = service.embedding_client.embeddings.create.call_args_list
    assert [len(call.kwargs['input']) for call in calls] == [32, 32, 1]
    assert all(call.kwargs['extra_body']['input_type'] == 'query' for call in calls)


@pytest.mark.parametrize("kwargs", [{"embedding_headers": {"Modal-Key": "test"}}, {"embedding_api_secret": "test"}])
def test_legacy_secrets_cannot_reach_new_endpoint(kwargs):
    with pytest.raises(ValueError, match="legacy provider"):
        OpenAIService(model_config={"embedding_input_type_enabled": True}, **kwargs)


def test_generation_changes_with_model_but_not_credentials_or_request_budget():
    original = config()
    space = embedding_space_for_config(original)
    assert space != "legacy"
    assert space == embedding_space_for_config(original.model_copy(update={
        "embedding_api_key": "rotated", "embedding_requests_per_minute": 20}))
    assert space != embedding_space_for_config(original.model_copy(update={"embedding_model": "nvidia/nemotron-3.5-embed-1b"}))
    assert embedding_space_for_config(LlmConfig()) == "legacy"
    with pytest.raises(ValueError, match="1024"):
        embedding_space_for_config(original.model_copy(update={"embedding_dimensions": 2048}))
    assert runtime_llm_config_from_etl(original).embedding_input_type_enabled is True


def test_profiler_uses_distinct_query_and_passage_vectors():
    provider = MagicMock(embedding_input_type_enabled=True)
    provider.generate_embeddings_batch.side_effect = lambda texts: [[1.0]*1024 for text in texts]
    provider.generate_query_embedding.return_value = [2.0]*1024
    sections = ResumeProfiler(provider).save_resume_section_embeddings("fp", make_resume_schema())
    summary = next(section for section in sections if section['section_type'] == 'summary')
    assert summary['embedding'] == [1.0]*1024
    assert summary['query_embedding'] == [2.0]*1024
    provider.generate_query_embedding.assert_called_once_with(summary['source_text'])


def test_requirement_from_previous_model_is_rejected(monkeypatch):
    monkeypatch.setattr("core.matcher.requirement_matcher.current_embedding_space", lambda: "new")
    req = SimpleNamespace(embedding_row=SimpleNamespace(embedding_space="legacy", embedding=[1.0]*1024))
    assert RequirementMatcher(0.5)._requirement_embedding(req) is None


def test_rescoring_preserves_notification_and_hidden_state(monkeypatch):
    monkeypatch.setattr("core.embedding_contract.current_embedding_space", lambda: "new")
    existing = SimpleNamespace(id="existing", tenant_id=None, job_content_hash="hash-1", status="active",
                               notified=True, is_hidden=True, embedding_space="legacy")
    repo = make_repo()
    repo.db.execute.return_value.scalar_one_or_none.return_value = existing
    result = save_match_to_db(make_dto(), repo)
    assert result.id == "existing"
    assert existing.embedding_space == "new"
    assert existing.notified is True
    assert existing.is_hidden is True
