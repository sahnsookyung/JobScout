"""Behavioral coverage for NVIDIA-first job extraction and protected defaults."""

from unittest.mock import Mock, patch

import httpx
import openai
import pytest

from core.config_loader import JobExtractionRoutingConfig, LlmConfig, load_config
from core.llm.global_budget import GlobalLlmBudgetExceeded, GlobalLlmBudgetUnavailable
from core.llm.interfaces import LLMProvider
from core.llm.job_extraction_provider import JobExtractionProvider, build_job_extraction_provider
from core.llm.openai_service import OpenAIService
from core.llm.provider_chain import LLMProviderCandidate, LLMProviderChain, LLMProviderChainError
from core.llm.provider_rate_limiter import ProviderCircuitBreaker, ProviderCircuitOpen
from core.llm.schema_models import JobExtraction
from tests.unit.core.llm.test_provider_rate_limiter import _FakeCircuitRedis


@pytest.fixture
def extraction():
    return {
        name: [] if name in {'tech_stack', 'requirements', 'benefits'} else
        'Test role' if name in {'thought_process', 'job_summary'} else None
        for name in JobExtraction.model_fields
    }


def make_chain(primary, fallback, *, circuit=None):
    return LLMProviderChain([
        LLMProviderCandidate('nvidia-extraction', 'nvidia', 'nvidia-model', primary, fallback_on_rate_limit=True),
        LLMProviderCandidate('etl-fallback', 'cerebras', 'cerebras-model', fallback),
    ], circuit_breaker=circuit or Mock(spec=ProviderCircuitBreaker))


def test_nvidia_success_does_not_call_cerebras(extraction):
    primary, fallback, default = (Mock(spec=LLMProvider) for _ in range(3))
    primary.extract_requirements_data.return_value = extraction
    service = JobExtractionProvider(default, make_chain(primary, fallback))
    assert service.extract_requirements_data('Public job') == extraction
    primary.extract_requirements_data.assert_called_once_with('Public job')
    fallback.extract_requirements_data.assert_not_called()
    default.extract_requirements_data.assert_not_called()


@pytest.mark.parametrize('primary_fails', [False, True])
def test_extraction_metadata_reports_successful_model(extraction, primary_fails):
    primary, fallback = Mock(spec=LLMProvider), Mock(spec=LLMProvider)
    primary.extract_requirements_data.return_value = extraction
    fallback.extract_requirements_data.return_value = extraction
    if primary_fails:
        primary.extract_requirements_data.side_effect = TimeoutError('timed out')
    default = Mock(extraction_model='previous-model')
    service = JobExtractionProvider(default, make_chain(primary, fallback))
    assert service.extraction_model == 'previous-model'
    service.extract_requirements_data('Public job')
    assert service.extraction_model == ('cerebras-model' if primary_fails else 'nvidia-model')


@pytest.mark.parametrize('status', [402, 429, 500, 503])
def test_provider_failures_use_fallback(status, extraction):
    primary, fallback = Mock(spec=LLMProvider), Mock(spec=LLMProvider)
    error = RuntimeError('provider failure')
    error.status_code = status
    primary.extract_requirements_data.side_effect = error
    fallback.extract_requirements_data.return_value = extraction
    circuit = Mock(spec=ProviderCircuitBreaker)
    service = JobExtractionProvider(Mock(), make_chain(primary, fallback, circuit=circuit))
    assert service.extract_requirements_data('Public job') == extraction
    if status == 402:
        circuit.defer.assert_called_once_with('nvidia-extraction', model='nvidia-model', seconds=3600)


@pytest.mark.parametrize('error', [GlobalLlmBudgetExceeded('requests', reset_at=12345), GlobalLlmBudgetUnavailable('redis')])
def test_global_budget_stops_without_fallback_and_preserves_reset(error):
    primary, fallback = Mock(spec=LLMProvider), Mock(spec=LLMProvider)
    primary.extract_requirements_data.side_effect = error
    with pytest.raises(type(error)) as caught:
        make_chain(primary, fallback).extract_requirements_data('Public job')
    assert caught.value is error
    fallback.extract_requirements_data.assert_not_called()


def test_budget_exhausted_between_providers_still_propagates():
    primary, fallback = Mock(spec=LLMProvider), Mock(spec=LLMProvider)
    primary.extract_requirements_data.side_effect = TimeoutError('timed out')
    fallback.extract_requirements_data.side_effect = GlobalLlmBudgetExceeded('requests', reset_at=12345)
    with pytest.raises(GlobalLlmBudgetExceeded) as caught:
        make_chain(primary, fallback).extract_requirements_data('Public job')
    assert caught.value.reset_at == 12345


def test_resumes_structured_calls_and_batch_embeddings_keep_original_provider():
    default, chain = Mock(spec=LLMProvider), Mock(spec=LLMProvider)
    service = JobExtractionProvider(default, chain)
    service.extract_resume_data('private resume')
    service.extract_structured_data('private data', {}, user_message='instructions')
    service.generate_embedding('text')
    service.generate_embeddings_batch(['one', 'two'])
    default.extract_resume_data.assert_called_once_with('private resume')
    default.extract_structured_data.assert_called_once_with('private data', {}, system_prompt=None, user_message='instructions')
    default.generate_embeddings_batch.assert_called_once_with(['one', 'two'])
    assert not chain.mock_calls


def test_quota_cooldown_survives_new_circuit_instance(extraction):
    redis = _FakeCircuitRedis()
    circuit = ProviderCircuitBreaker(client_factory=lambda: redis)
    circuit.defer('etl-fallback', model='cerebras-model', seconds=3600)
    restarted = ProviderCircuitBreaker(client_factory=lambda: redis)
    with pytest.raises(ProviderCircuitOpen) as caught:
        restarted.assert_available('etl-fallback', model='cerebras-model')
    assert caught.value.retry_after_seconds == 3600
    restarted.assert_available('nvidia-extraction', model='nvidia-model')


def test_routing_disabled_returns_original():
    default = Mock(spec=LLMProvider)
    assert build_job_extraction_provider(LlmConfig(), default) is default


def test_enabled_routing_requires_key():
    with pytest.raises(ValueError, match='NVIDIA_EXTRACTION_API_KEY'):
        build_job_extraction_provider(LlmConfig(job_routing={'enabled': True}), Mock())


def test_dedicated_key_overrides_general_nvidia_key(monkeypatch):
    monkeypatch.setenv('NVIDIA_API_KEY', 'general-test-secret')
    monkeypatch.setenv('NVIDIA_EXTRACTION_API_KEY', 'extraction-test-secret')
    monkeypatch.setenv('JOB_EXTRACTION_NVIDIA_FIRST', 'true')
    config = load_config().etl.llm.job_routing
    assert config.nvidia_api_key == 'extraction-test-secret'
    assert config.enabled
    assert 'extraction-test-secret' not in repr(config)
    assert 'nvidia_api_key' not in config.model_dump()


def test_builder_bounds_attempts_and_uses_nvidia_first():
    config = LlmConfig(api_key='cerebras-test', extraction_model='gpt-oss-120b', job_routing=JobExtractionRoutingConfig(enabled=True, nvidia_api_key='nvidia-test'))
    with patch('core.llm.job_extraction_provider.build_llm_provider') as build:
        provider = build_job_extraction_provider(config, Mock())
    first, second = [call.args[0] for call in build.call_args_list]
    assert first.base_url == 'https://integrate.api.nvidia.com/v1'
    assert first.api_key == 'nvidia-test'
    assert first.enable_thinking is False
    assert first.retry_max_attempts == second.retry_max_attempts == 1
    assert first.timeout_seconds == second.timeout_seconds == 60
    assert provider.chain._candidates[0].name == 'nvidia-extraction'
    assert provider.chain._candidates[0].rate_limit_max_wait_seconds == 0


def test_nvidia_sampling_reaches_api_without_changing_fallback(monkeypatch):
    monkeypatch.setenv('JOBSCOUT_CLOUD_GLOBAL_LLM_BUDGET_ENABLED', 'false')
    config = LlmConfig(
        api_key='cerebras-test', extraction_model='gpt-oss-120b',
        extraction_temperature=0.2,
        job_routing=JobExtractionRoutingConfig(enabled=True, nvidia_api_key='nvidia-test'),
    )
    provider = build_job_extraction_provider(config, Mock())
    primary, fallback = [candidate.provider for candidate in provider.chain._candidates]
    try:
        for service in (primary, fallback):
            with patch.object(service.client.chat.completions, 'create') as create:
                service._create_chat_completion([{'role': 'user', 'content': 'job'}], None)
                kwargs = create.call_args.kwargs
                assert kwargs['max_tokens'] == 4096
                if service is primary:
                    assert kwargs['temperature'] == 1.0
                    assert kwargs['top_p'] == 0.95
                    assert kwargs['extra_body']['chat_template_kwargs']['enable_thinking'] is False
                else:
                    assert kwargs['temperature'] == 0.2
                    assert 'top_p' not in kwargs
    finally:
        for service in (primary, fallback):
            service.client.close()


def test_single_attempt_and_thinking_disabled(monkeypatch):
    monkeypatch.setenv('JOBSCOUT_CLOUD_GLOBAL_LLM_BUDGET_ENABLED', 'false')
    service = OpenAIService(api_key='test', retry_max_attempts=1, enable_thinking=False)
    request = httpx.Request('POST', 'https://example.com/v1/chat/completions')
    error = openai.APITimeoutError(request=request)
    with patch.object(service.client.chat.completions, 'create', side_effect=error) as create:
        with pytest.raises(openai.APITimeoutError):
            service.extract_structured_data('job', {'type': 'object', 'properties': {}})
        assert create.call_count == 1
        assert create.call_args.kwargs['extra_body'] == {'chat_template_kwargs': {'enable_thinking': False}}
    service.client.close()


def test_auth_failure_does_not_hide_bad_key_with_fallback():
    primary, fallback = Mock(spec=LLMProvider), Mock(spec=LLMProvider)
    error = RuntimeError('unauthorized')
    error.status_code = 401
    primary.extract_requirements_data.side_effect = error
    with pytest.raises(LLMProviderChainError) as caught:
        make_chain(primary, fallback).extract_requirements_data('job')
    assert caught.value.error_category == 'invalid_auth'
    fallback.extract_requirements_data.assert_not_called()


def test_invalid_output_can_fallback_without_persisting_invalid_json(extraction):
    primary, fallback = Mock(spec=LLMProvider), Mock(spec=LLMProvider)
    primary.extract_requirements_data.return_value = {'wrong_schema': True}
    fallback.extract_requirements_data.return_value = extraction
    chain = LLMProviderChain([
        LLMProviderCandidate('nvidia-extraction', 'nvidia', 'model', primary, fallback_on_invalid_output=True),
        LLMProviderCandidate('etl-fallback', 'cerebras', 'model', fallback),
    ], circuit_breaker=Mock(spec=ProviderCircuitBreaker))
    assert JobExtractionProvider(Mock(), chain).extract_requirements_data('job') == extraction
    assert chain.last_attempts[0]['status'] == 'failed'
    assert chain.last_attempts[0]['error_category'] == 'schema_error'
    assert chain.last_success['provider'] == 'etl-fallback'


def test_daily_quota_opens_circuit_and_next_job_skips_failed_provider(extraction):
    primary, fallback = Mock(spec=LLMProvider), Mock(spec=LLMProvider)
    primary.extract_requirements_data.side_effect = TimeoutError('timed out')
    error = RuntimeError('daily quota exhausted')
    error.status_code = 429
    fallback.extract_requirements_data.side_effect = error
    redis = _FakeCircuitRedis()
    first = make_chain(primary, fallback, circuit=ProviderCircuitBreaker(client_factory=lambda: redis))
    with pytest.raises(LLMProviderChainError):
        first.extract_requirements_data('job')
    restarted = make_chain(primary, fallback, circuit=ProviderCircuitBreaker(client_factory=lambda: redis))
    with pytest.raises(LLMProviderChainError):
        restarted.extract_requirements_data('another job')
    assert fallback.extract_requirements_data.call_count == 1
    assert restarted.last_attempts[-1]['status'] == 'circuit_open'


def test_exhausted_chain_uses_durable_backoff_instead_of_immediate_retry():
    import threading
    from services.base.extraction import _on_extraction_error
    error = LLMProviderChainError('failed', error_category='provider_quota', attempts=[], retryable=True)
    stop = Mock(spec=threading.Event)
    with patch('services.base.extraction._mark_job_retryable') as mark:
        assert _on_extraction_error(error, 1, None, 0, [30, 60, 120], 30, stop)
    stop.wait.assert_not_called()
    mark.assert_called_once_with(1, 'LLMProviderChainError', 'provider_quota')


def test_job_prompt_override_does_not_change_default_resume_prompt():
    service = OpenAIService(api_key='test', requirements_system_prompt='dedicated job prompt')
    with patch.object(service, 'extract_structured_data', return_value={}) as extract:
        service.extract_requirements_data('job')
        assert extract.call_args.kwargs['system_prompt'] == 'dedicated job prompt'
    service.client.close()
