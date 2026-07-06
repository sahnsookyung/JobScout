import { apiClient } from './api';
import type {
    LlmEvaluationQueueOperationResponse,
    LlmEvaluationQueueStatusResponse,
    LlmProviderCanaryResponse,
    LlmProviderCircuitResetResponse,
    LlmProviderStatusResponse,
    PipelineRunDetailResponse,
    PipelineRunOperationResponse,
    PipelineRunsResponse,
} from '@/types/api';

export interface GetPipelineRunsParams {
    status?: string;
    run_type?: string;
    limit?: number;
    offset?: number;
    cursor?: string | null;
    page_mode?: 'offset' | 'cursor';
    view?: 'compact' | 'detail';
}

export const pipelineRunsApi = {
    getPipelineRuns: (params: GetPipelineRunsParams = {}) =>
        apiClient.get<PipelineRunsResponse>('/pipeline-runs', { params }),

    getPipelineRun: (runId: string) =>
        apiClient.get<PipelineRunDetailResponse>(`/pipeline-runs/${runId}`),

    cancelPipelineRun: (runId: string) =>
        apiClient.post<PipelineRunOperationResponse>(`/pipeline-runs/${runId}/cancel`),

    requeuePipelineRun: (runId: string) =>
        apiClient.post<PipelineRunOperationResponse>(`/pipeline-runs/${runId}/requeue`),

    retryPipelineRun: (runId: string) =>
        apiClient.post<PipelineRunOperationResponse>(`/pipeline-runs/${runId}/retry`),

    getLlmEvaluationQueueStatus: () =>
        apiClient.get<LlmEvaluationQueueStatusResponse>('/pipeline-runs/llm-evaluations/queue'),

    pauseLlmEvaluationQueue: (reason = 'manual', ttl_seconds?: number | null) =>
        apiClient.post<LlmEvaluationQueueOperationResponse>('/pipeline-runs/llm-evaluations/queue/pause', {
            reason,
            ttl_seconds,
        }),

    resumeLlmEvaluationQueue: () =>
        apiClient.post<LlmEvaluationQueueOperationResponse>('/pipeline-runs/llm-evaluations/queue/resume'),

    retryLlmEvaluationQueue: (limit = 100) =>
        apiClient.post<LlmEvaluationQueueOperationResponse>('/pipeline-runs/llm-evaluations/queue/retry', null, {
            params: { limit },
        }),

    getLlmProviderStatus: () =>
        apiClient.get<LlmProviderStatusResponse>('/pipeline-runs/llm-evaluations/providers'),

    runLlmProviderCanaries: () =>
        apiClient.post<LlmProviderCanaryResponse>('/pipeline-runs/llm-evaluations/providers/canary'),

    resetLlmProviderCircuit: (provider: string, model: string) =>
        apiClient.post<LlmProviderCircuitResetResponse>('/pipeline-runs/llm-evaluations/providers/circuit/reset', {
            provider,
            model,
        }),
};
