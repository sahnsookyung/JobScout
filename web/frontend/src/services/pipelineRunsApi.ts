import { apiClient } from './api';
import type {
    LlmEvaluationQueueStatusResponse,
    PipelineRunDetailResponse,
    PipelineRunOperationResponse,
    PipelineRunsResponse,
} from '@/types/api';

export interface GetPipelineRunsParams {
    status?: string;
    run_type?: string;
    limit?: number;
    offset?: number;
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
};
