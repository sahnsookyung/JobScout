import { apiClient } from './api';
import type { PipelineTaskResponse, PipelineStatusResponse } from '@/types/api';

export const pipelineApi = {
    runMatching: () =>
        apiClient.post<PipelineTaskResponse>('/pipeline/run-matching'),

    getPipelineStatus: (taskId: string) =>
        apiClient.get<PipelineStatusResponse>(`/pipeline/status/${taskId}`),

    getActivePipeline: () =>
        apiClient.get<PipelineStatusResponse | null>('/pipeline/active'),

    stopMatching: () =>
        apiClient.post<PipelineTaskResponse>('/pipeline/stop'),
};
