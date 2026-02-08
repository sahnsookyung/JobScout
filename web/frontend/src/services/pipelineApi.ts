import { apiClient } from './api';
import type { PipelineTaskResponse, PipelineStatusResponse } from '@/types/api';

export const pipelineApi = {
    runMatching: () =>
        apiClient.post<PipelineTaskResponse>('/pipeline/run-matching'),

    getStatus: (taskId: string) =>
        apiClient.get<PipelineStatusResponse>(`/pipeline/status/${taskId}`),

    getActiveTask: () =>
        apiClient.get<PipelineStatusResponse | null>('/pipeline/active'),

    stopMatching: () =>
        apiClient.post<PipelineTaskResponse>('/pipeline/stop'),
};
