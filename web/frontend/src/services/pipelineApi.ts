import { apiClient } from './api';
import type { PipelineTaskResponse, PipelineStatusResponse } from '@/types/api';

interface ResumeHashCheckResponse {
    exists: boolean;
    resume_hash: string;
}

interface ResumeUploadResponse {
    success: boolean;
    resume_hash: string;
    message: string;
}

export const pipelineApi = {
    runMatching: () =>
        apiClient.post<PipelineTaskResponse>('/pipeline/run-matching'),

    getPipelineStatus: (taskId: string) =>
        apiClient.get<PipelineStatusResponse>(`/pipeline/status/${taskId}`),

    getActivePipeline: () =>
        apiClient.get<PipelineStatusResponse | null>('/pipeline/active'),

    stopMatching: () =>
        apiClient.post<PipelineTaskResponse>('/pipeline/stop'),

    checkResumeHash: (hash: string) =>
        apiClient.post<ResumeHashCheckResponse>('/pipeline/check-resume-hash', { resume_hash: hash }),

    uploadResume: (file: File, resumeHash?: string) => {
        const formData = new FormData();
        formData.append('file', file);
        if (resumeHash) {
            formData.append('resume_hash', resumeHash);
        }
        return apiClient.post<ResumeUploadResponse>('/pipeline/upload-resume', formData, {
            headers: { 'Content-Type': 'multipart/form-data' }
        });
    },
};
