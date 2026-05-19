import { apiClient } from './api';
import type {
    FetchSourcesResponse,
    PipelineTaskResponse,
    PipelineStatusResponse,
    ResumeEligibilityResponse,
    ResumePreflightResponse,
    ResumeStatusResponse,
    ResumeUploadResponse,
} from '@/types/api';

interface ResumeHashCheckResponse {
    exists: boolean;
    resume_hash: string;
}

interface FetchSourcesParams {
    search?: string;
    includeStatus?: boolean;
}

export const pipelineApi = {
    runMatching: () =>
        apiClient.post<PipelineTaskResponse>('/pipeline/run-matching'),

    getPipelineStatus: (taskId: string) =>
        apiClient.get<PipelineStatusResponse>(`/pipeline/status/${taskId}`),

    getActivePipeline: () =>
        apiClient.get<PipelineStatusResponse | null>('/pipeline/active'),

    getSources: (params: FetchSourcesParams = {}) =>
        apiClient.get<FetchSourcesResponse>('/pipeline/sources', {
            params: {
                search: params.search?.trim() || undefined,
                include_status: params.includeStatus || undefined,
            },
        }),

    getResumeEligibility: () =>
        apiClient.get<ResumeEligibilityResponse>('/pipeline/resume-eligibility'),

    preflightResume: (hash: string) =>
        apiClient.post<ResumePreflightResponse>('/pipeline/resume-preflight', { resume_hash: hash }),

    stopMatching: () =>
        apiClient.post<PipelineTaskResponse>('/pipeline/stop'),

    checkResumeHash: (hash: string) =>
        apiClient.post<ResumeHashCheckResponse>('/pipeline/check-resume-hash', { resume_hash: hash }),

    getResumeStatus: (taskId: string) =>
        apiClient.get<ResumeStatusResponse>(`/pipeline/resume-status/${taskId}`),

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

    selectResume: (resumeHash: string, originalFilename?: string) =>
        apiClient.post<ResumeUploadResponse>('/pipeline/select-resume', {
            resume_hash: resumeHash,
            original_filename: originalFilename,
        }),

    retryResume: (uploadId: string) =>
        apiClient.post<ResumeUploadResponse>('/pipeline/retry-resume', {
            upload_id: uploadId,
        }),
};
