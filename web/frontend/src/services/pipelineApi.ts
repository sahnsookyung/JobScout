import { apiClient } from './api';
import type {
    AtsSourceCreateRequest,
    AtsSourceDiscoveryCandidate,
    AtsSourceHistoryEvent,
    AtsSourceUpdateRequest,
    CloudIntegration,
    FetchSourcesResponse,
    IntegrationUpdateRequest,
    PipelineTaskResponse,
    PipelineStatusResponse,
    ResumeEligibilityResponse,
    ResumePreflightResponse,
    ResumeStatusResponse,
    ResumeUploadResponse,
    SourceFetchResponse,
    SyncRunResponse,
    UserAtsSource,
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

    processJobs: () =>
        apiClient.post<PipelineTaskResponse>('/pipeline/process-jobs'),

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

    fetchSource: (source: string, limit?: number) =>
        apiClient.post<SourceFetchResponse>('/pipeline/source-fetch', {
            source,
            ...(limit === undefined ? {} : { limit }),
        }),

    getCloudIntegrations: () =>
        apiClient.get<CloudIntegration[]>('/cloud/integrations', {
            validateStatus: (status) => status < 500,
        }),

    updateCloudIntegration: (integrationId: string, payload: IntegrationUpdateRequest) =>
        apiClient.patch<CloudIntegration>(`/cloud/integrations/${integrationId}`, payload),

    deleteCloudIntegration: (integrationId: string) =>
        apiClient.delete(`/cloud/integrations/${integrationId}`),

    syncCloudIntegration: (integrationId: string, force = false) =>
        apiClient.post<SyncRunResponse>(`/cloud/integrations/${integrationId}/sync`, { force }),

    getUserAtsSources: () =>
        apiClient.get<UserAtsSource[]>('/cloud/integrations/sources', {
            validateStatus: (status) => status < 500,
        }),

    discoverAtsSources: (payload: AtsSourceCreateRequest) =>
        apiClient.post<AtsSourceDiscoveryCandidate[]>('/cloud/integrations/sources/discover', payload),

    getUserAtsSourceHistory: () =>
        apiClient.get<AtsSourceHistoryEvent[]>('/cloud/integrations/sources/history', {
            validateStatus: (status) => status < 500,
        }),

    createUserAtsSource: (payload: AtsSourceCreateRequest) =>
        apiClient.post<UserAtsSource>('/cloud/integrations/sources', payload),

    updateUserAtsSource: (sourceId: string, payload: AtsSourceUpdateRequest) =>
        apiClient.patch<UserAtsSource>(`/cloud/integrations/sources/${sourceId}`, payload),

    deleteUserAtsSource: (sourceId: string) =>
        apiClient.delete(`/cloud/integrations/sources/${sourceId}`),

    syncUserAtsSource: (sourceId: string, force = false) =>
        apiClient.post<SyncRunResponse>(`/cloud/integrations/sources/${sourceId}/sync`, { force }),

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
