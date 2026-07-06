import { apiClient } from './api';
import type {
    JobAvailabilityMutationResponse,
    JobLifecycleStatus,
    JobProcessingStatus,
    JobsResponse,
    ProcessingBlockersResponse,
} from '@/types/api';

export interface GetJobsParams {
    job_status?: JobLifecycleStatus;
    processing_status?: JobProcessingStatus;
    search?: string;
    limit?: number;
    offset?: number;
}

function shouldUseCloudJobRefresh(): boolean {
    const explicit = String(import.meta.env.VITE_CLOUD_JOB_REFRESH ?? '').toLowerCase();
    if (explicit) {
        return explicit === 'true';
    }
    return import.meta.env.PROD
        || String(import.meta.env.VITE_AUTH_REQUIRED ?? '').toLowerCase() === 'true';
}

export const jobsApi = {
    getJobs: (params: GetJobsParams = {}) =>
        apiClient.get<JobsResponse>('/jobs', { params }),

    retireJob: (jobId: string) =>
        apiClient.post<JobAvailabilityMutationResponse>(`/jobs/${jobId}/retire`),

    restoreJob: (jobId: string) =>
        apiClient.post<JobAvailabilityMutationResponse>(`/jobs/${jobId}/restore`),

    refreshJobAvailability: (jobId: string) =>
        apiClient.post<JobAvailabilityMutationResponse>(
            shouldUseCloudJobRefresh()
                ? `/cloud/integrations/jobs/${jobId}/refresh-availability`
                : `/jobs/${jobId}/refresh-availability`
        ),

    getProcessingBlockers: (
        params: {
            stage?: string;
            limit?: number;
            cursor?: string | null;
            view?: 'compact' | 'detail';
        } = {}
    ) =>
        apiClient.get<ProcessingBlockersResponse>('/jobs/processing-blockers', { params }),
};
