import { apiClient } from './api';
import type {
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

export const jobsApi = {
    getJobs: (params: GetJobsParams = {}) =>
        apiClient.get<JobsResponse>('/jobs', { params }),

    getProcessingBlockers: (params: { stage?: string; limit?: number } = {}) =>
        apiClient.get<ProcessingBlockersResponse>('/jobs/processing-blockers', { params }),
};
