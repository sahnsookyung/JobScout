import { apiClient } from './api';
import type { JobLifecycleStatus, JobProcessingStatus, JobsResponse } from '@/types/api';

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
};
