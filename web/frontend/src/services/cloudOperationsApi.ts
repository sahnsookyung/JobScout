import { apiClient } from './api';

export interface CloudOperationsStatus {
    generated_at: string;
    tenant_id: string;
    quotas?: Record<string, unknown>;
    notifications?: Record<string, unknown>;
    ats?: Record<string, unknown>;
    warnings?: Array<{ code: string; message: string }>;
}

export const cloudOperationsApi = {
    getStatus: () => apiClient.get<CloudOperationsStatus>('/cloud/operations/status'),
};
