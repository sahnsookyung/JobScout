import { apiClient } from './api';
import type { CloudAuthExchangeResponse, CloudTenant, CloudUser } from '@/types/api';

export interface GoogleCredentialExchangeRequest {
    credential: string;
}

export const cloudAuthApi = {
    exchangeGoogleCredential: (credential: string) =>
        apiClient.post<CloudAuthExchangeResponse>('/cloud/auth/google/exchange', {
            credential,
        } satisfies GoogleCredentialExchangeRequest),

    getCurrentUser: () =>
        apiClient.get<CloudUser>('/cloud/auth/me'),

    refreshSession: () =>
        apiClient.post<CloudAuthExchangeResponse>('/cloud/auth/refresh'),

    logout: () =>
        apiClient.post<void>('/cloud/auth/logout'),

    listTenants: () =>
        apiClient.get<CloudTenant[]>('/cloud/auth/tenants'),
};
