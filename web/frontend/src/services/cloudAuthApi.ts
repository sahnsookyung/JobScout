import { apiClient } from './api';
import type { CloudAuthExchangeResponse, CloudUser } from '@/types/api';

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
};
