import { apiClient } from './api';
import type { CloudAuthExchangeResponse, CloudTenant, CloudUser } from '@/types/api';

export interface GoogleCredentialExchangeRequest {
    credential: string;
    nonce: string;
}

export interface GoogleLoginNonceResponse {
    nonce: string;
    expires_at: number;
}

export const cloudAuthApi = {
    createGoogleLoginNonce: () =>
        apiClient.get<GoogleLoginNonceResponse>('/cloud/auth/google/nonce'),

    exchangeGoogleCredential: (credential: string, nonce: string) =>
        apiClient.post<CloudAuthExchangeResponse>('/cloud/auth/google/exchange', {
            credential,
            nonce,
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
