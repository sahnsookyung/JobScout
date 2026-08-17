import type { AxiosResponse } from 'axios';

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

let logoutInFlight: Promise<AxiosResponse<void>> | null = null;

function clearCompletedLogout(request: Promise<AxiosResponse<void>>): void {
    if (logoutInFlight === request) {
        logoutInFlight = null;
    }
}

export const cloudAuthApi = {
    createGoogleLoginNonce: () =>
        apiClient.get<GoogleLoginNonceResponse>('/cloud/auth/google/nonce'),

    exchangeGoogleCredential: async (credential: string, nonce: string) => {
        const pendingLogout = logoutInFlight;
        if (pendingLogout) {
            await pendingLogout.catch(() => undefined);
        }
        return apiClient.post<CloudAuthExchangeResponse>('/cloud/auth/google/exchange', {
            credential,
            nonce,
        } satisfies GoogleCredentialExchangeRequest);
    },

    getCurrentUser: () =>
        apiClient.get<CloudUser>('/cloud/auth/me'),

    refreshSession: () =>
        apiClient.post<CloudAuthExchangeResponse>('/cloud/auth/refresh'),

    logout: () => {
        if (logoutInFlight) {
            return logoutInFlight;
        }
        const request = apiClient.post<void>('/cloud/auth/logout');
        logoutInFlight = request;
        void request.then(
            () => clearCompletedLogout(request),
            () => clearCompletedLogout(request)
        );
        return request;
    },

    listTenants: () =>
        apiClient.get<CloudTenant[]>('/cloud/auth/tenants'),
};
