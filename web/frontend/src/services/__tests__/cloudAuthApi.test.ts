import { vi } from 'vitest';

import { apiClient } from '../api';
import { cloudAuthApi } from '../cloudAuthApi';

vi.mock('../api', () => ({
    apiClient: {
        get: vi.fn(),
        post: vi.fn(),
    },
}));

const mockGet = vi.mocked(apiClient.get);
const mockPost = vi.mocked(apiClient.post);

describe('cloudAuthApi', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('exchanges a Google credential for an app token', async () => {
        mockPost.mockResolvedValueOnce({
            data: {
                access_token: 'app-token',
                token_type: 'Bearer',
                user: {
                    id: 'user-1',
                    email: 'user@example.com',
                    name: 'User Example',
                    provider: 'google',
                    token_kind: 'google_id_token',
                },
            },
        } as never);

        await cloudAuthApi.exchangeGoogleCredential('google-credential');

        expect(mockPost).toHaveBeenCalledWith('/cloud/auth/google/exchange', {
            credential: 'google-credential',
        });
    });

    it('loads the authenticated cloud user', async () => {
        mockGet.mockResolvedValueOnce({
            data: {
                id: 'user-1',
                email: 'user@example.com',
                name: 'User Example',
                provider: 'google',
                token_kind: 'app_jwt',
            },
        } as never);

        const result = await cloudAuthApi.getCurrentUser();

        expect(mockGet).toHaveBeenCalledWith('/cloud/auth/me');
        expect(result.data.email).toBe('user@example.com');
    });

    it('refreshes the current session token', async () => {
        mockPost.mockResolvedValueOnce({
            data: {
                access_token: 'refreshed-token',
                token_type: 'Bearer',
                user: {
                    id: 'user-1',
                    email: 'user@example.com',
                    name: 'User Example',
                    provider: 'google',
                    token_kind: 'app_jwt',
                },
            },
        } as never);

        const result = await cloudAuthApi.refreshSession();

        expect(mockPost).toHaveBeenCalledWith('/cloud/auth/refresh');
        expect(result.data.access_token).toBe('refreshed-token');
    });
});
