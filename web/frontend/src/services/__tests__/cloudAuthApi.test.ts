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
        });

        await cloudAuthApi.exchangeGoogleCredential('google-credential', 'login-nonce');

        expect(mockPost).toHaveBeenCalledWith('/cloud/auth/google/exchange', {
            credential: 'google-credential',
            nonce: 'login-nonce',
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
        });

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
        });

        const result = await cloudAuthApi.refreshSession();

        expect(mockPost).toHaveBeenCalledWith('/cloud/auth/refresh');
        expect(result.data.access_token).toBe('refreshed-token');
    });

    it('logs out the current browser session', async () => {
        const expected = { data: undefined };
        mockPost.mockResolvedValueOnce(expected);

        const result = await cloudAuthApi.logout();

        expect(mockPost).toHaveBeenCalledWith('/cloud/auth/logout');
        expect(result).toEqual(expected);
    });

    it('lists tenants visible to the authenticated user', async () => {
        const expected = {
            data: [
                {
                    id: 'tenant-1',
                    slug: 'personal',
                    display_name: 'Personal',
                    role: 'owner',
                    is_default: true,
                },
            ],
        };
        mockGet.mockResolvedValueOnce(expected);

        const result = await cloudAuthApi.listTenants();

        expect(mockGet).toHaveBeenCalledWith('/cloud/auth/tenants');
        expect(result.data[0].id).toBe('tenant-1');
    });
});
