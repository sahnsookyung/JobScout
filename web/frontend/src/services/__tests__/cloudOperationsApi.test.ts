import { vi } from 'vitest';

import { apiClient } from '../api';
import { cloudOperationsApi } from '../cloudOperationsApi';

vi.mock('../api', () => ({
    apiClient: {
        get: vi.fn(),
    },
}));

const mockGet = vi.mocked(apiClient.get);

describe('cloudOperationsApi', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('loads the tenant operations status endpoint', async () => {
        const expected = {
            data: {
                generated_at: '2026-05-24T00:00:00Z',
                tenant_id: 'tenant-1',
                quotas: { backend: 'redis' },
                warnings: [],
            },
        };
        mockGet.mockResolvedValueOnce(expected);

        const result = await cloudOperationsApi.getStatus();

        expect(mockGet).toHaveBeenCalledWith('/cloud/operations/status');
        expect(result.data.quotas?.backend).toBe('redis');
    });
});
