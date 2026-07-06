import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const mockGet = vi.fn();
const mockPost = vi.fn();

vi.mock('@/services/api', () => ({
    apiClient: {
        get: mockGet,
        post: mockPost,
    },
}));

describe('jobsApi', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        vi.unstubAllEnvs();
        mockPost.mockResolvedValue({ data: { success: true } });
    });

    afterEach(() => {
        vi.unstubAllEnvs();
    });

    it('uses the local refresh endpoint by default in development', async () => {
        const { jobsApi } = await import('../jobsApi');

        await jobsApi.refreshJobAvailability('job-1');

        expect(mockPost).toHaveBeenCalledWith('/jobs/job-1/refresh-availability');
    });

    it('uses the cloud refresh endpoint when cloud refresh is enabled', async () => {
        vi.stubEnv('VITE_CLOUD_JOB_REFRESH', 'true');
        const { jobsApi } = await import('../jobsApi');

        await jobsApi.refreshJobAvailability('job-1');

        expect(mockPost).toHaveBeenCalledWith('/cloud/integrations/jobs/job-1/refresh-availability');
    });

    it('uses the cloud refresh endpoint when hosted auth is required', async () => {
        vi.stubEnv('VITE_AUTH_REQUIRED', 'true');
        const { jobsApi } = await import('../jobsApi');

        await jobsApi.refreshJobAvailability('job-1');

        expect(mockPost).toHaveBeenCalledWith('/cloud/integrations/jobs/job-1/refresh-availability');
    });
});
