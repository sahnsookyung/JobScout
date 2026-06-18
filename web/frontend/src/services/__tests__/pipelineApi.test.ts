import { beforeEach, describe, expect, it, vi } from 'vitest';

const mockGet = vi.fn();
const mockPost = vi.fn();
const mockPatch = vi.fn();
const mockDelete = vi.fn();

vi.mock('@/services/api', () => ({
    apiClient: {
        get: mockGet,
        post: mockPost,
        patch: mockPatch,
        delete: mockDelete,
    },
}));

describe('pipelineApi', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('normalizes source query params and optional fetch limits', async () => {
        mockGet.mockResolvedValueOnce({ data: { sources: [] } });
        mockPost.mockResolvedValueOnce({ data: { success: true } });
        mockPost.mockResolvedValueOnce({ data: { success: true } });
        const { pipelineApi } = await import('../pipelineApi');

        await pipelineApi.getSources({ search: '  tokyo  ', includeStatus: true });
        await pipelineApi.fetchSource('tokyodev', 12);
        await pipelineApi.fetchSource('japandev');

        expect(mockGet).toHaveBeenCalledWith('/pipeline/sources', {
            params: { search: 'tokyo', include_status: true },
        });
        expect(mockPost).toHaveBeenCalledWith('/pipeline/source-fetch', {
            source: 'tokyodev',
            limit: 12,
        });
        expect(mockPost).toHaveBeenCalledWith('/pipeline/source-fetch', {
            source: 'japandev',
        });
    });

    it('uses cloud integration and user ATS source routes', async () => {
        mockGet.mockResolvedValue({ data: [] });
        mockPost.mockResolvedValue({ data: {} });
        mockPatch.mockResolvedValue({ data: {} });
        mockDelete.mockResolvedValue({ data: {} });
        const { pipelineApi } = await import('../pipelineApi');

        await pipelineApi.getCloudIntegrations();
        await pipelineApi.updateCloudIntegration('integration-1', { status: 'disabled' });
        await pipelineApi.deleteCloudIntegration('integration-1');
        await pipelineApi.syncCloudIntegration('integration-1', true);
        await pipelineApi.getUserAtsSources();
        await pipelineApi.discoverAtsSources({ display_name: 'Acme' });
        await pipelineApi.getUserAtsSourceHistory();
        await pipelineApi.createUserAtsSource({ display_name: 'Acme' });
        await pipelineApi.updateUserAtsSource('source-1', { status: 'active' });
        await pipelineApi.deleteUserAtsSource('source-1');
        await pipelineApi.syncUserAtsSource('source-1');

        expect(mockGet).toHaveBeenCalledWith('/cloud/integrations', {
            validateStatus: expect.any(Function),
        });
        expect(mockGet.mock.calls[0][1].validateStatus(499)).toBe(true);
        expect(mockGet.mock.calls[0][1].validateStatus(500)).toBe(false);
        expect(mockPatch).toHaveBeenCalledWith('/cloud/integrations/integration-1', { status: 'disabled' });
        expect(mockDelete).toHaveBeenCalledWith('/cloud/integrations/integration-1');
        expect(mockPost).toHaveBeenCalledWith('/cloud/integrations/integration-1/sync', { force: true });
        expect(mockGet).toHaveBeenCalledWith('/cloud/integrations/sources', {
            validateStatus: expect.any(Function),
        });
        expect(mockGet.mock.calls[1][1].validateStatus(499)).toBe(true);
        expect(mockGet.mock.calls[1][1].validateStatus(500)).toBe(false);
        expect(mockPost).toHaveBeenCalledWith('/cloud/integrations/sources/discover', { display_name: 'Acme' });
        expect(mockGet).toHaveBeenCalledWith('/cloud/integrations/sources/history', {
            validateStatus: expect.any(Function),
        });
        expect(mockGet.mock.calls[2][1].validateStatus(499)).toBe(true);
        expect(mockGet.mock.calls[2][1].validateStatus(500)).toBe(false);
        expect(mockPost).toHaveBeenCalledWith('/cloud/integrations/sources', { display_name: 'Acme' });
        expect(mockPatch).toHaveBeenCalledWith('/cloud/integrations/sources/source-1', { status: 'active' });
        expect(mockDelete).toHaveBeenCalledWith('/cloud/integrations/sources/source-1');
        expect(mockPost).toHaveBeenCalledWith('/cloud/integrations/sources/source-1/sync', { force: false });
    });

    it('uses resume task routes and streams upload form data in memory', async () => {
        mockGet.mockResolvedValue({ data: {} });
        mockPost.mockResolvedValue({ data: {} });
        const { pipelineApi } = await import('../pipelineApi');
        const file = new File(['resume'], 'resume.txt', { type: 'text/plain' });

        await pipelineApi.runMatching();
        await pipelineApi.processJobs();
        await pipelineApi.getPipelineStatus('task-1');
        await pipelineApi.getActivePipeline();
        await pipelineApi.getResumeEligibility();
        await pipelineApi.preflightResume('hash-1');
        await pipelineApi.stopMatching();
        await pipelineApi.checkResumeHash('hash-1');
        await pipelineApi.getResumeStatus('task-1');
        await pipelineApi.uploadResume(file, 'hash-1');
        await pipelineApi.selectResume('hash-1', 'resume.txt');
        await pipelineApi.retryResume('upload-1');

        expect(mockPost).toHaveBeenCalledWith('/pipeline/run-matching');
        expect(mockPost).toHaveBeenCalledWith('/pipeline/process-jobs');
        expect(mockGet).toHaveBeenCalledWith('/pipeline/status/task-1');
        expect(mockGet).toHaveBeenCalledWith('/pipeline/active');
        expect(mockGet).toHaveBeenCalledWith('/pipeline/resume-eligibility');
        expect(mockPost).toHaveBeenCalledWith('/pipeline/resume-preflight', { resume_hash: 'hash-1' });
        expect(mockPost).toHaveBeenCalledWith('/pipeline/stop');
        expect(mockPost).toHaveBeenCalledWith('/pipeline/check-resume-hash', { resume_hash: 'hash-1' });
        expect(mockGet).toHaveBeenCalledWith('/pipeline/resume-status/task-1');
        const uploadCall = mockPost.mock.calls.find(([url]) => url === '/pipeline/upload-resume');
        expect(uploadCall?.[1]).toBeInstanceOf(FormData);
        expect((uploadCall?.[1] as FormData).get('file')).toBe(file);
        expect((uploadCall?.[1] as FormData).get('resume_hash')).toBe('hash-1');
        expect(uploadCall?.[2]).toEqual({ headers: { 'Content-Type': 'multipart/form-data' } });
        expect(mockPost).toHaveBeenCalledWith('/pipeline/select-resume', {
            resume_hash: 'hash-1',
            original_filename: 'resume.txt',
        });
        expect(mockPost).toHaveBeenCalledWith('/pipeline/retry-resume', {
            upload_id: 'upload-1',
        });
    });
});
