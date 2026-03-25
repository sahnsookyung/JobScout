import { vi } from 'vitest';
import { pipelineApi } from '../pipelineApi';
import { apiClient } from '../api';

vi.mock('../api', () => ({
    apiClient: {
        get: vi.fn(),
        post: vi.fn(),
    },
}));

const mockGet = vi.mocked(apiClient.get);
const mockPost = vi.mocked(apiClient.post);

describe('pipelineApi', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('runMatching', () => {
        it('calls POST /pipeline/run-matching', () => {
            mockPost.mockResolvedValueOnce({ data: { task_id: 'task-1', status: 'started' } } as any);
            pipelineApi.runMatching();
            expect(mockPost).toHaveBeenCalledWith('/pipeline/run-matching');
        });

        it('returns the api client response', async () => {
            const expected = { data: { task_id: 't1', status: 'running' } };
            mockPost.mockResolvedValueOnce(expected as any);
            const result = await pipelineApi.runMatching();
            expect(result).toEqual(expected);
        });
    });

    describe('getPipelineStatus', () => {
        it('calls GET /pipeline/status/:taskId', () => {
            mockGet.mockResolvedValueOnce({ data: { task_id: 'abc', status: 'running' } } as any);
            pipelineApi.getPipelineStatus('abc');
            expect(mockGet).toHaveBeenCalledWith('/pipeline/status/abc');
        });

        it('interpolates different task IDs into the URL', () => {
            mockGet.mockResolvedValueOnce({ data: {} } as any);
            pipelineApi.getPipelineStatus('xyz-999');
            expect(mockGet).toHaveBeenCalledWith('/pipeline/status/xyz-999');
        });
    });

    describe('getActivePipeline', () => {
        it('calls GET /pipeline/active', () => {
            mockGet.mockResolvedValueOnce({ data: null } as any);
            pipelineApi.getActivePipeline();
            expect(mockGet).toHaveBeenCalledWith('/pipeline/active');
        });

        it('returns the active pipeline response', async () => {
            const expected = { data: { task_id: 'active-1', status: 'running' } };
            mockGet.mockResolvedValueOnce(expected as any);
            const result = await pipelineApi.getActivePipeline();
            expect(result).toEqual(expected);
        });
    });

    describe('stopMatching', () => {
        it('calls POST /pipeline/stop', () => {
            mockPost.mockResolvedValueOnce({ data: { task_id: 'stop-1', status: 'stopped' } } as any);
            pipelineApi.stopMatching();
            expect(mockPost).toHaveBeenCalledWith('/pipeline/stop');
        });
    });

    describe('checkResumeHash', () => {
        it('calls POST /pipeline/check-resume-hash with the hash payload', () => {
            const hash = 'abc123hash456';
            mockPost.mockResolvedValueOnce({ data: { exists: false, resume_hash: hash } } as any);
            pipelineApi.checkResumeHash(hash);
            expect(mockPost).toHaveBeenCalledWith('/pipeline/check-resume-hash', { resume_hash: hash });
        });

        it('returns a response indicating hash exists', async () => {
            const hash = 'existinghash';
            mockPost.mockResolvedValueOnce({ data: { exists: true, resume_hash: hash } } as any);
            const result = await pipelineApi.checkResumeHash(hash);
            expect((result as any).data.exists).toBe(true);
        });

        it('returns a response indicating hash does not exist', async () => {
            mockPost.mockResolvedValueOnce({ data: { exists: false, resume_hash: 'newhash' } } as any);
            const result = await pipelineApi.checkResumeHash('newhash');
            expect((result as any).data.exists).toBe(false);
        });
    });

    describe('getResumeStatus', () => {
        it('calls GET /pipeline/resume-status/:taskId', () => {
            mockGet.mockResolvedValueOnce({ data: { task_id: 'r-1', status: 'completed' } } as any);
            pipelineApi.getResumeStatus('r-1');
            expect(mockGet).toHaveBeenCalledWith('/pipeline/resume-status/r-1');
        });

        it('interpolates the task ID correctly', () => {
            mockGet.mockResolvedValueOnce({ data: {} } as any);
            pipelineApi.getResumeStatus('resume-task-999');
            expect(mockGet).toHaveBeenCalledWith('/pipeline/resume-status/resume-task-999');
        });
    });

    describe('uploadResume', () => {
        it('calls POST /pipeline/upload-resume', () => {
            const file = new File(['content'], 'resume.pdf', { type: 'application/pdf' });
            mockPost.mockResolvedValueOnce({ data: { success: true, resume_hash: 'h1', message: 'OK' } } as any);
            pipelineApi.uploadResume(file);
            expect(mockPost).toHaveBeenCalledWith(
                '/pipeline/upload-resume',
                expect.any(FormData),
                expect.objectContaining({ headers: { 'Content-Type': 'multipart/form-data' } })
            );
        });

        it('includes the file in the FormData under key "file"', () => {
            const file = new File(['data'], 'my-resume.pdf', { type: 'application/pdf' });
            mockPost.mockResolvedValueOnce({ data: { success: true, resume_hash: '', message: 'OK' } } as any);
            pipelineApi.uploadResume(file);
            const formData: FormData = mockPost.mock.calls[0][1] as FormData;
            expect(formData.get('file')).toBe(file);
        });

        it('includes resume_hash in FormData when provided', () => {
            const file = new File(['data'], 'cv.pdf');
            const hash = 'myhash-abc123';
            mockPost.mockResolvedValueOnce({ data: { success: true, resume_hash: hash, message: 'OK' } } as any);
            pipelineApi.uploadResume(file, hash);
            const formData: FormData = mockPost.mock.calls[0][1] as FormData;
            expect(formData.get('resume_hash')).toBe(hash);
        });

        it('does not include resume_hash in FormData when not provided', () => {
            const file = new File(['data'], 'cv.pdf');
            mockPost.mockResolvedValueOnce({ data: { success: true, resume_hash: '', message: 'OK' } } as any);
            pipelineApi.uploadResume(file);
            const formData: FormData = mockPost.mock.calls[0][1] as FormData;
            expect(formData.get('resume_hash')).toBeNull();
        });

        it('does not include resume_hash when empty string passed', () => {
            const file = new File(['data'], 'cv.pdf');
            mockPost.mockResolvedValueOnce({ data: { success: true, resume_hash: '', message: 'OK' } } as any);
            pipelineApi.uploadResume(file, '');
            const formData: FormData = mockPost.mock.calls[0][1] as FormData;
            expect(formData.get('resume_hash')).toBeNull();
        });

        it('returns the upload response', async () => {
            const file = new File(['data'], 'cv.pdf');
            const expected = { data: { success: true, resume_hash: 'hash123', message: 'Uploaded', task_id: 'bg-1' } };
            mockPost.mockResolvedValueOnce(expected as any);
            const result = await pipelineApi.uploadResume(file, 'hash123');
            expect(result).toEqual(expected);
        });
    });
});
