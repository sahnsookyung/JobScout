/**
 * Tests for usePipeline hook
 * Covers: usePipeline.ts
 */

import { renderHook, waitFor, act } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { usePipeline } from '../usePipeline';
import { pipelineApi } from '@/services/pipelineApi';

vi.mock('@/services/pipelineApi', () => ({
    pipelineApi: {
        getActivePipeline: vi.fn(),
        getResumeEligibility: vi.fn(),
        preflightResume: vi.fn(),
        selectResume: vi.fn(),
        retryResume: vi.fn(),
        runMatching: vi.fn(),
        stopMatching: vi.fn(),
        uploadResume: vi.fn(),
        getResumeStatus: vi.fn(),
    },
}));

vi.mock('@/utils/indexedDB', () => ({
    getResumeHash: vi.fn().mockResolvedValue(null),
    getResume: vi.fn().mockResolvedValue(null),
    saveResume: vi.fn().mockResolvedValue(undefined),
}));

vi.mock('@/utils/fileUtils', () => ({
    computeFileHash: vi.fn().mockResolvedValue('mock-hash-abc123'),
}));

// Mock EventSource
class MockEventSource {
    static readonly CONNECTING = 0;
    static readonly OPEN = 1;
    static readonly CLOSED = 2;
    readonly CONNECTING = 0;
    readonly OPEN = 1;
    readonly CLOSED = 2;
    onopen: (() => void) | null = null;
    onmessage: ((event: MessageEvent) => void) | null = null;
    onerror: (() => void) | null = null;
    constructor(public url: string) {}
    close(): void {
        // Mock implementation - required by EventSource interface
    }
}

vi.stubGlobal('EventSource', MockEventSource);

const createWrapper = () => {
    const queryClient = new QueryClient({
        defaultOptions: { queries: { retry: false } },
    });
    return ({ children }: { children: React.ReactNode }) => (
        <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );
};

describe('usePipeline', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        vi.mocked(pipelineApi.getActivePipeline).mockResolvedValue({ data: null } as never);
        vi.mocked(pipelineApi.getResumeEligibility).mockResolvedValue({
            data: {
                can_run: false,
                status: 'missing',
                message: 'No resume uploaded yet.',
            },
        } as never);
        vi.mocked(pipelineApi.preflightResume).mockResolvedValue({
            data: {
                status: 'upload_required',
                message: 'Upload required',
            },
        } as never);
    });

    describe('hook initialization', () => {
        it('should return correct initial structure', () => {
            const { result } = renderHook(() => usePipeline(), {
                wrapper: createWrapper(),
            });

            expect(result.current).toHaveProperty('activePipeline');
            expect(result.current).toHaveProperty('status');
            expect(result.current).toHaveProperty('connectionState');
            expect(result.current).toHaveProperty('sseError');
            expect(result.current).toHaveProperty('isLoading');
            expect(result.current).toHaveProperty('runPipeline');
            expect(result.current).toHaveProperty('stopPipeline');
            expect(result.current).toHaveProperty('uploadResume');
            expect(result.current).toHaveProperty('isPreparingResume');
            expect(result.current).toHaveProperty('retrySSE');
        });

        it('should start in loading state', () => {
            const { result } = renderHook(() => usePipeline(), {
                wrapper: createWrapper(),
            });
            expect(result.current.isLoading).toBe(true);
        });
    });

    describe('activePipeline query', () => {
        it('should fetch active pipeline', async () => {
            vi.mocked(pipelineApi.getActivePipeline).mockResolvedValue({
                data: { task_id: 'test-123', status: 'running', step: 'vector_matching' },
            } as never);

            const { result } = renderHook(() => usePipeline(), {
                wrapper: createWrapper(),
            });

            await waitFor(() => {
                expect(result.current.isLoading).toBe(false);
            });

            expect(pipelineApi.getActivePipeline).toHaveBeenCalled();
        });

        it('uses active pipeline status before SSE emits', async () => {
            vi.mocked(pipelineApi.getActivePipeline).mockResolvedValue({
                data: { task_id: 'test-123', status: 'pending', step: 'initializing' },
            } as never);

            const { result } = renderHook(() => usePipeline(), {
                wrapper: createWrapper(),
            });

            await waitFor(() => {
                expect(result.current.isLoading).toBe(false);
            });

            expect(result.current.status).toEqual({
                task_id: 'test-123',
                status: 'pending',
                step: 'initializing',
            });
            expect(result.current.isRunning).toBe(true);
        });

        it('should handle null response', async () => {
            const { result } = renderHook(() => usePipeline(), {
                wrapper: createWrapper(),
            });

            await waitFor(() => {
                expect(result.current.isLoading).toBe(false);
            });

            expect(result.current.activePipeline).toBeNull();
        });

        it('should handle API error', async () => {
            vi.mocked(pipelineApi.getActivePipeline).mockRejectedValue(
                new Error('API Error')
            );

            const { result } = renderHook(() => usePipeline(), {
                wrapper: createWrapper(),
            });

            await waitFor(() => {
                expect(result.current.isLoading).toBe(false);
            });

            expect(result.current.activePipeline).toBeNull();
        });
    });

    describe('runPipeline', () => {
        it('calls onError when no resume in IndexedDB', async () => {
            const { getResumeHash } = await import('@/utils/indexedDB');
            vi.mocked(getResumeHash).mockResolvedValue(null);

            const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });
            const onError = vi.fn();

            await act(async () => {
                await result.current.runPipeline(onError);
            });

            expect(onError).toHaveBeenCalledWith(expect.stringContaining('No resume found'));
            expect(pipelineApi.runMatching).not.toHaveBeenCalled();
        });

        it('blocks matching immediately when a resume task is already pending', async () => {
            const mockFile = new File(['test'], 'resume.pdf');
            vi.mocked(pipelineApi.preflightResume).mockResolvedValue({
                data: { status: 'processing_existing', message: 'Processing...', task_id: 'resume-task-pending' },
            } as never);
            vi.mocked(pipelineApi.getResumeStatus).mockResolvedValue({
                data: { status: 'running' },
            } as never);

            const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });
            const onError = vi.fn();

            await act(async () => {
                await result.current.uploadResume(mockFile);
            });

            await act(async () => {
                await result.current.runPipeline(onError);
            });

            expect(onError).toHaveBeenCalledWith('Resume is still being processed. Please wait a moment and try again.');
            expect(pipelineApi.getResumeEligibility).not.toHaveBeenCalled();
        });

        it('calls runMatching when resume exists on backend', async () => {
            vi.mocked(pipelineApi.getResumeEligibility).mockResolvedValue({
                data: {
                    can_run: true,
                    status: 'ready',
                    message: 'Resume ready',
                },
            } as never);
            vi.mocked(pipelineApi.runMatching).mockResolvedValue({ data: { task_id: 'new-task' } } as never);

            const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });
            const onError = vi.fn();

            await act(async () => {
                await result.current.runPipeline(onError);
            });

            expect(pipelineApi.runMatching).toHaveBeenCalledTimes(1);
            expect(onError).not.toHaveBeenCalled();
            expect(result.current.status).toEqual({
                task_id: 'new-task',
                status: 'pending',
                step: 'initializing',
            });
        });

        it('uploads then runs matching when resume not on backend', async () => {
            const { getResumeHash, getResume } = await import('@/utils/indexedDB');
            vi.mocked(getResumeHash).mockResolvedValue('abc123');
            vi.mocked(getResume).mockResolvedValue(new File(['data'], 'resume.pdf', { type: 'application/pdf' }));
            vi.mocked(pipelineApi.getResumeEligibility)
                .mockResolvedValueOnce({
                    data: {
                        can_run: false,
                        status: 'missing',
                        message: 'Missing resume',
                    },
                } as never)
                .mockResolvedValueOnce({
                    data: {
                        can_run: true,
                        status: 'ready',
                        message: 'Resume ready',
                    },
                } as never);
            vi.mocked(pipelineApi.uploadResume).mockResolvedValue({
                data: { message: 'ok', status: 'ready' },
            } as never);
            vi.mocked(pipelineApi.runMatching).mockResolvedValue({ data: { task_id: 'new-task' } } as never);

            const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });

            await act(async () => {
                await result.current.runPipeline();
            });

            expect(pipelineApi.uploadResume).toHaveBeenCalled();
            expect(pipelineApi.runMatching).toHaveBeenCalledTimes(1);
        });

        it('asks for re-upload when IndexedDB hash exists but file blob is missing', async () => {
            const { getResumeHash, getResume } = await import('@/utils/indexedDB');
            vi.mocked(getResumeHash).mockResolvedValue('abc123');
            vi.mocked(getResume).mockResolvedValue(null);
            vi.mocked(pipelineApi.getResumeEligibility).mockResolvedValue({
                data: {
                    can_run: false,
                    status: 'missing',
                    message: 'Missing resume',
                },
            } as never);

            const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });
            const onError = vi.fn();

            await act(async () => {
                await result.current.runPipeline(onError);
            });

            expect(onError).toHaveBeenCalledWith('Resume file not found in browser storage. Please re-upload.');
            expect(pipelineApi.uploadResume).not.toHaveBeenCalled();
        });

        it('tracks pending task when eligibility reports existing processing', async () => {
            vi.mocked(pipelineApi.getResumeEligibility).mockResolvedValue({
                data: {
                    can_run: false,
                    status: 'embedding',
                    task_id: 'resume-task-embedding',
                    message: 'Resume is still processing.',
                },
            } as never);

            const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });
            const onError = vi.fn();

            await act(async () => {
                await result.current.runPipeline(onError);
            });

            expect(onError).toHaveBeenCalledWith('Resume is still processing.');
            expect(result.current.isPreparingResume).toBe(true);
            expect(pipelineApi.runMatching).not.toHaveBeenCalled();
        });

        it('reports backend eligibility error after browser fallback upload completes', async () => {
            vi.useFakeTimers();
            const { getResumeHash, getResume } = await import('@/utils/indexedDB');
            vi.mocked(getResumeHash).mockResolvedValue('abc123');
            vi.mocked(getResume).mockResolvedValue(new File(['data'], 'resume.pdf', { type: 'application/pdf' }));
            vi.mocked(pipelineApi.getResumeEligibility)
                .mockResolvedValueOnce({
                    data: {
                        can_run: false,
                        status: 'missing',
                        message: 'Missing resume',
                    },
                } as never)
                .mockResolvedValueOnce({
                    data: {
                        can_run: false,
                        status: 'failed_retryable',
                        message: 'Resume failed and needs attention.',
                    },
                } as never);
            vi.mocked(pipelineApi.uploadResume).mockResolvedValue({
                data: { message: 'Uploaded', task_id: 'resume-task-1', status: 'in_progress' },
            } as never);
            vi.mocked(pipelineApi.getResumeStatus).mockResolvedValue({
                data: { status: 'completed' },
            } as never);

            const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });
            const onError = vi.fn();

            await act(async () => {
                const promise = result.current.runPipeline(onError);
                await vi.advanceTimersByTimeAsync(2000);
                await promise;
            });

            expect(pipelineApi.getResumeStatus).toHaveBeenCalledWith('resume-task-1');
            expect(onError).toHaveBeenCalledWith('Resume failed and needs attention.');
            expect(pipelineApi.runMatching).not.toHaveBeenCalled();
            vi.useRealTimers();
        });

        it('reports mutate failure through the generic error handler', async () => {
            vi.mocked(pipelineApi.getResumeEligibility).mockResolvedValue({
                data: {
                    can_run: true,
                    status: 'ready',
                    message: 'Resume ready',
                },
            } as never);
            vi.mocked(pipelineApi.runMatching).mockRejectedValue(new Error('server exploded'));

            const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });
            const onError = vi.fn();

            await act(async () => {
                await result.current.runPipeline(onError);
            });

            expect(onError).toHaveBeenCalledWith('Failed to start matching: server exploded');
        });
    });

    describe('stopPipeline', () => {
        it('should call stopMatching API', async () => {
            const { result } = renderHook(() => usePipeline(), {
                wrapper: createWrapper(),
            });

            await act(async () => {
                result.current.stopPipeline();
            });

            expect(pipelineApi.stopMatching).toHaveBeenCalledTimes(1);
        });
    });

    describe('uploadResume', () => {
        it('calls API with file when hash not on backend', async () => {
            const mockFile = new File(['test'], 'resume.pdf', { type: 'application/pdf' });
            vi.mocked(pipelineApi.preflightResume).mockResolvedValue({
                data: { status: 'upload_required', message: 'Upload required' },
            } as never);
            vi.mocked(pipelineApi.uploadResume).mockResolvedValue({
                data: { message: 'Resume uploaded successfully', status: 'ready' },
            } as never);

            const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });

            await act(async () => {
                await result.current.uploadResume(mockFile);
            });

            expect(pipelineApi.uploadResume).toHaveBeenCalledWith(mockFile, 'mock-hash-abc123');
        });

        it('selects existing ready resume when hash already on backend', async () => {
            const mockFile = new File(['test'], 'resume.pdf');
            vi.mocked(pipelineApi.preflightResume).mockResolvedValue({
                data: { status: 'ready_already_known', message: 'Already ready' },
            } as never);
            vi.mocked(pipelineApi.selectResume).mockResolvedValue({
                data: { message: 'Resume already ready', status: 'ready' },
            } as never);

            const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });
            let uploadResult: { alreadyExists: boolean; message: string } | undefined;

            await act(async () => {
                uploadResult = await result.current.uploadResume(mockFile);
            });

            expect(pipelineApi.uploadResume).not.toHaveBeenCalled();
            expect(pipelineApi.selectResume).toHaveBeenCalledWith('mock-hash-abc123', 'resume.pdf');
            expect(uploadResult?.alreadyExists).toBe(true);
        });

        it('sets pendingResumeTaskId when preflight reports existing processing', async () => {
            const mockFile = new File(['test'], 'resume.pdf');
            vi.mocked(pipelineApi.preflightResume).mockResolvedValue({
                data: { status: 'processing_existing', message: 'Processing...', task_id: 'bg-task-1' },
            } as never);
            vi.mocked(pipelineApi.getResumeStatus).mockResolvedValue({
                data: { status: 'running' },
            } as never);

            const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });

            await act(async () => {
                await result.current.uploadResume(mockFile);
            });

            // isPreparingResume should be true while task is pending
            expect(result.current.isPreparingResume).toBe(true);
        });

        it('isUploading is true during upload', async () => {
            const mockFile = new File(['test'], 'resume.pdf');
            let resolveUpload!: (value: any) => void;
            vi.mocked(pipelineApi.preflightResume).mockResolvedValue({
                data: { status: 'upload_required', message: 'Upload required' },
            } as never);
            vi.mocked(pipelineApi.uploadResume).mockReturnValue(
                new Promise(resolve => { resolveUpload = resolve; }) as any
            );

            const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });

            act(() => {
                result.current.uploadResume(mockFile);
            });

            await waitFor(() => {
                expect(result.current.isUploading).toBe(true);
            });

            await act(async () => {
                resolveUpload({ data: { message: 'ok' } });
            });
        });

        it('handles upload error', async () => {
            const mockFile = new File(['test'], 'resume.pdf');
            vi.mocked(pipelineApi.preflightResume).mockResolvedValue({
                data: { status: 'upload_required', message: 'Upload required' },
            } as never);
            vi.mocked(pipelineApi.uploadResume).mockRejectedValue(new Error('Network error'));

            const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });

            await act(async () => {
                await expect(result.current.uploadResume(mockFile)).rejects.toThrow('Network error');
            });

            expect(result.current.isUploading).toBe(false);
        });

        it('retries existing retryable upload when preflight allows it', async () => {
            const mockFile = new File(['test'], 'resume.pdf');
            vi.mocked(pipelineApi.preflightResume).mockResolvedValue({
                data: {
                    status: 'failed_retryable',
                    message: 'Retryable failure',
                    upload_id: 'upload-1',
                },
            } as never);
            vi.mocked(pipelineApi.retryResume).mockResolvedValue({
                data: { message: 'Retry started', task_id: 'retry-task-1', status: 'in_progress' },
            } as never);

            const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });

            await act(async () => {
                await result.current.uploadResume(mockFile);
            });

            expect(pipelineApi.retryResume).toHaveBeenCalledWith('upload-1');
            expect(pipelineApi.uploadResume).not.toHaveBeenCalled();
        });
    });

    describe('SSE connection', () => {
        it('should create EventSource when task_id exists', async () => {
            vi.mocked(pipelineApi.getActivePipeline).mockResolvedValue({
                data: { task_id: 'sse-task', status: 'running' },
            } as never);

            const { result } = renderHook(() => usePipeline(), {
                wrapper: createWrapper(),
            });

            await waitFor(() => {
                expect(result.current.connectionState).toBeDefined();
            });
        });

        it('should provide retrySSE function', () => {
            const { result } = renderHook(() => usePipeline(), {
                wrapper: createWrapper(),
            });

            expect(typeof result.current.retrySSE).toBe('function');
        });
    });

    describe('state flags', () => {
        it('should have isRunning flag', () => {
            const { result } = renderHook(() => usePipeline(), {
                wrapper: createWrapper(),
            });

            expect(typeof result.current.isRunning).toBe('boolean');
        });

        it('should have isStopping flag', () => {
            const { result } = renderHook(() => usePipeline(), {
                wrapper: createWrapper(),
            });

            expect(typeof result.current.isStopping).toBe('boolean');
        });

        it('should have isUploading flag', () => {
            const { result } = renderHook(() => usePipeline(), {
                wrapper: createWrapper(),
            });

            expect(typeof result.current.isUploading).toBe('boolean');
        });

        it('should have isPreparingResume flag', () => {
            const { result } = renderHook(() => usePipeline(), {
                wrapper: createWrapper(),
            });

            expect(typeof result.current.isPreparingResume).toBe('boolean');
        });
    });

    describe('error states', () => {
        it('should have runPipelineError property', () => {
            const { result } = renderHook(() => usePipeline(), {
                wrapper: createWrapper(),
            });

            expect('runPipelineError' in result.current).toBe(true);
        });

        it('should have stopPipelineError property', () => {
            const { result } = renderHook(() => usePipeline(), {
                wrapper: createWrapper(),
            });

            expect('stopPipelineError' in result.current).toBe(true);
        });
    });

    describe('clearTask', () => {
        it('should provide clearTask function', () => {
            const { result } = renderHook(() => usePipeline(), {
                wrapper: createWrapper(),
            });

            expect(typeof result.current.clearTask).toBe('function');
        });
    });
});
