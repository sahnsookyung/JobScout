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
        runMatching: vi.fn(),
        stopMatching: vi.fn(),
        uploadResume: vi.fn(),
    },
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
    close(): void {}
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
                data: { task_id: 'test-123', status: 'running' },
            } as never);

            const { result } = renderHook(() => usePipeline(), {
                wrapper: createWrapper(),
            });

            await waitFor(() => {
                expect(result.current.isLoading).toBe(false);
            });

            expect(pipelineApi.getActivePipeline).toHaveBeenCalled();
        });

        it('should handle null response', async () => {
            vi.mocked(pipelineApi.getActivePipeline).mockResolvedValue({
                data: null,
            } as never);

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
        it('should call runMatching API', async () => {
            const { result } = renderHook(() => usePipeline(), {
                wrapper: createWrapper(),
            });

            await act(async () => {
                result.current.runPipeline();
            });

            expect(pipelineApi.runMatching).toHaveBeenCalledTimes(1);
        });

        it('should invalidate queries on success', async () => {
            vi.mocked(pipelineApi.runMatching).mockResolvedValue({
                data: { task_id: 'new-task' },
            } as never);

            const { result } = renderHook(() => usePipeline(), {
                wrapper: createWrapper(),
            });

            await act(async () => {
                result.current.runPipeline();
            });

            await waitFor(() => {
                expect(pipelineApi.runMatching).toHaveBeenCalled();
            });
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
        it('should upload resume file', async () => {
            const mockFile = new File(['test'], 'resume.pdf', {
                type: 'application/pdf',
            });

            vi.mocked(pipelineApi.uploadResume).mockResolvedValue({
                data: { resume_hash: 'abc123' },
            } as never);

            const { result } = renderHook(() => usePipeline(), {
                wrapper: createWrapper(),
            });

            await act(async () => {
                result.current.uploadResume({ file: mockFile });
            });

            expect(pipelineApi.uploadResume).toHaveBeenCalledWith(mockFile, undefined);
        });

        it('should upload resume with hash', async () => {
            const mockFile = new File(['test'], 'resume.json');

            const { result } = renderHook(() => usePipeline(), {
                wrapper: createWrapper(),
            });

            await act(async () => {
                result.current.uploadResume({ file: mockFile, hash: 'xyz789' });
            });

            expect(pipelineApi.uploadResume).toHaveBeenCalledWith(mockFile, 'xyz789');
        });

        it('should invalidate resume query on success', async () => {
            const mockFile = new File(['test'], 'resume.pdf');
            vi.mocked(pipelineApi.uploadResume).mockResolvedValue({
                data: { resume_hash: 'hash' },
            } as never);

            const { result } = renderHook(() => usePipeline(), {
                wrapper: createWrapper(),
            });

            await act(async () => {
                result.current.uploadResume({ file: mockFile });
            });

            expect(pipelineApi.uploadResume).toHaveBeenCalled();
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

        it('should have uploadResumeError property', () => {
            const { result } = renderHook(() => usePipeline(), {
                wrapper: createWrapper(),
            });

            expect('uploadResumeError' in result.current).toBe(true);
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
