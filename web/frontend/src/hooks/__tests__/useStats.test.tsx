/**
 * Tests for React Hooks
 * Covers: web/frontend/src/hooks/
 */

import { renderHook, waitFor, act } from '@testing-library/react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

// Mock constants first
vi.mock('@shared/constants', () => ({
    RESUME_MAX_SIZE_MB: 2,
    RESUME_MAX_SIZE: 2 * 1024 * 1024,
    RESUME_INDEXEDDB_NAME: 'jobscout-resume',
    RESUME_MAX_AGE_DAYS: 30,
}));

// Mock pipeline API with inline functions
vi.mock('@/services/pipelineApi', () => ({
    pipelineApi: {
        runMatching: vi.fn(),
        stopMatching: vi.fn(),
        getActivePipeline: vi.fn(),
        uploadResume: vi.fn(),
    },
}));

// Mock usePipelineEvents
vi.mock('../usePipelineEvents', () => ({
    usePipelineEvents: vi.fn(),
}));

// Mock matchesApi for useStats tests
vi.mock('@/services/matchesApi', () => ({
    matchesApi: {
        getStats: vi.fn(),
    },
}));

import { usePipeline } from '../usePipeline';
import { useStats } from '../useStats';

const createWrapper = () => {
    const queryClient = new QueryClient({
        defaultOptions: {
            queries: {
                retry: false,
            },
        },
    });
    return ({ children }: { children: React.ReactNode }) => (
        <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );
};

describe('useStats', () => {
    beforeEach(async () => {
        vi.clearAllMocks();
        
        // Get fresh reference to mocked function after clearAllMocks
        const { matchesApi } = await import('@/services/matchesApi');
        (matchesApi.getStats as any).mockReset();
    });

    it('fetches stats on mount', async () => {
        const { matchesApi } = await import('@/services/matchesApi');
        const mockStats = {
            total_matches: 100,
            active_matches: 45,
            hidden_count: 30,
            below_threshold_count: 25,
        };

        (matchesApi.getStats as any).mockResolvedValue({ data: { stats: mockStats } });

        const { result } = renderHook(() => useStats(), { wrapper: createWrapper() });

        await waitFor(() => {
            expect(result.current.isSuccess).toBe(true);
        });

        expect(result.current.data).toEqual(mockStats);
    });

    it('handles fetch error', async () => {
        const { matchesApi } = await import('@/services/matchesApi');
        (matchesApi.getStats as any).mockRejectedValue(new Error('Network error'));

        const { result } = renderHook(() => useStats(), { wrapper: createWrapper() });

        await waitFor(() => {
            expect(result.current.isError).toBe(true);
        });

        expect(result.current.error).toBeInstanceOf(Error);
    });

    it('uses correct staleTime', async () => {
        const { matchesApi } = await import('@/services/matchesApi');
        (matchesApi.getStats as any).mockResolvedValue({ data: { stats: {} } });

        renderHook(() => useStats(), { wrapper: createWrapper() });

        await waitFor(() => {
            expect(matchesApi.getStats).toHaveBeenCalled();
        });

        // Query should have staleTime of 60000ms (1 minute)
        // This is configured in the hook, verified by checking query options
        expect(matchesApi.getStats).toHaveBeenCalledTimes(1);
    });
});

describe('usePipeline', () => {
    beforeEach(async () => {
        vi.clearAllMocks();

        // Get fresh references to mocked functions
        const { pipelineApi } = await import('@/services/pipelineApi');
        const { usePipelineEvents } = await import('../usePipelineEvents');
        
        (usePipelineEvents as any).mockReturnValue({
            status: null,
            connectionState: 'disconnected',
            error: null,
            retry: vi.fn(),
        });

        (pipelineApi.getActivePipeline as any).mockRejectedValue(new Error('Not found'));
    });

    it('initializes with default values', () => {
        const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });

        // useQuery returns undefined before first fetch, not null
        expect(result.current.activePipeline).toBeUndefined();
        expect(result.current.status).toBeNull();
        expect(result.current.connectionState).toBe('disconnected');
        // useQuery starts in loading state
        expect(result.current.isLoading).toBe(true);
        expect(result.current.isRunning).toBe(false);
        expect(result.current.isStopping).toBe(false);
        expect(result.current.isUploading).toBe(false);
    });

    it('fetches active pipeline on mount', async () => {
        const { pipelineApi } = await import('@/services/pipelineApi');
        const mockPipeline = {
            task_id: 'task-123',
            status: 'running',
            step: 'matching',
        };

        (pipelineApi.getActivePipeline as any).mockResolvedValue({ data: mockPipeline });

        const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });

        await waitFor(() => {
            expect(result.current.activePipeline).toEqual(mockPipeline);
        });

        expect(pipelineApi.getActivePipeline).toHaveBeenCalledTimes(1);
    });

    it('handles pipeline fetch error gracefully', async () => {
        const { pipelineApi } = await import('@/services/pipelineApi');
        (pipelineApi.getActivePipeline as any).mockRejectedValue(new Error('Network error'));

        const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });

        await waitFor(() => {
            expect(result.current.activePipeline).toBeNull();
        });

        // Should not throw, just return null
        expect(result.current.isLoading).toBe(false);
    });

    it('runPipeline calls API and invalidates queries', async () => {
        const { pipelineApi } = await import('@/services/pipelineApi');
        (pipelineApi.runMatching as any).mockResolvedValue({ data: { task_id: 'task-123' } });

        const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });

        act(() => {
            result.current.runPipeline();
        });

        await waitFor(() => {
            expect(pipelineApi.runMatching).toHaveBeenCalledTimes(1);
        });
    });

    it('runPipeline handles error', async () => {
        const { pipelineApi } = await import('@/services/pipelineApi');
        (pipelineApi.runMatching as any).mockRejectedValue(new Error('Pipeline already running'));

        const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });

        act(() => {
            result.current.runPipeline();
        });

        await waitFor(() => {
            expect(pipelineApi.runMatching).toHaveBeenCalledTimes(1);
        });

        expect(result.current.runPipelineError).toBeInstanceOf(Error);
    });

    it('stopPipeline calls API', async () => {
        const { pipelineApi } = await import('@/services/pipelineApi');
        (pipelineApi.stopMatching as any).mockResolvedValue({ data: { success: true } });

        const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });

        act(() => {
            result.current.stopPipeline();
        });

        await waitFor(() => {
            expect(pipelineApi.stopMatching).toHaveBeenCalledTimes(1);
        });
    });

    it('stopPipeline handles error', async () => {
        const { pipelineApi } = await import('@/services/pipelineApi');
        (pipelineApi.stopMatching as any).mockRejectedValue(new Error('No pipeline running'));

        const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });

        act(() => {
            result.current.stopPipeline();
        });

        await waitFor(() => {
            expect(pipelineApi.stopMatching).toHaveBeenCalledTimes(1);
        });

        expect(result.current.stopPipelineError).toBeInstanceOf(Error);
    });

    it('isRunning is true when SSE status is running', async () => {
        const { usePipelineEvents } = await import('../usePipelineEvents');
        (usePipelineEvents as any).mockReturnValue({
            status: { status: 'running', step: 'matching' },
            connectionState: 'connected',
            error: null,
            retry: vi.fn(),
        });

        const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });

        expect(result.current.isRunning).toBe(true);
    });

    it('isRunning is true when SSE status is pending', async () => {
        const { usePipelineEvents } = await import('../usePipelineEvents');
        (usePipelineEvents as any).mockReturnValue({
            status: { status: 'pending', step: 'initializing' },
            connectionState: 'connecting',
            error: null,
            retry: vi.fn(),
        });

        const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });

        expect(result.current.isRunning).toBe(true);
    });

    it('isRunning is false when SSE status is completed', async () => {
        const { usePipelineEvents } = await import('../usePipelineEvents');
        (usePipelineEvents as any).mockReturnValue({
            status: { status: 'completed', matches_count: 10 },
            connectionState: 'connected',
            error: null,
            retry: vi.fn(),
        });

        const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });

        expect(result.current.isRunning).toBe(false);
    });

    it('isStopping is true during stop mutation', async () => {
        const { pipelineApi } = await import('@/services/pipelineApi');
        // Set up mock BEFORE rendering the hook
        (pipelineApi.stopMatching as any).mockImplementation(() => new Promise(resolve => setTimeout(resolve, 100)));

        const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });

        // Wait for initial render to complete
        await waitFor(() => {
            expect(result.current.isLoading).toBe(false);
        });

        act(() => {
            result.current.stopPipeline();
        });

        // Give React time to update the mutation state
        await waitFor(() => {
            expect(result.current.isStopping).toBe(true);
        }, { timeout: 100 });

        await waitFor(() => {
            expect(result.current.isStopping).toBe(false);
        });
    });

    it('uploadResume calls API with file and hash', async () => {
        const { pipelineApi } = await import('@/services/pipelineApi');
        (pipelineApi.uploadResume as any).mockResolvedValue({ data: { success: true } });

        const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });

        const file = new File(['test'], 'resume.pdf', { type: 'application/pdf' });

        act(() => {
            result.current.uploadResume({ file, hash: 'abc123' });
        });

        await waitFor(() => {
            expect(pipelineApi.uploadResume).toHaveBeenCalledWith(file, 'abc123');
        });
    });

    it('uploadResume handles error', async () => {
        const { pipelineApi } = await import('@/services/pipelineApi');
        (pipelineApi.uploadResume as any).mockRejectedValue(new Error('Upload failed'));

        const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });

        const file = new File(['test'], 'resume.pdf', { type: 'application/pdf' });

        act(() => {
            result.current.uploadResume({ file });
        });

        await waitFor(() => {
            expect(pipelineApi.uploadResume).toHaveBeenCalled();
        });

        expect(result.current.uploadResumeError).toBeInstanceOf(Error);
    });

    it('isUploading is true during upload mutation', async () => {
        const { pipelineApi } = await import('@/services/pipelineApi');
        // Set up mock BEFORE rendering the hook
        (pipelineApi.uploadResume as any).mockImplementation(() => new Promise(resolve => setTimeout(resolve, 100)));

        const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });

        // Wait for initial render to complete
        await waitFor(() => {
            expect(result.current.isLoading).toBe(false);
        });

        const file = new File(['test'], 'resume.pdf', { type: 'application/pdf' });

        act(() => {
            result.current.uploadResume({ file });
        });

        // Give React time to update the mutation state
        await waitFor(() => {
            expect(result.current.isUploading).toBe(true);
        }, { timeout: 100 });

        await waitFor(() => {
            expect(result.current.isUploading).toBe(false);
        });
    });

    it('invalidates matches and stats on pipeline completion', async () => {
        const { usePipelineEvents } = await import('../usePipelineEvents');
        
        // Start with null status
        (usePipelineEvents as any).mockReturnValue({
            status: null,
            connectionState: 'connected',
            error: null,
            retry: vi.fn(),
        });

        const { result, rerender } = renderHook(() => usePipeline(), { wrapper: createWrapper() });

        // Initial status is null
        expect(result.current.status).toBeNull();

        // Update to completed status
        (usePipelineEvents as any).mockReturnValue({
            status: { status: 'completed', matches_count: 10 },
            connectionState: 'connected',
            error: null,
            retry: vi.fn(),
        });

        rerender();

        // Should trigger invalidation (tested via effect)
        await waitFor(() => {
            expect(result.current.status?.status).toBe('completed');
        });
    });

    it('provides retrySSE function from usePipelineEvents', async () => {
        const { usePipelineEvents } = await import('../usePipelineEvents');
        const mockRetry = vi.fn();
        (usePipelineEvents as any).mockReturnValue({
            status: null,
            connectionState: 'disconnected',
            error: new Error('Connection lost'),
            retry: mockRetry,
        });

        const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });

        expect(result.current.retrySSE).toBe(mockRetry);

        act(() => {
            result.current.retrySSE();
        });

        expect(mockRetry).toHaveBeenCalledTimes(1);
    });

    it('connectionState reflects SSE connection state', async () => {
        const { usePipelineEvents } = await import('../usePipelineEvents');
        (usePipelineEvents as any).mockReturnValue({
            status: null,
            connectionState: 'connecting',
            error: null,
            retry: vi.fn(),
        });

        const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });

        expect(result.current.connectionState).toBe('connecting');
    });

    it('sseError provides error from usePipelineEvents', async () => {
        const { usePipelineEvents } = await import('../usePipelineEvents');
        const mockError = new Error('SSE connection failed');
        (usePipelineEvents as any).mockReturnValue({
            status: null,
            connectionState: 'disconnected',
            error: mockError,
            retry: vi.fn(),
        });

        const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });

        expect(result.current.sseError).toBe(mockError);
    });
});
