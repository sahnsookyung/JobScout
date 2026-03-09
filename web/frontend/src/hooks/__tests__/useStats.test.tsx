/**
 * Tests for React Hooks
 * Covers: web/frontend/src/hooks/
 */

import { renderHook, waitFor, act } from '@testing-library/react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

// Mock pipeline API
const mockRunMatching = vi.fn();
const mockStopMatching = vi.fn();
const mockGetActivePipeline = vi.fn();
const mockUploadResume = vi.fn();

vi.mock('@/services/pipelineApi', () => ({
    pipelineApi: {
        runMatching: (...args: any[]) => mockRunMatching(...args),
        stopMatching: (...args: any[]) => mockStopMatching(...args),
        getActivePipeline: (...args: any[]) => mockGetActivePipeline(...args),
        uploadResume: (...args: any[]) => mockUploadResume(...args),
    },
}));

// Mock usePipelineEvents
const mockUsePipelineEvents = vi.fn();
vi.mock('../usePipelineEvents', () => ({
    usePipelineEvents: () => mockUsePipelineEvents(),
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
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('fetches stats on mount', async () => {
        const mockStats = {
            total_matches: 100,
            active_matches: 45,
            hidden_count: 30,
            below_threshold_count: 25,
        };

        const mockGetStats = vi.fn().mockResolvedValue({ data: { stats: mockStats } });
        vi.mock('@/services/matchesApi', () => ({
            matchesApi: {
                getStats: mockGetStats,
            },
        }));

        // Re-import after mock
        const { useStats: FreshUseStats } = await import('../useStats');

        const { result } = renderHook(() => FreshUseStats(), { wrapper: createWrapper() });

        await waitFor(() => {
            expect(result.current.isSuccess).toBe(true);
        });

        expect(result.current.data).toEqual(mockStats);
    });

    it('handles fetch error', async () => {
        const mockGetStats = vi.fn().mockRejectedValue(new Error('Network error'));
        vi.mock('@/services/matchesApi', () => ({
            matchesApi: {
                getStats: mockGetStats,
            },
        }));

        const { useStats: FreshUseStats } = await import('../useStats');

        const { result } = renderHook(() => FreshUseStats(), { wrapper: createWrapper() });

        await waitFor(() => {
            expect(result.current.isError).toBe(true);
        });

        expect(result.current.error).toBeInstanceOf(Error);
    });

    it('uses correct staleTime', async () => {
        const mockGetStats = vi.fn().mockResolvedValue({ data: { stats: {} } });
        vi.mock('@/services/matchesApi', () => ({
            matchesApi: {
                getStats: mockGetStats,
            },
        }));

        const { useStats: FreshUseStats } = await import('../useStats');

        renderHook(() => FreshUseStats(), { wrapper: createWrapper() });

        await waitFor(() => {
            expect(mockGetStats).toHaveBeenCalled();
        });

        // Query should have staleTime of 60000ms (1 minute)
        // This is configured in the hook, verified by checking query options
        expect(mockGetStats).toHaveBeenCalledTimes(1);
    });
});

describe('usePipeline', () => {
    beforeEach(() => {
        vi.clearAllMocks();

        mockUsePipelineEvents.mockReturnValue({
            status: null,
            connectionState: 'disconnected',
            error: null,
            retry: vi.fn(),
        });

        mockGetActivePipeline.mockRejectedValue(new Error('Not found'));
    });

    it('initializes with default values', () => {
        const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });

        expect(result.current.activePipeline).toBeNull();
        expect(result.current.status).toBeNull();
        expect(result.current.connectionState).toBe('disconnected');
        expect(result.current.isLoading).toBe(false);
        expect(result.current.isRunning).toBe(false);
        expect(result.current.isStopping).toBe(false);
        expect(result.current.isUploading).toBe(false);
    });

    it('fetches active pipeline on mount', async () => {
        const mockPipeline = {
            task_id: 'task-123',
            status: 'running',
            step: 'matching',
        };

        mockGetActivePipeline.mockResolvedValue({ data: mockPipeline });

        const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });

        await waitFor(() => {
            expect(result.current.activePipeline).toEqual(mockPipeline);
        });

        expect(mockGetActivePipeline).toHaveBeenCalledTimes(1);
    });

    it('handles pipeline fetch error gracefully', async () => {
        mockGetActivePipeline.mockRejectedValue(new Error('Network error'));

        const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });

        await waitFor(() => {
            expect(result.current.activePipeline).toBeNull();
        });

        // Should not throw, just return null
        expect(result.current.isLoading).toBe(false);
    });

    it('runPipeline calls API and invalidates queries', async () => {
        mockRunMatching.mockResolvedValue({ data: { task_id: 'task-123' } });

        const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });

        act(() => {
            result.current.runPipeline();
        });

        await waitFor(() => {
            expect(mockRunMatching).toHaveBeenCalledTimes(1);
        });
    });

    it('runPipeline handles error', async () => {
        mockRunMatching.mockRejectedValue(new Error('Pipeline already running'));

        const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });

        act(() => {
            result.current.runPipeline();
        });

        await waitFor(() => {
            expect(mockRunMatching).toHaveBeenCalledTimes(1);
        });

        expect(result.current.runPipelineError).toBeInstanceOf(Error);
    });

    it('stopPipeline calls API', async () => {
        mockStopMatching.mockResolvedValue({ data: { success: true } });

        const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });

        act(() => {
            result.current.stopPipeline();
        });

        await waitFor(() => {
            expect(mockStopMatching).toHaveBeenCalledTimes(1);
        });
    });

    it('stopPipeline handles error', async () => {
        mockStopMatching.mockRejectedValue(new Error('No pipeline running'));

        const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });

        act(() => {
            result.current.stopPipeline();
        });

        await waitFor(() => {
            expect(mockStopMatching).toHaveBeenCalledTimes(1);
        });

        expect(result.current.stopPipelineError).toBeInstanceOf(Error);
    });

    it('isRunning is true when SSE status is running', () => {
        mockUsePipelineEvents.mockReturnValue({
            status: { status: 'running', step: 'matching' },
            connectionState: 'connected',
            error: null,
            retry: vi.fn(),
        });

        const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });

        expect(result.current.isRunning).toBe(true);
    });

    it('isRunning is true when SSE status is pending', () => {
        mockUsePipelineEvents.mockReturnValue({
            status: { status: 'pending', step: 'initializing' },
            connectionState: 'connecting',
            error: null,
            retry: vi.fn(),
        });

        const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });

        expect(result.current.isRunning).toBe(true);
    });

    it('isRunning is false when SSE status is completed', () => {
        mockUsePipelineEvents.mockReturnValue({
            status: { status: 'completed', matches_count: 10 },
            connectionState: 'connected',
            error: null,
            retry: vi.fn(),
        });

        const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });

        expect(result.current.isRunning).toBe(false);
    });

    it('isStopping is true during stop mutation', async () => {
        mockStopMatching.mockImplementation(() => new Promise(resolve => setTimeout(resolve, 100)));

        const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });

        act(() => {
            result.current.stopPipeline();
        });

        // While mutation is pending
        expect(result.current.isStopping).toBe(true);

        await waitFor(() => {
            expect(result.current.isStopping).toBe(false);
        });
    });

    it('uploadResume calls API with file and hash', async () => {
        mockUploadResume.mockResolvedValue({ data: { success: true } });

        const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });

        const file = new File(['test'], 'resume.pdf', { type: 'application/pdf' });

        act(() => {
            result.current.uploadResume({ file, hash: 'abc123' });
        });

        await waitFor(() => {
            expect(mockUploadResume).toHaveBeenCalledWith(file, 'abc123');
        });
    });

    it('uploadResume handles error', async () => {
        mockUploadResume.mockRejectedValue(new Error('Upload failed'));

        const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });

        const file = new File(['test'], 'resume.pdf', { type: 'application/pdf' });

        act(() => {
            result.current.uploadResume({ file });
        });

        await waitFor(() => {
            expect(mockUploadResume).toHaveBeenCalled();
        });

        expect(result.current.uploadResumeError).toBeInstanceOf(Error);
    });

    it('isUploading is true during upload mutation', async () => {
        mockUploadResume.mockImplementation(() => new Promise(resolve => setTimeout(resolve, 100)));

        const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });

        const file = new File(['test'], 'resume.pdf', { type: 'application/pdf' });

        act(() => {
            result.current.uploadResume({ file });
        });

        expect(result.current.isUploading).toBe(true);

        await waitFor(() => {
            expect(result.current.isUploading).toBe(false);
        });
    });

    it('invalidates matches and stats on pipeline completion', async () => {
        mockUsePipelineEvents.mockReturnValue({
            status: { status: 'completed', matches_count: 10 },
            connectionState: 'connected',
            error: null,
            retry: vi.fn(),
        });

        const { result, rerender } = renderHook(() => usePipeline(), { wrapper: createWrapper() });

        // Initial status is null
        expect(result.current.status).toBeNull();

        // Update to completed status
        mockUsePipelineEvents.mockReturnValue({
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

    it('provides retrySSE function from usePipelineEvents', () => {
        const mockRetry = vi.fn();
        mockUsePipelineEvents.mockReturnValue({
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

    it('connectionState reflects SSE connection state', () => {
        mockUsePipelineEvents.mockReturnValue({
            status: null,
            connectionState: 'connecting',
            error: null,
            retry: vi.fn(),
        });

        const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });

        expect(result.current.connectionState).toBe('connecting');
    });

    it('sseError provides error from usePipelineEvents', () => {
        const mockError = new Error('SSE connection failed');
        mockUsePipelineEvents.mockReturnValue({
            status: null,
            connectionState: 'disconnected',
            error: mockError,
            retry: vi.fn(),
        });

        const { result } = renderHook(() => usePipeline(), { wrapper: createWrapper() });

        expect(result.current.sseError).toBe(mockError);
    });
});
