import { act, renderHook, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { pipelineApi } from '@/services/pipelineApi';
import { usePipelineEvents } from '../usePipelineEvents';
import type { PipelineStatusResponse } from '@/types/api';

vi.mock('@/services/pipelineApi', () => ({
    pipelineApi: {
        getPipelineStatus: vi.fn(),
    },
}));

const statusResponse = (
    taskId: string,
    status: PipelineStatusResponse['status']
): PipelineStatusResponse => ({ task_id: taskId, status });

describe('usePipelineEvents authenticated status polling', () => {
    beforeEach(() => {
        vi.useFakeTimers({ shouldAdvanceTime: true });
        vi.mocked(pipelineApi.getPipelineStatus).mockReset();
    });

    afterEach(() => {
        vi.clearAllTimers();
        vi.useRealTimers();
    });

    it('does not request status without a task and exposes retry controls', () => {
        const { result } = renderHook(() => usePipelineEvents(null));

        expect(result.current.connectionState).toBe('disconnected');
        expect(result.current.status).toBeNull();
        expect(result.current.error).toBeNull();
        expect(typeof result.current.retry).toBe('function');
        expect(typeof result.current.disconnect).toBe('function');
        expect(pipelineApi.getPipelineStatus).not.toHaveBeenCalled();
    });

    it('uses the authenticated API client for task status', async () => {
        const response = statusResponse('tenant-task', 'running');
        vi.mocked(pipelineApi.getPipelineStatus).mockResolvedValue({ data: response } as never);

        const { result, unmount } = renderHook(() => usePipelineEvents('tenant-task', {
            pollInterval: 60_000,
        }));

        await waitFor(() => expect(result.current.status).toEqual(response));
        expect(pipelineApi.getPipelineStatus).toHaveBeenCalledWith(
            'tenant-task',
            expect.any(AbortSignal)
        );
        unmount();
    });

    it('recovers from a lost status request with bounded backoff polling', async () => {
        vi.mocked(pipelineApi.getPipelineStatus)
            .mockRejectedValueOnce(new Error('network unavailable'))
            .mockResolvedValueOnce({ data: statusResponse('task-1', 'running') } as never);

        const { result, unmount } = renderHook(() => usePipelineEvents('task-1', {
            maxRetries: 2,
            baseDelay: 50,
            maxDelay: 100,
            pollInterval: 60_000,
        }));

        await act(async () => {
            await Promise.resolve();
        });
        expect(result.current.connectionState).toBe('reconnecting');
        expect(result.current.error).toContain('Retrying (1/2)');

        await act(async () => {
            await vi.advanceTimersByTimeAsync(50);
        });

        await waitFor(() => expect(result.current.status?.status).toBe('running'));
        expect(result.current.connectionState).toBe('connected');
        expect(pipelineApi.getPipelineStatus).toHaveBeenCalledTimes(2);
        unmount();
    });

    it('stops after the configured transient retry limit', async () => {
        vi.mocked(pipelineApi.getPipelineStatus).mockRejectedValue(new Error('offline'));
        const { result, unmount } = renderHook(() => usePipelineEvents('task-1', {
            maxRetries: 2,
            baseDelay: 10,
            maxDelay: 20,
        }));

        await waitFor(() => expect(result.current.connectionState).toBe('failed'));
        expect(result.current.error).toContain('after 2 retries');
        expect(pipelineApi.getPipelineStatus).toHaveBeenCalledTimes(3);
        unmount();
    });

    it.each([
        [401, 'session expired'],
        [403, 'cannot access this run'],
    ])('treats HTTP %s as a visible terminal transport error', async (httpStatus, message) => {
        vi.mocked(pipelineApi.getPipelineStatus).mockRejectedValue({
            response: { status: httpStatus },
        });
        const { result, unmount } = renderHook(() => usePipelineEvents('task-1', {
            maxRetries: 4,
            baseDelay: 10,
        }));

        await waitFor(() => expect(result.current.connectionState).toBe('failed'));
        expect(result.current.error).toContain(message);
        expect(pipelineApi.getPipelineStatus).toHaveBeenCalledTimes(1);
        unmount();
    });

    it('aborts stale requests on task changes and ignores their late responses', async () => {
        let resolveOldRequest!: (value: { data: PipelineStatusResponse }) => void;
        let oldSignal: AbortSignal | undefined;
        let newSignal: AbortSignal | undefined;
        vi.mocked(pipelineApi.getPipelineStatus).mockImplementation((taskId, signal) => {
            if (taskId === 'old-task') {
                oldSignal = signal;
                return new Promise((resolve) => { resolveOldRequest = resolve; }) as never;
            }
            newSignal = signal;
            return Promise.resolve({ data: statusResponse('new-task', 'running') }) as never;
        });

        const { result, rerender, unmount } = renderHook(
            ({ taskId }: { taskId: string }) => usePipelineEvents(taskId),
            { initialProps: { taskId: 'old-task' } }
        );
        await waitFor(() => expect(pipelineApi.getPipelineStatus).toHaveBeenCalledWith(
            'old-task', expect.anything()
        ));

        rerender({ taskId: 'new-task' });
        await waitFor(() => expect(result.current.status?.task_id).toBe('new-task'));
        expect(oldSignal?.aborted).toBe(true);

        await act(async () => {
            resolveOldRequest({ data: statusResponse('old-task', 'failed') });
        });
        expect(result.current.status?.task_id).toBe('new-task');

        unmount();
        expect(newSignal?.aborted).toBe(true);
    });

    it('aborts an in-flight status request on unmount', async () => {
        let signal: AbortSignal | undefined;
        vi.mocked(pipelineApi.getPipelineStatus).mockImplementation((_taskId, requestSignal) => {
            signal = requestSignal;
            return new Promise(() => {}) as never;
        });
        const { unmount } = renderHook(() => usePipelineEvents('task-1'));

        await waitFor(() => expect(signal).toBeDefined());
        unmount();

        expect(signal?.aborted).toBe(true);
    });

    it('stops polling after it receives a terminal task status', async () => {
        vi.mocked(pipelineApi.getPipelineStatus).mockResolvedValue({
            data: statusResponse('task-1', 'completed'),
        } as never);
        const { result, unmount } = renderHook(() => usePipelineEvents('task-1', {
            pollInterval: 100,
        }));

        await waitFor(() => expect(result.current.status?.status).toBe('completed'));
        await act(async () => {
            await vi.advanceTimersByTimeAsync(500);
        });

        expect(result.current.connectionState).toBe('disconnected');
        expect(pipelineApi.getPipelineStatus).toHaveBeenCalledTimes(1);
        unmount();
    });
});
