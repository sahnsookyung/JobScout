/**
 * Unit tests for usePipelineEvents hook
 *
 * Tests the SSE connection management, retry logic, and state handling
 * for real-time pipeline status updates.
 */

import { renderHook, waitFor, act } from '@testing-library/react';
import { usePipelineEvents } from '../usePipelineEvents';
import type { PipelineStatusResponse } from '@/types/api';

// Mock EventSource for SSE testing
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
        this.onclose?.();
    }

    onclose?: () => void;

    // Test helpers
    simulateOpen(): void {
        this.onopen?.();
    }

    simulateMessage(data: object): void {
        this.onmessage?.({ data: JSON.stringify(data) } as MessageEvent);
    }

    simulateError(): void {
        this.onerror?.();
    }
}

// Replace global EventSource with mock
const OriginalEventSource = (global as unknown as { EventSource?: unknown }).EventSource;

describe('usePipelineEvents', () => {
    const mockTaskId = 'test-task-123';
    const mockOptions = {
        maxRetries: 3,
        baseDelay: 100,
        maxDelay: 500,
    };

    let eventSourceInstances: MockEventSource[] = [];

    beforeEach(() => {
        vi.useFakeTimers();
        eventSourceInstances = [];

        // Mock EventSource constructor
        vi.stubGlobal(
            'EventSource',
            vi.fn((url: string) => {
                const instance = new MockEventSource(url);
                eventSourceInstances.push(instance);
                return instance;
            })
        );
    });

    afterEach(() => {
        vi.useRealTimers();
        vi.clearAllMocks();
        vi.restoreAllMocks();

        // Restore original EventSource if it existed
        if (OriginalEventSource) {
            vi.stubGlobal('EventSource', OriginalEventSource);
        } else {
            vi.unstubAllGlobals();
        }
    });

    const getLastEventSource = () => eventSourceInstances[eventSourceInstances.length - 1];

    describe('Initial Connection', () => {
        it('should start in disconnected state when no taskId', () => {
            const { result } = renderHook(() => usePipelineEvents(null, mockOptions));

            expect(result.current.connectionState).toBe('disconnected');
            expect(result.current.status).toBeNull();
            expect(result.current.error).toBeNull();
        });

        it('should connect when taskId is provided', () => {
            const { result } = renderHook(() => usePipelineEvents(mockTaskId, mockOptions));

            expect(EventSource).toHaveBeenCalledWith(`/api/pipeline/events/${mockTaskId}`);
            expect(result.current.connectionState).toBe('connecting');
        });

        it('should transition to connected state on successful connection', async () => {
            const { result } = renderHook(() => usePipelineEvents(mockTaskId, mockOptions));

            const eventSource = getLastEventSource();
            act(() => {
                eventSource.simulateOpen();
            });

            expect(result.current.connectionState).toBe('connected');
            expect(result.current.error).toBeNull();
        });

        it('should set error state on connection failure', async () => {
            const { result } = renderHook(() => usePipelineEvents(mockTaskId, mockOptions));

            act(() => {
                getLastEventSource().simulateError();
            });

            await waitFor(() => {
                expect(result.current.connectionState).toBe('reconnecting');
            });
            expect(result.current.error).toContain('Connection lost');
        });
    });

    describe('Message Handling', () => {
        const mockStatusData: PipelineStatusResponse = {
            task_id: mockTaskId,
            status: 'running',
            step: 'extracting',
            message: 'Processing resume',
            created_at: '2024-01-01T00:00:00Z',
        };

        it('should update status on receiving message', async () => {
            const { result } = renderHook(() => usePipelineEvents(mockTaskId, mockOptions));

            const eventSource = getLastEventSource();
            act(() => {
                eventSource.simulateOpen();
                eventSource.simulateMessage(mockStatusData);
            });

            expect(result.current.status).toEqual(mockStatusData);
        });

        it('should ignore heartbeat messages', async () => {
            const { result } = renderHook(() => usePipelineEvents(mockTaskId, mockOptions));

            const eventSource = getLastEventSource();
            act(() => {
                eventSource.simulateOpen();
                eventSource.simulateMessage({ type: 'heartbeat' });
            });

            expect(result.current.status).toBeNull();
        });

        it('should disconnect on completed status', async () => {
            const { result } = renderHook(() => usePipelineEvents(mockTaskId, mockOptions));

            const completedData: PipelineStatusResponse = {
                ...mockStatusData,
                status: 'completed',
            };

            const eventSource = getLastEventSource();
            act(() => {
                eventSource.simulateOpen();
                eventSource.simulateMessage(completedData);
            });

            await waitFor(() => {
                expect(result.current.connectionState).toBe('disconnected');
            });
            expect(result.current.status?.status).toBe('completed');
        });

        it('should disconnect on failed status', async () => {
            const { result } = renderHook(() => usePipelineEvents(mockTaskId, mockOptions));

            const failedData: PipelineStatusResponse = {
                ...mockStatusData,
                status: 'failed',
            };

            const eventSource = getLastEventSource();
            act(() => {
                eventSource.simulateOpen();
                eventSource.simulateMessage(failedData);
            });

            await waitFor(() => {
                expect(result.current.connectionState).toBe('disconnected');
            });
            expect(result.current.status?.status).toBe('failed');
        });
    });

    describe('Retry Logic', () => {
        it('should retry on connection error', async () => {
            const { result } = renderHook(() => usePipelineEvents(mockTaskId, mockOptions));

            // First connection fails
            act(() => {
                getLastEventSource().simulateError();
            });

            await waitFor(() => {
                expect(result.current.connectionState).toBe('reconnecting');
            });

            // Fast-forward retry delay
            act(() => {
                vi.advanceTimersByTime(mockOptions.baseDelay);
            });

            await waitFor(() => {
                expect(eventSourceInstances.length).toBeGreaterThan(1);
            });
        });

        it('should use exponential backoff for retries', async () => {
            const { result } = renderHook(() => usePipelineEvents(mockTaskId, mockOptions));

            // First error
            act(() => {
                getLastEventSource().simulateError();
            });

            await waitFor(() => {
                expect(result.current.retryCount).toBe(0);
            });

            act(() => {
                vi.advanceTimersByTime(mockOptions.baseDelay);
            });

            // Second error
            await waitFor(() => {
                expect(eventSourceInstances.length).toBe(2);
            });

            act(() => {
                getLastEventSource().simulateError();
            });

            // Should wait longer for second retry (baseDelay * 2^1)
            act(() => {
                vi.advanceTimersByTime(mockOptions.baseDelay * 2);
            });

            await waitFor(() => {
                expect(eventSourceInstances.length).toBe(3);
            });
        });

        it('should fail after max retries', async () => {
            const { result } = renderHook(() => usePipelineEvents(mockTaskId, mockOptions));

            // Fail maxRetries times
            for (let i = 0; i < mockOptions.maxRetries; i++) {
                // eslint-disable-next-line no-await-in-loop
                await waitFor(() => {
                    expect(eventSourceInstances.length).toBeGreaterThanOrEqual(i + 1);
                });

                act(() => {
                    getLastEventSource().simulateError();
                });

                if (i < mockOptions.maxRetries - 1) {
                    act(() => {
                        vi.advanceTimersByTime(mockOptions.baseDelay * Math.pow(2, i));
                    });
                }
            }

            await waitFor(() => {
                expect(result.current.connectionState).toBe('failed');
            });
            expect(result.current.error).toContain('after');
            expect(result.current.error).toContain('attempts');
        });

        it('should reset retry count on successful reconnection', async () => {
            const { result } = renderHook(() => usePipelineEvents(mockTaskId, mockOptions));

            // First connection fails
            act(() => {
                getLastEventSource().simulateError();
            });

            await waitFor(() => {
                expect(result.current.retryCount).toBe(0);
            });

            act(() => {
                vi.advanceTimersByTime(mockOptions.baseDelay);
            });

            // Successful reconnection
            await waitFor(() => {
                expect(eventSourceInstances.length).toBe(2);
            });

            act(() => {
                getLastEventSource().simulateOpen();
            });

            expect(result.current.retryCount).toBe(0);
            expect(result.current.connectionState).toBe('connected');
        });
    });

    describe('Manual Retry', () => {
        it('should allow manual retry via retry function', async () => {
            const { result } = renderHook(() => usePipelineEvents(mockTaskId, mockOptions));

            // Initial connection fails
            act(() => {
                getLastEventSource().simulateError();
            });

            await waitFor(() => {
                expect(result.current.error).toBeTruthy();
            });

            // Manual retry
            act(() => {
                result.current.retry();
            });

            await waitFor(() => {
                expect(eventSourceInstances.length).toBeGreaterThan(1);
            });

            expect(result.current.error).toBeNull();
        });
    });

    describe('Cleanup', () => {
        it('should disconnect when taskId becomes null', async () => {
            const { result, rerender } = renderHook(
                ({ taskId }) => usePipelineEvents(taskId, mockOptions),
                { initialProps: { taskId: mockTaskId } }
            );

            act(() => {
                getLastEventSource().simulateOpen();
            });

            expect(result.current.connectionState).toBe('connected');

            // Change taskId to null
            rerender({ taskId: null });

            await waitFor(() => {
                expect(result.current.connectionState).toBe('disconnected');
            });
            expect(result.current.status).toBeNull();
        });

        it('should close EventSource on unmount', async () => {
            const closeSpy = vi.spyOn(MockEventSource.prototype, 'close');

            const { unmount } = renderHook(() => usePipelineEvents(mockTaskId, mockOptions));

            unmount();

            expect(closeSpy).toHaveBeenCalled();
        });

        it('should not update state after unmount', async () => {
            const { result, unmount } = renderHook(() => usePipelineEvents(mockTaskId, mockOptions));

            unmount();

            // Simulate message after unmount
            act(() => {
                getLastEventSource().simulateMessage({
                    status: 'completed',
                    task_id: mockTaskId,
                    step: 'done',
                    message: 'Done',
                    created_at: '2024-01-01T00:00:00Z',
                });
            });

            // State should not have changed
            expect(result.current.status).toBeNull();
        });
    });

    describe('Default Options', () => {
        it('should use default options when not provided', () => {
            renderHook(() => usePipelineEvents(mockTaskId));

            expect(EventSource).toHaveBeenCalledWith(`/api/pipeline/events/${mockTaskId}`);
        });

        it('should merge partial options with defaults', () => {
            renderHook(() => usePipelineEvents(mockTaskId, { maxRetries: 10 }));

            // Should use custom maxRetries but default baseDelay
            expect(EventSource).toHaveBeenCalled();
        });
    });
});
