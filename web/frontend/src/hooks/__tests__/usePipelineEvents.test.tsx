/**
 * Tests for usePipelineEvents hook
 * Covers: usePipelineEvents.ts
 */

import { renderHook, waitFor, act } from '@testing-library/react';
import { usePipelineEvents } from '../usePipelineEvents';
import type { PipelineStatusResponse } from '@/types/api';

// Helper to flush pending promises
const flushPromises = () => new Promise(resolve => setTimeout(resolve, 0));

// Mock EventSource with precise typing
class MockEventSource {
    static readonly instances: MockEventSource[] = [];
    static readonly CONNECTING = 0;
    static readonly OPEN = 1;
    static readonly CLOSED = 2;
    readonly CONNECTING = 0;
    readonly OPEN = 1;
    readonly CLOSED = 2;

    onopen: (() => void) | null = null;
    onmessage: ((event: MessageEvent) => void) | null = null;
    onerror: (() => void) | null = null;

    constructor(public url: string) {
        MockEventSource.instances.push(this);
    }

    close(): void {
        // EventSource has no onclose event - don't call it
    }

    // Typed helper methods for tests
    simulateOpen(): void {
        this.onopen?.();
    }

    simulateMessage(data: PipelineStatusResponse | { type: 'heartbeat' }): void {
        this.onmessage?.({ data: JSON.stringify(data) } as MessageEvent);
    }

    simulateError(): void {
        this.onerror?.();
    }
}

vi.stubGlobal('EventSource', MockEventSource);

describe('usePipelineEvents', () => {
    beforeEach(() => {
        // Use fake timers with shouldAdvanceTime to allow waitFor to work
        vi.useFakeTimers({ shouldAdvanceTime: true });
        MockEventSource.instances = [];
    });

    afterEach(() => {
        // Flush pending timers before restoring
        vi.runOnlyPendingTimers();
        vi.useRealTimers();
        vi.restoreAllMocks();
        MockEventSource.instances = [];
    });

    const getLastEventSource = (): MockEventSource => MockEventSource.instances[MockEventSource.instances.length - 1];

    describe('initial state', () => {
        it('should start disconnected when no taskId', () => {
            const { result } = renderHook(() => usePipelineEvents(null));
            expect(result.current.connectionState).toBe('disconnected');
            expect(result.current.status).toBeNull();
            expect(result.current.error).toBeNull();
        });

        it('should have required functions', () => {
            const { result } = renderHook(() => usePipelineEvents(null));
            expect(typeof result.current.retry).toBe('function');
            expect(typeof result.current.disconnect).toBe('function');
        });
    });

    describe('connection', () => {
        it('should create EventSource when taskId provided', async () => {
            const { rerender } = renderHook(
                ({ taskId }) => usePipelineEvents(taskId),
                { initialProps: { taskId: null } }
            );

            rerender({ taskId: 'test-123' });

            await waitFor(() => {
                expect(MockEventSource.instances.length).toBe(1);
            });

            expect(getLastEventSource().url).toContain('test-123');
        });

        it('should close EventSource on unmount', async () => {
            const { unmount, rerender } = renderHook(
                ({ taskId }) => usePipelineEvents(taskId),
                { initialProps: { taskId: null } }
            );

            rerender({ taskId: 'test-123' });

            await waitFor(() => {
                expect(MockEventSource.instances.length).toBe(1);
            });

            const closeSpy = vi.spyOn(getLastEventSource(), 'close');
            unmount();
            expect(closeSpy).toHaveBeenCalled();
        });
    });

    describe('status updates', () => {
        it('should update status on message', async () => {
            const { result, rerender } = renderHook(
                ({ taskId }) => usePipelineEvents(taskId),
                { initialProps: { taskId: null } }
            );

            rerender({ taskId: 'test-123' });

            await waitFor(() => {
                expect(MockEventSource.instances.length).toBe(1);
            });

            const statusData: PipelineStatusResponse = {
                task_id: 'test-123',
                status: 'extracting',
                created_at: new Date().toISOString(),
            };

            await act(async () => {
                getLastEventSource().simulateMessage(statusData);
            });

            expect(result.current.status).toEqual(statusData);
        });

        it('should ignore heartbeat messages', async () => {
            const { result, rerender } = renderHook(
                ({ taskId }) => usePipelineEvents(taskId),
                { initialProps: { taskId: null } }
            );

            rerender({ taskId: 'test-123' });

            await waitFor(() => {
                expect(MockEventSource.instances.length).toBe(1);
            });

            await act(async () => {
                getLastEventSource().simulateMessage({ type: 'heartbeat' });
            });

            expect(result.current.status).toBeNull();
        });

        it('should disconnect on completed status', async () => {
            const { result, rerender } = renderHook(
                ({ taskId }) => usePipelineEvents(taskId),
                { initialProps: { taskId: null } }
            );

            rerender({ taskId: 'test-123' });

            await waitFor(() => {
                expect(MockEventSource.instances.length).toBe(1);
            });

            await act(async () => {
                getLastEventSource().simulateOpen();
                getLastEventSource().simulateMessage({
                    task_id: 'test-123',
                    status: 'completed',
                    created_at: new Date().toISOString(),
                });
            });

            expect(result.current.connectionState).toBe('disconnected');
        });

        it('should disconnect on failed status', async () => {
            const { result, rerender } = renderHook(
                ({ taskId }) => usePipelineEvents(taskId),
                { initialProps: { taskId: null } }
            );

            rerender({ taskId: 'test-123' });

            await waitFor(() => {
                expect(MockEventSource.instances.length).toBe(1);
            });

            await act(async () => {
                getLastEventSource().simulateOpen();
                getLastEventSource().simulateMessage({
                    task_id: 'test-123',
                    status: 'failed',
                    created_at: new Date().toISOString(),
                });
            });

            expect(result.current.connectionState).toBe('disconnected');
        });
    });

    describe('error handling', () => {
        it('should handle onerror without crashing', async () => {
            const { rerender } = renderHook(
                ({ taskId }) => usePipelineEvents(taskId, { maxRetries: 5, baseDelay: 100 }),
                { initialProps: { taskId: null } }
            );

            rerender({ taskId: 'test-123' });

            await waitFor(() => {
                expect(MockEventSource.instances.length).toBe(1);
            });

            expect(() => {
                getLastEventSource().simulateError();
            }).not.toThrow();
        });

        it('should retry on connection error', async () => {
            const { rerender } = renderHook(
                ({ taskId }) => usePipelineEvents(taskId, { maxRetries: 5, baseDelay: 1000 }),
                { initialProps: { taskId: null } }
            );

            rerender({ taskId: 'test-123' });

            await waitFor(() => {
                expect(MockEventSource.instances.length).toBe(1);
            });

            // Trigger error
            await act(async () => {
                getLastEventSource().simulateError();
            });

            // Fast-forward time to trigger retry
            await act(async () => {
                await vi.advanceTimersByTimeAsync(1000);
            });

            // Should have created a new EventSource for retry
            expect(MockEventSource.instances.length).toBeGreaterThanOrEqual(2);
        });

        it('should use exponential backoff for retries', async () => {
            const { rerender } = renderHook(
                ({ taskId }) => usePipelineEvents(taskId, { maxRetries: 5, baseDelay: 1000 }),
                { initialProps: { taskId: null } }
            );

            rerender({ taskId: 'test-123' });

            await waitFor(() => {
                expect(MockEventSource.instances.length).toBe(1);
            });

            const initialCount = MockEventSource.instances.length;

            // First error
            await act(async () => {
                getLastEventSource().simulateError();
            });

            // Fast-forward to trigger first retry (1000ms)
            await act(async () => {
                await vi.advanceTimersByTimeAsync(1000);
            });

            expect(MockEventSource.instances.length).toBe(initialCount + 1);

            // Second error
            if (MockEventSource.instances.length > initialCount) {
                await act(async () => {
                    getLastEventSource().simulateError();
                });

                // Fast-forward for second retry (should be 2000ms with exponential backoff)
                await act(async () => {
                    await vi.advanceTimersByTimeAsync(2000);
                });

                // Should have attempted another retry
                expect(MockEventSource.instances.length).toBeGreaterThanOrEqual(initialCount + 2);
            }
        });

        it('should fail after max retries', async () => {
            const { result, rerender } = renderHook(
                ({ taskId }) => usePipelineEvents(taskId, { maxRetries: 3, baseDelay: 10 }),
                { initialProps: { taskId: null } }
            );

            rerender({ taskId: 'test-123' });

            await waitFor(() => {
                expect(MockEventSource.instances.length).toBe(1);
            });

            // Trigger errors until max retries exceeded
            // Initial connection + 3 retries = 4 total attempts
            for (let i = 0; i < 4; i++) {
                const currentEs = getLastEventSource();
                if (currentEs) {
                    await act(async () => {
                        currentEs.simulateError();
                    });

                    // Wait for retry delay (exponential backoff)
                    if (i < 3) {
                        await act(async () => {
                            await vi.advanceTimersByTimeAsync(10 * Math.pow(2, i));
                        });
                        
                        // Wait for new EventSource to be created
                        await waitFor(() => {
                            expect(MockEventSource.instances.length).toBeGreaterThan(i + 1);
                        }, { timeout: 1000 });
                    }
                }
            }

            // Should be in failed state after all retries exhausted
            await waitFor(() => {
                expect(result.current.connectionState).toBe('failed');
            }, { timeout: 1000 });
            
            expect(result.current.error).toContain('after 3 attempts');
        });

        it('should reset retry count on successful reconnection', async () => {
            const { result, rerender } = renderHook(
                ({ taskId }) => usePipelineEvents(taskId, { maxRetries: 5, baseDelay: 100 }),
                { initialProps: { taskId: null } }
            );

            rerender({ taskId: 'test-123' });

            await waitFor(() => {
                expect(MockEventSource.instances.length).toBe(1);
            });

            // Trigger error
            await act(async () => {
                getLastEventSource().simulateError();
            });

            // Fast-forward to trigger retry
            await act(async () => {
                await vi.advanceTimersByTimeAsync(100);
            });

            // Verify retry count increased
            expect(result.current.retryCount).toBeGreaterThanOrEqual(1);

            // Successful reconnection
            await act(async () => {
                getLastEventSource().simulateOpen();
            });

            // Retry count should be reset
            expect(result.current.retryCount).toBe(0);
        });
    });

    describe('manual retry', () => {
        it('should allow manual retry via retry function', async () => {
            const { result, rerender } = renderHook(
                ({ taskId }) => usePipelineEvents(taskId, { maxRetries: 5, baseDelay: 100 }),
                { initialProps: { taskId: null } }
            );

            rerender({ taskId: 'test-123' });

            await waitFor(() => {
                expect(MockEventSource.instances.length).toBe(1);
            });

            // Trigger error
            await act(async () => {
                getLastEventSource().simulateError();
            });

            // Verify error state
            expect(result.current.error).toBeTruthy();

            // Manual retry
            await act(async () => {
                result.current.retry();
            });

            // Error should be cleared
            expect(result.current.error).toBeNull();
        });
    });

    describe('cleanup', () => {
        it('should disconnect when taskId becomes null', async () => {
            const { result, rerender } = renderHook(
                ({ taskId }) => usePipelineEvents(taskId),
                { initialProps: { taskId: 'test-123' } }
            );

            await waitFor(() => {
                expect(MockEventSource.instances.length).toBe(1);
            });

            // Remove taskId
            rerender({ taskId: null });

            expect(result.current.connectionState).toBe('disconnected');
            expect(result.current.status).toBeNull();
        });

        it('should not update state after unmount', async () => {
            const { result, unmount } = renderHook(
                ({ taskId }) => usePipelineEvents(taskId),
                { initialProps: { taskId: 'test-123' } }
            );

            await waitFor(() => {
                expect(MockEventSource.instances.length).toBe(1);
            });

            // Open the connection before unmounting
            await act(async () => {
                getLastEventSource().simulateOpen();
            });

            expect(result.current.connectionState).toBe('connected');

            unmount();

            // Message after unmount should not update state
            await act(async () => {
                getLastEventSource().simulateMessage({
                    task_id: 'test-123',
                    status: 'extracting',
                    created_at: new Date().toISOString(),
                });
            });

            expect(result.current.status).toBeNull();
        });
    });

    describe('options', () => {
        it('should accept custom options', () => {
            const { result } = renderHook(() =>
                usePipelineEvents(null, {
                    maxRetries: 10,
                    baseDelay: 5000,
                    maxDelay: 120000,
                })
            );
            expect(result.current).toBeDefined();
        });

        it('should work with empty options', () => {
            const { result } = renderHook(() => usePipelineEvents(null, {}));
            expect(result.current).toBeDefined();
        });
    });
});
