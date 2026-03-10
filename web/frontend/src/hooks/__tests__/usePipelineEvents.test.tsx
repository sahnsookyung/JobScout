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

    // Helper functions to reduce duplication
    const renderConnectedHook = (taskId: string, options?: Parameters<typeof usePipelineEvents>[1]) => {
        const { result, rerender, unmount } = renderHook(
            ({ taskId }) => usePipelineEvents(taskId, options),
            { initialProps: { taskId: null } }
        );
        rerender({ taskId });
        return { result, rerender, unmount };
    };

    const waitForConnection = async () => {
        await waitFor(() => {
            expect(MockEventSource.instances.length).toBe(1);
        });
    };

    const createStatusData = (status: string): PipelineStatusResponse => ({
        task_id: 'test-123',
        status,
        created_at: new Date().toISOString(),
    });

    const simulateErrorAndRetry = async (delayMs: number) => {
        await act(async () => {
            getLastEventSource().simulateError();
        });
        await act(async () => {
            await vi.advanceTimersByTimeAsync(delayMs);
        });
    };

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
            const { rerender } = renderConnectedHook('test-123');
            await waitForConnection();

            expect(getLastEventSource().url).toContain('test-123');
        });

        it('should close EventSource on unmount', async () => {
            const { unmount } = renderConnectedHook('test-123');
            await waitForConnection();

            const closeSpy = vi.spyOn(getLastEventSource(), 'close');
            unmount();
            expect(closeSpy).toHaveBeenCalled();
        });
    });

    describe('status updates', () => {
        it('should update status on message', async () => {
            const { result } = renderConnectedHook('test-123');
            await waitForConnection();

            const statusData = createStatusData('extracting');

            await act(async () => {
                getLastEventSource().simulateMessage(statusData);
            });

            expect(result.current.status).toEqual(statusData);
        });

        it('should ignore heartbeat messages', async () => {
            const { result } = renderConnectedHook('test-123');
            await waitForConnection();

            await act(async () => {
                getLastEventSource().simulateMessage({ type: 'heartbeat' });
            });

            expect(result.current.status).toBeNull();
        });

        it.each(['completed', 'failed'])(
            'should disconnect on %s status',
            async (status) => {
                const { result } = renderConnectedHook('test-123');
                await waitForConnection();

                await act(async () => {
                    getLastEventSource().simulateOpen();
                    getLastEventSource().simulateMessage(createStatusData(status));
                });

                expect(result.current.connectionState).toBe('disconnected');
            }
        );
    });

    describe('error handling', () => {
        it('should handle onerror without crashing', async () => {
            renderConnectedHook('test-123', { maxRetries: 5, baseDelay: 100 });
            await waitForConnection();

            expect(() => {
                getLastEventSource().simulateError();
            }).not.toThrow();
        });

        it('should retry on connection error', async () => {
            renderConnectedHook('test-123', { maxRetries: 5, baseDelay: 1000 });
            await waitForConnection();

            await simulateErrorAndRetry(1000);

            expect(MockEventSource.instances.length).toBeGreaterThanOrEqual(2);
        });

        it('should use exponential backoff for retries', async () => {
            renderConnectedHook('test-123', { maxRetries: 5, baseDelay: 1000 });
            await waitForConnection();

            const initialCount = MockEventSource.instances.length;

            await simulateErrorAndRetry(1000);
            expect(MockEventSource.instances.length).toBe(initialCount + 1);

            if (MockEventSource.instances.length > initialCount) {
                await simulateErrorAndRetry(2000);
                expect(MockEventSource.instances.length).toBeGreaterThanOrEqual(initialCount + 2);
            }
        });

        it('should fail after max retries', async () => {
            const { result } = renderConnectedHook('test-123', { maxRetries: 3, baseDelay: 10 });
            await waitForConnection();

            for (let i = 0; i < 4; i++) {
                const currentEs = getLastEventSource();
                if (currentEs) {
                    await act(async () => {
                        currentEs.simulateError();
                    });

                    if (i < 3) {
                        await act(async () => {
                            await vi.advanceTimersByTimeAsync(10 * Math.pow(2, i));
                        });

                        await waitFor(() => {
                            expect(MockEventSource.instances.length).toBeGreaterThan(i + 1);
                        }, { timeout: 1000 });
                    }
                }
            }

            await waitFor(() => {
                expect(result.current.connectionState).toBe('failed');
            }, { timeout: 1000 });

            expect(result.current.error).toContain('after 3 attempts');
        });

        it('should reset retry count on successful reconnection', async () => {
            const { result } = renderConnectedHook('test-123', { maxRetries: 5, baseDelay: 100 });
            await waitForConnection();

            await simulateErrorAndRetry(100);
            expect(result.current.retryCount).toBeGreaterThanOrEqual(1);

            await act(async () => {
                getLastEventSource().simulateOpen();
            });

            expect(result.current.retryCount).toBe(0);
        });
    });

    describe('manual retry', () => {
        it('should allow manual retry via retry function', async () => {
            const { result } = renderConnectedHook('test-123', { maxRetries: 5, baseDelay: 100 });
            await waitForConnection();

            await act(async () => {
                getLastEventSource().simulateError();
            });

            expect(result.current.error).toBeTruthy();

            await act(async () => {
                result.current.retry();
            });

            expect(result.current.error).toBeNull();
        });
    });

    describe('cleanup', () => {
        it('should disconnect when taskId becomes null', async () => {
            const { result, rerender } = renderHook(
                ({ taskId }) => usePipelineEvents(taskId),
                { initialProps: { taskId: 'test-123' } }
            );

            await waitForConnection();

            rerender({ taskId: null });

            expect(result.current.connectionState).toBe('disconnected');
            expect(result.current.status).toBeNull();
        });

        it('should not update state after unmount', async () => {
            const { result, unmount } = renderHook(
                ({ taskId }) => usePipelineEvents(taskId),
                { initialProps: { taskId: 'test-123' } }
            );

            await waitForConnection();

            await act(async () => {
                getLastEventSource().simulateOpen();
            });

            expect(result.current.connectionState).toBe('connected');

            unmount();

            await act(async () => {
                getLastEventSource().simulateMessage(createStatusData('extracting'));
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
