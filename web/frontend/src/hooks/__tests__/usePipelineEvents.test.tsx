/**
 * Tests for usePipelineEvents hook
 * Covers: usePipelineEvents.ts - core functionality
 */

import { renderHook, waitFor, act } from '@testing-library/react';
import { usePipelineEvents } from '../usePipelineEvents';
import type { PipelineStatusResponse } from '@/types/api';

// Mock EventSource
class MockEventSource {
    static instances: MockEventSource[] = [];
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

    close(): void {}
}

vi.stubGlobal('EventSource', MockEventSource);

describe('usePipelineEvents', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        MockEventSource.instances = [];
    });

    afterEach(() => {
        MockEventSource.instances = [];
    });

    const getLastEventSource = () => MockEventSource.instances[MockEventSource.instances.length - 1];

    describe('initial state', () => {
        it('should start disconnected when no taskId', () => {
            const { result } = renderHook(() => usePipelineEvents(null));
            expect(result.current.connectionState).toBe('disconnected');
            expect(result.current.status).toBeNull();
            expect(result.current.error).toBeNull();
            expect(result.current.retryCount).toBe(0);
        });

        it('should have correct initial values', () => {
            const { result } = renderHook(() => usePipelineEvents(null));
            expect(typeof result.current.retry).toBe('function');
            expect(typeof result.current.disconnect).toBe('function');
        });
    });

    describe('connection flow', () => {
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

        it('should close EventSource when taskId becomes null', async () => {
            const { rerender } = renderHook(
                ({ taskId }) => usePipelineEvents(taskId),
                { initialProps: { taskId: 'test-123' } }
            );

            await waitFor(() => {
                expect(MockEventSource.instances.length).toBe(1);
            });

            const closeSpy = vi.spyOn(getLastEventSource(), 'close');
            rerender({ taskId: null });

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
                getLastEventSource().onmessage?.({
                    data: JSON.stringify(statusData),
                } as MessageEvent);
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
                getLastEventSource().onmessage?.({
                    data: JSON.stringify({ type: 'heartbeat' }),
                } as MessageEvent);
            });

            expect(result.current.status).toBeNull();
        });

        it('should handle parse errors gracefully', async () => {
            const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
            const { result, rerender } = renderHook(
                ({ taskId }) => usePipelineEvents(taskId),
                { initialProps: { taskId: null } }
            );

            rerender({ taskId: 'test-123' });

            await waitFor(() => {
                expect(MockEventSource.instances.length).toBe(1);
            });

            await act(async () => {
                getLastEventSource().onmessage?.({
                    data: 'invalid',
                } as MessageEvent);
            });

            expect(consoleSpy).toHaveBeenCalled();
            consoleSpy.mockRestore();
        });
    });

    describe('error handling', () => {
        it('should call onerror handler', async () => {
            const { rerender } = renderHook(
                ({ taskId }) => usePipelineEvents(taskId, { maxRetries: 5, baseDelay: 100 }),
                { initialProps: { taskId: null } }
            );

            rerender({ taskId: 'test-123' });

            await waitFor(() => {
                expect(MockEventSource.instances.length).toBe(1);
            });

            // Should not throw
            expect(() => {
                getLastEventSource().onerror?.();
            }).not.toThrow();
        });
    });

    describe('disconnect function', () => {
        it('should be callable', async () => {
            const { result, rerender } = renderHook(
                ({ taskId }) => usePipelineEvents(taskId),
                { initialProps: { taskId: null } }
            );

            rerender({ taskId: 'test-123' });

            await waitFor(() => {
                expect(MockEventSource.instances.length).toBe(1);
            });

            expect(() => {
                result.current.disconnect();
            }).not.toThrow();
        });
    });

    describe('retry function', () => {
        it('should be callable', async () => {
            const { result, rerender } = renderHook(
                ({ taskId }) => usePipelineEvents(taskId),
                { initialProps: { taskId: null } }
            );

            rerender({ taskId: 'test-123' });

            await waitFor(() => {
                expect(MockEventSource.instances.length).toBe(1);
            });

            expect(() => {
                result.current.retry();
            }).not.toThrow();
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

            // Should not throw with custom options
            expect(result.current).toBeDefined();
        });

        it('should work with empty options object', () => {
            const { result } = renderHook(() => usePipelineEvents(null, {}));
            expect(result.current).toBeDefined();
        });
    });
});
