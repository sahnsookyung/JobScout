import { useCallback, useEffect, useRef, useState } from 'react';
import { pipelineApi } from '@/services/pipelineApi';
import type { PipelineStatusResponse } from '@/types/api';

export type ConnectionState = 'disconnected' | 'connecting' | 'connected' | 'reconnecting' | 'failed';

interface UsePipelineEventsOptions {
    maxRetries?: number;
    baseDelay?: number;
    maxDelay?: number;
    pollInterval?: number;
}

interface PipelineStatusSnapshot {
    taskId: string | null;
    status: PipelineStatusResponse | null;
    connectionState: ConnectionState;
    error: string | null;
    retryCount: number;
}

const DEFAULT_OPTIONS: Required<UsePipelineEventsOptions> = {
    maxRetries: 5,
    baseDelay: 3000,
    maxDelay: 60000,
    pollInterval: 2000,
};

const EMPTY_SNAPSHOT: PipelineStatusSnapshot = {
    taskId: null,
    status: null,
    connectionState: 'disconnected',
    error: null,
    retryCount: 0,
};

function getHttpStatus(error: unknown): number | null {
    if (typeof error !== 'object' || error === null) return null;
    const candidate = error as { status?: unknown; response?: { status?: unknown } };
    const status = candidate.status ?? candidate.response?.status;
    return typeof status === 'number' && Number.isInteger(status) ? status : null;
}

function isTerminalStatus(status: string): boolean {
    return status === 'completed' || status === 'failed' || status === 'cancelled';
}

function isNonRetryableClientError(status: number | null): boolean {
    return status !== null
        && status >= 400
        && status < 500
        && status !== 404
        && status !== 408
        && status !== 429;
}

function requestErrorMessage(status: number | null): string {
    if (status === 401) return 'Your session expired. Sign in again to view run progress.';
    if (status === 403) return 'Your account or selected workspace cannot access this run.';
    if (status === 400) return 'This run is unavailable in the selected workspace. Check the workspace and retry.';
    return 'Progress updates are unavailable for this run. The run may still be processing.';
}

export const usePipelineEvents = (
    taskId: string | null,
    options: UsePipelineEventsOptions = {}
) => {
    const { maxRetries, baseDelay, maxDelay, pollInterval } = { ...DEFAULT_OPTIONS, ...options };
    const [snapshot, setSnapshot] = useState<PipelineStatusSnapshot>(EMPTY_SNAPSHOT);
    const [retryKey, setRetryKey] = useState(0);
    const [manuallyDisconnectedTaskId, setManuallyDisconnectedTaskId] = useState<string | null>(null);
    const taskIdRef = useRef(taskId);
    const snapshotRef = useRef(snapshot);
    const activeRequestRef = useRef<AbortController | null>(null);

    taskIdRef.current = taskId;
    snapshotRef.current = snapshot;

    useEffect(() => {
        if (!taskId) {
            setSnapshot(EMPTY_SNAPSHOT);
            return;
        }

        let active = true;
        let timer: ReturnType<typeof setTimeout> | null = null;
        let controller: AbortController | null = null;
        let failures = 0;

        const updateSnapshot = (update: Partial<PipelineStatusSnapshot>) => {
            if (!active) return;
            setSnapshot((previous) => {
                const current = previous.taskId === taskId
                    ? previous
                    : { ...EMPTY_SNAPSHOT, taskId };
                return { ...current, ...update, taskId };
            });
        };

        if (manuallyDisconnectedTaskId === taskId) {
            updateSnapshot({ connectionState: 'disconnected', error: null });
            return () => {
                active = false;
            };
        }

        setSnapshot((previous) => ({
            taskId,
            status: previous.taskId === taskId ? previous.status : null,
            connectionState: 'connecting',
            error: null,
            retryCount: 0,
        }));

        const poll = async (): Promise<void> => {
            if (!active) return;
            const hasStatus = snapshotRef.current.taskId === taskId
                && snapshotRef.current.status !== null;
            if (!hasStatus) updateSnapshot({ connectionState: 'connecting' });

            controller = new AbortController();
            activeRequestRef.current = controller;
            try {
                const response = await pipelineApi.getPipelineStatus(taskId, controller.signal);
                if (!active) return;

                const nextStatus = response.data;
                if (nextStatus.task_id !== taskId) {
                    throw new Error('Progress response did not match the requested task.');
                }
                failures = 0;
                updateSnapshot({
                    status: nextStatus,
                    connectionState: isTerminalStatus(nextStatus.status) ? 'disconnected' : 'connected',
                    error: null,
                    retryCount: 0,
                });
                if (!isTerminalStatus(nextStatus.status)) {
                    timer = setTimeout(() => void poll(), pollInterval);
                }
            } catch (error) {
                if (!active || controller.signal.aborted) return;

                const httpStatus = getHttpStatus(error);
                if (isNonRetryableClientError(httpStatus)) {
                    updateSnapshot({
                        connectionState: 'failed',
                        error: requestErrorMessage(httpStatus),
                        retryCount: failures,
                    });
                    return;
                }

                failures += 1;
                if (failures > maxRetries) {
                    updateSnapshot({
                        connectionState: 'failed',
                        error: `Progress updates could not be restored after ${maxRetries} retries. The run may still be processing.`,
                        retryCount: maxRetries,
                    });
                    return;
                }

                updateSnapshot({
                    connectionState: 'reconnecting',
                    error: `Progress connection lost. Retrying (${failures}/${maxRetries})…`,
                    retryCount: failures,
                });
                const delay = Math.min(baseDelay * (2 ** (failures - 1)), maxDelay);
                timer = setTimeout(() => void poll(), delay);
            } finally {
                if (activeRequestRef.current === controller) {
                    activeRequestRef.current = null;
                }
            }
        };

        void poll();

        return () => {
            active = false;
            if (timer !== null) clearTimeout(timer);
            controller?.abort();
            if (activeRequestRef.current === controller) {
                activeRequestRef.current = null;
            }
        };
    }, [baseDelay, manuallyDisconnectedTaskId, maxDelay, maxRetries, pollInterval, retryKey, taskId]);

    const retry = useCallback(() => {
        const currentTaskId = taskIdRef.current;
        if (!currentTaskId) return;
        setManuallyDisconnectedTaskId(null);
        setSnapshot((previous) => previous.taskId === currentTaskId
            ? { ...previous, connectionState: 'connecting', error: null, retryCount: 0 }
            : previous);
        setRetryKey((previous) => previous + 1);
    }, []);

    const disconnect = useCallback(() => {
        const currentTaskId = taskIdRef.current;
        if (!currentTaskId) return;
        setManuallyDisconnectedTaskId(currentTaskId);
        activeRequestRef.current?.abort();
        setSnapshot((previous) => previous.taskId === currentTaskId
            ? { ...previous, connectionState: 'disconnected' }
            : previous);
    }, []);

    const hasCurrentSnapshot = snapshot.taskId === taskId;
    return {
        status: hasCurrentSnapshot ? snapshot.status : null,
        connectionState: !taskId
            ? 'disconnected'
            : hasCurrentSnapshot
                ? snapshot.connectionState
                : 'connecting',
        error: hasCurrentSnapshot ? snapshot.error : null,
        retryCount: hasCurrentSnapshot ? snapshot.retryCount : 0,
        retry,
        disconnect,
    };
};
