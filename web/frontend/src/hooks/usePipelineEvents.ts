import { useState, useEffect, useCallback, useRef } from 'react';
import type { PipelineStatusResponse } from '@/types/api';

export type ConnectionState = 'disconnected' | 'connecting' | 'connected' | 'reconnecting' | 'failed';

interface UsePipelineEventsOptions {
    maxRetries?: number;
    baseDelay?: number;
    maxDelay?: number;
}

const DEFAULT_OPTIONS: Required<UsePipelineEventsOptions> = {
    maxRetries: 5,
    baseDelay: 3000,
    maxDelay: 60000,
};

export const usePipelineEvents = (
    taskId: string | null,
    options: UsePipelineEventsOptions = {}
) => {
    const { maxRetries, baseDelay, maxDelay } = { ...DEFAULT_OPTIONS, ...options };

    const [status, setStatus] = useState<PipelineStatusResponse | null>(null);
    const [connectionState, setConnectionState] = useState<ConnectionState>('disconnected');
    const [error, setError] = useState<string | null>(null);
    const [retryCount, setRetryCount] = useState(0);

    const eventSourceRef = useRef<EventSource | null>(null);
    const retryTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
    const mountedRef = useRef(true);
    const retryCountRef = useRef(0);
    const taskIdRef = useRef<string | null>(null);

    taskIdRef.current = taskId;
    retryCountRef.current = retryCount;

    const clearRetryTimeout = useCallback(() => {
        if (retryTimeoutRef.current) {
            clearTimeout(retryTimeoutRef.current);
            retryTimeoutRef.current = null;
        }
    }, []);

    const closeCurrentEventSource = useCallback(() => {
        if (eventSourceRef.current) {
            eventSourceRef.current.close();
            eventSourceRef.current = null;
        }
    }, []);

    const disconnect = useCallback(() => {
        clearRetryTimeout();
        closeCurrentEventSource();
        if (mountedRef.current) {
            setConnectionState('disconnected');
        }
    }, [clearRetryTimeout, closeCurrentEventSource]);

    const calculateDelay = useCallback((attempt: number): number => {
        const delay = baseDelay * Math.pow(2, attempt);
        return Math.min(delay, maxDelay);
    }, [baseDelay, maxDelay]);

    const handleOpen = useCallback((eventSource: EventSource) => {
        if (!mountedRef.current) {
            eventSource.close();
            return;
        }

        setConnectionState('connected');
        setError(null);
        setRetryCount(0);
    }, []);

    const handleMessage = useCallback((eventSource: EventSource, event: MessageEvent<string>) => {
        if (!mountedRef.current) {
            return;
        }

        try {
            const data = JSON.parse(event.data) as PipelineStatusResponse & { type?: string };
            if (data.type === 'heartbeat') {
                return;
            }

            setStatus(data);

            if (data.status === 'completed' || data.status === 'failed' || data.status === 'cancelled') {
                clearRetryTimeout();
                eventSource.close();
                eventSourceRef.current = null;
                setConnectionState('disconnected');
            }
        } catch (parseError) {
            console.error('Failed to parse SSE data:', parseError);
        }
    }, [clearRetryTimeout]);

    const scheduleReconnect = useCallback((currentRetryCount: number) => {
        setConnectionState('reconnecting');
        setError(`Connection lost. Reconnecting... (${currentRetryCount + 1}/${maxRetries})`);
        retryTimeoutRef.current = setTimeout(() => {
            if (mountedRef.current) {
                setRetryCount((prev) => prev + 1);
            }
        }, calculateDelay(currentRetryCount));
    }, [calculateDelay, maxRetries]);

    const handleError = useCallback((eventSource: EventSource) => {
        if (!mountedRef.current) {
            return;
        }

        eventSource.close();
        eventSourceRef.current = null;

        const currentRetryCount = retryCountRef.current;
        if (currentRetryCount < maxRetries) {
            scheduleReconnect(currentRetryCount);
            return;
        }

        setConnectionState('failed');
        setError(`Connection failed after ${maxRetries} attempts. Please refresh the page to retry.`);
    }, [maxRetries, scheduleReconnect]);

    const connect = useCallback(() => {
        const currentTaskId = taskIdRef.current;
        if (!currentTaskId) {
            return;
        }

        closeCurrentEventSource();

        const isReconnecting = retryCountRef.current > 0;
        setConnectionState(isReconnecting ? 'reconnecting' : 'connecting');

        const eventSource = new EventSource(`/api/pipeline/events/${currentTaskId}`);
        eventSourceRef.current = eventSource;
        eventSource.onopen = () => handleOpen(eventSource);
        eventSource.onmessage = (event) => handleMessage(eventSource, event);
        eventSource.onerror = () => handleError(eventSource);
    }, [closeCurrentEventSource, handleError, handleMessage, handleOpen]);

    useEffect(() => {
        mountedRef.current = true;

        if (taskId) {
            connect();
        } else {
            disconnect();
            setStatus(null);
        }

        return () => {
            mountedRef.current = false;
            disconnect();
        };
    }, [taskId, connect, disconnect]);

    useEffect(() => {
        if (retryCount > 0 && retryCount <= maxRetries && mountedRef.current) {
            connect();
        }
    }, [connect, retryCount, maxRetries]);

    const retry = useCallback(() => {
        setRetryCount(0);
        setError(null);
        disconnect();

        if (taskIdRef.current) {
            connect();
        }
    }, [connect, disconnect]);

    return {
        status,
        connectionState,
        error,
        retryCount,
        retry,
        disconnect,
    };
};
