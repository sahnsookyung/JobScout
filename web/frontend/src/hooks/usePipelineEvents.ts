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

    const disconnect = useCallback(() => {
        clearRetryTimeout();
        if (eventSourceRef.current) {
            eventSourceRef.current.close();
            eventSourceRef.current = null;
        }
        if (mountedRef.current) {
            setConnectionState('disconnected');
        }
    }, [clearRetryTimeout]);

    const calculateDelay = useCallback((attempt: number): number => {
        const delay = baseDelay * Math.pow(2, attempt);
        return Math.min(delay, maxDelay);
    }, [baseDelay, maxDelay]);

    const connect = useCallback(() => {
        const currentTaskId = taskIdRef.current;
        if (!currentTaskId) return;
        
        if (eventSourceRef.current) {
            eventSourceRef.current.close();
        }

        const isReconnecting = retryCountRef.current > 0;
        setConnectionState(isReconnecting ? 'reconnecting' : 'connecting');
        
        const eventSource = new EventSource(`/api/pipeline/events/${currentTaskId}`);
        eventSourceRef.current = eventSource;

        eventSource.onopen = () => {
            if (!mountedRef.current) {
                eventSource.close();
                return;
            }
            setConnectionState('connected');
            setError(null);
            setRetryCount(0);
        };

        eventSource.onmessage = (event) => {
            if (!mountedRef.current) return;
            
            try {
                const data = JSON.parse(event.data) as PipelineStatusResponse & { type?: string };
                if (data.type === 'heartbeat') return;
                setStatus(data);
            } catch (e) {
                console.error('Failed to parse SSE data:', e);
            }
        };

        eventSource.onerror = () => {
            if (!mountedRef.current) return;
            
            eventSource.close();
            eventSourceRef.current = null;
            
            const currentRetryCount = retryCountRef.current;
            
            if (currentRetryCount < maxRetries) {
                setConnectionState('reconnecting');
                setError(`Connection lost. Reconnecting... (${currentRetryCount + 1}/${maxRetries})`);
                
                const delay = calculateDelay(currentRetryCount);
                
                retryTimeoutRef.current = setTimeout(() => {
                    if (mountedRef.current) {
                        setRetryCount(prev => prev + 1);
                    }
                }, delay);
            } else {
                setConnectionState('failed');
                setError(`Connection failed after ${maxRetries} attempts. Please refresh the page to retry.`);
            }
        };
    }, [maxRetries, calculateDelay]);

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
    }, [taskId]);

    useEffect(() => {
        if (retryCount > 0 && retryCount <= maxRetries && mountedRef.current) {
            connect();
        }
    }, [retryCount, maxRetries]);

    const retry = useCallback(() => {
        setRetryCount(0);
        setError(null);
        disconnect();
        setTimeout(() => {
            if (mountedRef.current) {
                setRetryCount(1);
            }
        }, 0);
    }, [disconnect]);

    return {
        status,
        connectionState,
        error,
        retryCount,
        retry,
        disconnect,
    };
};
