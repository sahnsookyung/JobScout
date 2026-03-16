import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { pipelineApi } from '@/services/pipelineApi';
import { usePipelineEvents } from './usePipelineEvents';
import React from 'react';
import type { PipelineStatusResponse } from '@/types/api';

export const usePipeline = () => {
    const queryClient = useQueryClient();
    const [pendingTaskId, setPendingTaskId] = React.useState<string | null>(null);

    const { data: activePipeline, isLoading } = useQuery<PipelineStatusResponse | null>({
        queryKey: ['pipeline', 'active'],
        queryFn: async () => {
            try {
                const response = await pipelineApi.getActivePipeline();
                return response.data ?? null;
            } catch {
                return null;
            }
        },
    });

    // Use pendingTaskId from mutation first, then fall back to activePipeline
    const taskIdForSSE = pendingTaskId ?? activePipeline?.task_id ?? null;
    const {
        status: sseStatus,
        connectionState,
        error: sseError,
        retry: retrySSE
    } = usePipelineEvents(taskIdForSSE);

    const runPipelineMutation = useMutation({
        mutationFn: () => pipelineApi.runMatching(),
        onSuccess: (response) => {
            // Immediately set the task_id for SSE connection - don't wait for query invalidation
            if (response.data?.task_id) {
                setPendingTaskId(response.data.task_id);
            }
            queryClient.invalidateQueries({ queryKey: ['pipeline', 'active'] });
        },
        onError: (error: Error) => {
            console.error('[Pipeline] Failed to start matching:', error.message);
        },
    });

    const stopPipelineMutation = useMutation({
        mutationFn: () => pipelineApi.stopMatching(),
        onError: (error: Error) => {
            console.error('[Pipeline] Failed to stop pipeline:', error.message);
        },
    });

    const clearTaskMutation = useMutation({
        mutationFn: async () => { },
    });

    const uploadResumeMutation = useMutation({
        mutationFn: ({ file, hash }: { file: File; hash?: string }) => pipelineApi.uploadResume(file, hash),
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ['resume'] });
        },
        onError: (error: Error) => {
            console.error('[Pipeline] Resume upload failed:', error.message);
        },
    });

    React.useEffect(() => {
        if (sseStatus?.status === 'completed' || sseStatus?.status === 'failed') {
            queryClient.invalidateQueries({ queryKey: ['matches'] });
            queryClient.invalidateQueries({ queryKey: ['stats'] });
            // Clear pending task ID when pipeline completes or fails
            setPendingTaskId(null);
        }
    }, [sseStatus?.status, queryClient]);

    // Clear pending task ID when clearTask is called
    const handleClearTask = React.useCallback(() => {
        setPendingTaskId(null);
        clearTaskMutation.mutate();
    }, [clearTaskMutation]);

    return {
        activePipeline,
        status: sseStatus,
        connectionState,
        sseError,
        isLoading,
        runPipeline: runPipelineMutation.mutate,
        runPipelineError: runPipelineMutation.error,
        stopPipeline: stopPipelineMutation.mutate,
        stopPipelineError: stopPipelineMutation.error,
        isRunning: sseStatus?.status === 'running' || sseStatus?.status === 'pending',
        isStopping: stopPipelineMutation.isPending,
        clearTask: handleClearTask,
        uploadResume: uploadResumeMutation.mutate,
        uploadResumeError: uploadResumeMutation.error,
        isUploading: uploadResumeMutation.isPending,
        retrySSE: retrySSE,
    };
};
