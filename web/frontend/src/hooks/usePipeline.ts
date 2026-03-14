import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { pipelineApi } from '@/services/pipelineApi';
import { usePipelineEvents } from './usePipelineEvents';
import React from 'react';
import type { PipelineStatusResponse } from '@/types/api';

export const usePipeline = () => {
    const queryClient = useQueryClient();

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

    const {
        status: sseStatus,
        connectionState,
        error: sseError,
        retry: retrySSE
    } = usePipelineEvents(activePipeline?.task_id ?? null);

    const runPipelineMutation = useMutation({
        mutationFn: () => pipelineApi.runMatching(),
        onSuccess: () => {
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
        }
    }, [sseStatus?.status, queryClient]);

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
        clearTask: clearTaskMutation.mutate,
        uploadResume: uploadResumeMutation.mutate,
        uploadResumeError: uploadResumeMutation.error,
        isUploading: uploadResumeMutation.isPending,
        retrySSE: retrySSE,
    };
};
