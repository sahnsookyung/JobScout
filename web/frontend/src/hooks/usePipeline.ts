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
    });

    const stopPipelineMutation = useMutation({
        mutationFn: () => pipelineApi.stopMatching(),
    });

    const clearTaskMutation = useMutation({
        mutationFn: async () => {},
    });

    const uploadResumeMutation = useMutation({
        mutationFn: ({ file, hash }: { file: File; hash?: string }) => pipelineApi.uploadResume(file, hash),
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ['resume'] });
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
        stopPipeline: stopPipelineMutation.mutate,
        isRunning: sseStatus?.status === 'running' || sseStatus?.status === 'pending',
        isStopping: stopPipelineMutation.isPending,
        clearTask: clearTaskMutation.mutate,
        uploadResume: uploadResumeMutation.mutate,
        isUploading: uploadResumeMutation.isPending,
        retrySSE: retrySSE,
    };
};
