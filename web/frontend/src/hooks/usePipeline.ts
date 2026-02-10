import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { pipelineApi } from '@/services/pipelineApi';
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

    const { data: status, refetch: refetchStatus } = useQuery<PipelineStatusResponse | null>({
        queryKey: ['pipeline', 'status', activePipeline?.task_id],
        queryFn: async () => {
            if (!activePipeline?.task_id) return null;
            try {
                const response = await pipelineApi.getPipelineStatus(activePipeline.task_id);
                return response.data ?? null;
            } catch {
                return null;
            }
        },
        enabled: !!activePipeline?.task_id,
        refetchInterval: (data) => {
            const statusData = data as unknown as PipelineStatusResponse;
            if (statusData?.status === 'running') {
                return 2000;
            }
            return false;
        },
    });

    const runPipelineMutation = useMutation({
        mutationFn: () => pipelineApi.runMatching(),
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ['pipeline', 'active'] });
            refetchStatus();
        },
    });

    const stopPipelineMutation = useMutation({
        mutationFn: () => pipelineApi.stopMatching(),
        onSuccess: () => {
            refetchStatus();
        },
    });

    const clearTaskMutation = useMutation({
        mutationFn: async () => {
            // Just refetch to clear the status display
            await refetchStatus();
        },
    });

    const uploadResumeMutation = useMutation({
        mutationFn: (file: File) => pipelineApi.uploadResume(file),
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ['resume'] });
        },
    });

    React.useEffect(() => {
        if (status?.status === 'completed' || status?.status === 'failed') {
            queryClient.invalidateQueries({ queryKey: ['matches'] });
            queryClient.invalidateQueries({ queryKey: ['stats'] });
        }
    }, [status?.status, queryClient]);

    return {
        activePipeline,
        status,
        isLoading,
        runPipeline: runPipelineMutation.mutate,
        stopPipeline: stopPipelineMutation.mutate,
        isRunning: status?.status === 'running',
        isStopping: stopPipelineMutation.isPending,
        clearTask: clearTaskMutation.mutate,
        uploadResume: uploadResumeMutation.mutate,
        isUploading: uploadResumeMutation.isPending,
    };
};
