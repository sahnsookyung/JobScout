import { useState, useEffect } from 'react';
import { useMutation, useQuery } from '@tanstack/react-query';
import { pipelineApi } from '@/services/pipelineApi';

export const usePipeline = () => {
    const [taskId, setTaskId] = useState<string | null>(null);

    // efficient check for active pipeline on mount
    useEffect(() => {
        const checkActive = async () => {
            try {
                const response = await pipelineApi.getActiveTask();
                if (response.data) {
                    setTaskId(response.data.task_id);
                }
            } catch (error) {
                console.error("Failed to check active pipeline:", error);
            }
        };
        checkActive();
    }, []);

    const runMutation = useMutation({
        mutationFn: () => pipelineApi.runMatching(),
        onSuccess: (response) => {
            setTaskId(response.data.task_id);
        },
    });

    const stopMutation = useMutation({
        mutationFn: () => pipelineApi.stopMatching(),
    });

    const statusQuery = useQuery({
        queryKey: ['pipeline-status', taskId],
        queryFn: async () => {
            if (!taskId) throw new Error('No task ID');
            const response = await pipelineApi.getStatus(taskId);
            console.log('[usePipeline] Status update:', response.data);
            return response.data;
        },
        enabled: !!taskId,
        refetchInterval: (data) => {
            // Poll every 2 seconds while running, stop when completed/failed
            if (data?.status === 'running' || data?.status === 'pending') {
                return 2000;
            }
            return false;
        },
    });

    return {
        runPipeline: runMutation.mutate,
        stopPipeline: stopMutation.mutate,
        isRunning: runMutation.isPending,
        isStopping: stopMutation.isPending,
        status: statusQuery.data,
        clearTask: () => setTaskId(null),
    };
};
