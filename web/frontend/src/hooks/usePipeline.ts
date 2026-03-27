import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { pipelineApi } from '@/services/pipelineApi';
import { usePipelineEvents } from './usePipelineEvents';
import { getResumeHash, getResume, saveResume } from '@/utils/indexedDB';
import { computeFileHash } from '@/utils/fileUtils';
import React from 'react';
import type { PipelineStatusResponse } from '@/types/api';

async function pollResumeProcessing(taskId: string): Promise<void> {
    const MAX_POLLS = 60; // 2 min @ 2 s/poll
    for (let i = 0; i < MAX_POLLS; i++) {
        await new Promise(r => setTimeout(r, 2000));
        const resp = await pipelineApi.getResumeStatus(taskId);
        const { status } = resp.data;
        if (status === 'completed' || status === 'failed') return;
    }
}

export const usePipeline = () => {
    const queryClient = useQueryClient();
    const [pendingTaskId, setPendingTaskId] = React.useState<string | null>(null);
    const [pendingResumeTaskId, setPendingResumeTaskId] = React.useState<string | null>(null);
    const [isUploading, setIsUploading] = React.useState(false);
    const [isRunningPreflight, setIsRunningPreflight] = React.useState(false);

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

    const optimisticPendingStatus = React.useMemo<PipelineStatusResponse | null>(() => {
        if (pendingTaskId === null || sseStatus !== null || activePipeline !== null) {
            return null;
        }
        return {
            task_id: pendingTaskId,
            status: 'pending',
            step: 'initializing',
        };
    }, [activePipeline, pendingTaskId, sseStatus]);

    const effectiveStatus = sseStatus ?? activePipeline ?? optimisticPendingStatus;

    // Poll resume processing status after explicit upload with a background task
    const { data: resumeProcessingStatus } = useQuery({
        queryKey: ['resume', 'processing', pendingResumeTaskId],
        queryFn: async () => {
            const resp = await pipelineApi.getResumeStatus(pendingResumeTaskId!);
            return resp.data;
        },
        enabled: pendingResumeTaskId !== null,
        refetchInterval: (query) => {
            const s = query.state.data?.status;
            return s === 'completed' || s === 'failed' ? false : 2000;
        },
    });

    React.useEffect(() => {
        if (
            resumeProcessingStatus?.status === 'completed' ||
            resumeProcessingStatus?.status === 'failed'
        ) {
            setPendingResumeTaskId(null);
        }
    }, [resumeProcessingStatus?.status]);

    const runPipelineMutation = useMutation({
        mutationFn: () => pipelineApi.runMatching(),
        onSuccess: (response) => {
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

    React.useEffect(() => {
        if (
            sseStatus?.status === 'completed' ||
            sseStatus?.status === 'failed' ||
            sseStatus?.status === 'cancelled'
        ) {
            queryClient.invalidateQueries({ queryKey: ['matches'] });
            queryClient.invalidateQueries({ queryKey: ['stats'] });
            queryClient.invalidateQueries({ queryKey: ['pipeline', 'active'] });
            setPendingTaskId(null);
        }
    }, [sseStatus?.status, queryClient]);

    const handleClearTask = React.useCallback(() => {
        setPendingTaskId(null);
        clearTaskMutation.mutate();
    }, [clearTaskMutation]);

    /**
     * Upload a resume file: compute hash → dedup check → upload if new → save to IndexedDB.
     * Returns metadata the caller can use to show appropriate toasts.
     * If the backend starts background processing, tracks the task so the Run button
     * stays disabled until processing completes.
     */
    const uploadResume = React.useCallback(async (
        file: File
    ): Promise<{ alreadyExists: boolean; message: string }> => {
        setIsUploading(true);
        try {
            const hash = await computeFileHash(file);
            const checkResp = await pipelineApi.checkResumeHash(hash);

            if (checkResp.data.exists) {
                // File already on backend — just sync to IndexedDB
                await saveResume(file, hash, file.name);
                return { alreadyExists: true, message: 'An identical resume has already been uploaded.' };
            }

            const uploadResp = await pipelineApi.uploadResume(file, hash);
            await saveResume(file, hash, file.name);

            if (uploadResp.data.task_id) {
                setPendingResumeTaskId(uploadResp.data.task_id);
            }

            return {
                alreadyExists: false,
                message: uploadResp.data.message || 'Resume uploaded successfully',
            };
        } finally {
            setIsUploading(false);
        }
    }, []);

    /**
     * Run the matching pipeline with a pre-flight guard:
     * 1. Verify resume exists in IndexedDB.
     * 2. If resume not on backend, upload it (and wait for processing).
     * 3. If a previous upload is still being processed, abort with an error.
     * 4. Start matching.
     *
     * @param onError  Called with a human-readable message if preflight fails.
     */
    const runPipeline = React.useCallback(async (onError?: (msg: string) => void) => {
        setIsRunningPreflight(true);
        try {
            const hash = await getResumeHash();
            if (!hash) {
                onError?.('No resume found in browser storage. Please upload a resume first.');
                return;
            }

            // Block if a previous explicit upload is still being processed
            if (pendingResumeTaskId) {
                onError?.('Resume is still being processed. Please wait a moment and try again.');
                return;
            }

            // Auto-upload if the resume isn't on the backend (e.g. storage was cleared
            // and the user re-added the file, or a first-run edge case).
            const checkResp = await pipelineApi.checkResumeHash(hash);
            if (!checkResp.data.exists) {
                const blob = await getResume(hash);
                if (!blob) {
                    onError?.('Resume file not found in browser storage. Please re-upload.');
                    return;
                }
                const uploadResp = await pipelineApi.uploadResume(blob as File, hash);
                if (uploadResp.data.task_id) {
                    await pollResumeProcessing(uploadResp.data.task_id);
                }
            }

            await runPipelineMutation.mutateAsync();
        } catch (error) {
            const msg = error instanceof Error ? error.message : 'Unknown error';
            onError?.(`Failed to start matching: ${msg}`);
        } finally {
            setIsRunningPreflight(false);
        }
    }, [pendingResumeTaskId, runPipelineMutation]);

    return {
        activePipeline,
        status: effectiveStatus,
        connectionState,
        sseError,
        isLoading,
        runPipeline,
        runPipelineError: runPipelineMutation.error,
        stopPipeline: stopPipelineMutation.mutate,
        stopPipelineError: stopPipelineMutation.error,
        isRunning: effectiveStatus?.status === 'running' || effectiveStatus?.status === 'pending',
        isStopping: stopPipelineMutation.isPending,
        clearTask: handleClearTask,
        uploadResume,
        isUploading,
        isPreparingResume: isRunningPreflight || pendingResumeTaskId !== null,
        resumeProcessingStep: resumeProcessingStatus?.step ?? null,
        retrySSE,
    };
};
