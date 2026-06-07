import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { pipelineApi } from '@/services/pipelineApi';
import { usePipelineEvents } from './usePipelineEvents';
import { getResumeHash, getResume, saveResume } from '@/utils/indexedDB';
import { computeFileHash } from '@/utils/fileUtils';
import React from 'react';
import type { PipelineStatusResponse, ResumeStatusResponse } from '@/types/api';

async function pollResumeProcessing(taskId: string): Promise<ResumeStatusResponse | null> {
    const MAX_POLLS = 60; // 2 min @ 2 s/poll
    for (let i = 0; i < MAX_POLLS; i++) {
        await new Promise(r => setTimeout(r, 2000));
        const resp = await pipelineApi.getResumeStatus(taskId);
        const { status } = resp.data;
        if (status === 'completed' || status === 'failed') return resp.data;
    }
    return null;
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
            phase: 'initializing',
            stats: {},
            warnings: [],
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
        if (resumeProcessingStatus?.matching_task_id) {
            setPendingTaskId(resumeProcessingStatus.matching_task_id);
            queryClient.invalidateQueries({ queryKey: ['pipeline', 'active'] });
        }
        if (
            resumeProcessingStatus?.status === 'completed' ||
            resumeProcessingStatus?.status === 'failed'
        ) {
            setPendingResumeTaskId(null);
        }
    }, [
        queryClient,
        resumeProcessingStatus?.matching_task_id,
        resumeProcessingStatus?.status,
    ]);

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

    const activeStatuses = new Set([
        'pending',
        'running',
        'cancellation_requested',
        'persisting',
    ]);

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
     * Upload a resume file, persist it locally, and track any async processing task.
     */
    const uploadResume = React.useCallback(async (
        file: File
    ): Promise<{ alreadyExists: boolean; message: string }> => {
        setIsUploading(true);
        try {
            const hash = await computeFileHash(file);
            const preflight = (await pipelineApi.preflightResume(hash)).data;
            await saveResume(file, hash, file.name);

            if (preflight.status === 'ready_already_known') {
                const selectResp = await pipelineApi.selectResume(hash, file.name);
                if (selectResp.data.matching_task_id) {
                    setPendingTaskId(selectResp.data.matching_task_id);
                    queryClient.invalidateQueries({ queryKey: ['pipeline', 'active'] });
                }
                return {
                    alreadyExists: true,
                    message: selectResp.data.message || 'Resume already ready',
                };
            }

            if (preflight.status === 'processing_existing') {
                if (preflight.task_id) {
                    setPendingResumeTaskId(preflight.task_id);
                }
                return {
                    alreadyExists: false,
                    message: preflight.message,
                };
            }

            let uploadResp;
            if (preflight.status === 'failed_retryable' && preflight.upload_id) {
                uploadResp = await pipelineApi.retryResume(preflight.upload_id);
            } else {
                uploadResp = await pipelineApi.uploadResume(file, hash);
            }

            if (uploadResp.data.task_id) {
                setPendingResumeTaskId(uploadResp.data.task_id);
            }
            if (uploadResp.data.matching_task_id) {
                setPendingTaskId(uploadResp.data.matching_task_id);
                queryClient.invalidateQueries({ queryKey: ['pipeline', 'active'] });
            }

            return {
                alreadyExists: uploadResp.data.status === 'ready',
                message: uploadResp.data.message || 'Resume uploaded successfully',
            };
        } finally {
            setIsUploading(false);
        }
    }, [queryClient]);

    /**
     * Run the matching pipeline using the backend eligibility decision as the source of truth.
     */
    const runPipeline = React.useCallback(async (onError?: (msg: string) => void) => {
        setIsRunningPreflight(true);
        try {
            if (pendingResumeTaskId) {
                onError?.('Resume is still being processed. Please wait a moment and try again.');
                return;
            }

            let eligibility = (await pipelineApi.getResumeEligibility()).data;

            if (!eligibility.can_run && eligibility.task_id && ['extracting', 'extracted', 'embedding'].includes(eligibility.status)) {
                setPendingResumeTaskId(eligibility.task_id);
                onError?.(eligibility.message);
                return;
            }

            if (!eligibility.can_run && eligibility.status === 'missing') {
                const hash = await getResumeHash();
                if (!hash) {
                    onError?.('No resume found in browser storage. Please upload a resume first.');
                    return;
                }

                const blob = await getResume(hash);
                if (!blob) {
                    onError?.('Resume file not found in browser storage. Please re-upload.');
                    return;
                }

                const uploadResp = await pipelineApi.uploadResume(blob as File, hash);
                if (uploadResp.data.task_id) {
                    setPendingResumeTaskId(uploadResp.data.task_id);
                    const resumeStatus = await pollResumeProcessing(uploadResp.data.task_id);
                    if (resumeStatus?.matching_task_id) {
                        setPendingTaskId(resumeStatus.matching_task_id);
                        queryClient.invalidateQueries({ queryKey: ['pipeline', 'active'] });
                        return;
                    }
                }
                if (uploadResp.data.matching_task_id) {
                    setPendingTaskId(uploadResp.data.matching_task_id);
                    queryClient.invalidateQueries({ queryKey: ['pipeline', 'active'] });
                    return;
                }
                eligibility = (await pipelineApi.getResumeEligibility()).data;
            }

            if (!eligibility.can_run) {
                onError?.(eligibility.message);
                return;
            }

            await runPipelineMutation.mutateAsync();
        } catch (error) {
            const msg = error instanceof Error ? error.message : 'Unknown error';
            onError?.(`Failed to start matching: ${msg}`);
        } finally {
            setIsRunningPreflight(false);
        }
    }, [pendingResumeTaskId, queryClient, runPipelineMutation]);

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
        isRunning: effectiveStatus ? activeStatuses.has(effectiveStatus.status) : false,
        isStopping: stopPipelineMutation.isPending,
        clearTask: handleClearTask,
        uploadResume,
        isUploading,
        isPreparingResume: isRunningPreflight || pendingResumeTaskId !== null,
        resumeProcessingStep: resumeProcessingStatus?.step ?? null,
        resumeProcessingStatus,
        retrySSE,
    };
};
