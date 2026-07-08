import React, { useEffect, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import {
    Activity,
    AlertTriangle,
    ChevronLeft,
    ChevronRight,
    Clock3,
    ExternalLink,
    ListChecks,
    RefreshCw,
    RotateCcw,
    Search,
    XCircle,
} from 'lucide-react';

import { useJobs } from '@/hooks/useJobs';
import { jobsApi } from '@/services/jobsApi';
import { pipelineApi } from '@/services/pipelineApi';
import { pipelineRunsApi } from '@/services/pipelineRunsApi';
import type {
    JobInventoryItem,
    JobLifecycleStatus,
    JobProcessingStatus,
    LlmProviderCanaryResponse,
    LlmProviderStatusResponse,
    LlmEvaluationQueueStatusResponse,
    PipelineRunSummary,
    PipelineStatusResponse,
    ProcessingBlockerItem,
} from '@/types/api';

const PAGE_SIZE = 50;

const PROCESSING_FILTERS: Array<{ key: JobProcessingStatus; label: string }> = [
    { key: 'all', label: 'All' },
    { key: 'ready', label: 'Ready' },
    { key: 'pending_extraction', label: 'Ready extract' },
    { key: 'missing_description', label: 'Missing desc' },
    { key: 'pending_embedding', label: 'Ready embed' },
    { key: 'failed', label: 'Retry or failed' },
];

const LIFECYCLE_FILTERS: Array<{ key: JobLifecycleStatus; label: string }> = [
    { key: 'all', label: 'All jobs' },
    { key: 'active', label: 'Active' },
    { key: 'inactive', label: 'Inactive' },
    { key: 'expired', label: 'Expired' },
];

const TERMINAL_PROCESS_STATUSES = new Set(['completed', 'failed', 'cancelled']);

export interface JobInventoryPanelProps {
    stats?: {
        job_post_total?: number;
        active_job_posts?: number;
        inactive_job_posts?: number;
        expired_job_posts?: number;
        ready_to_score_job_posts?: number;
        active_ready_to_score_job_posts?: number;
        pending_extraction_job_posts?: number;
        retryable_extraction_job_posts?: number;
        pending_embedding_job_posts?: number;
        retryable_embedding_job_posts?: number;
        active_pending_extraction_job_posts?: number;
        active_retryable_extraction_job_posts?: number;
        inactive_pending_extraction_job_posts?: number;
        ready_for_extraction_job_posts?: number;
        active_ready_for_extraction_job_posts?: number;
        active_pending_embedding_job_posts?: number;
        active_retryable_embedding_job_posts?: number;
        inactive_pending_embedding_job_posts?: number;
        missing_description_job_posts?: number;
        active_missing_description_job_posts?: number;
        inactive_missing_description_job_posts?: number;
        description_recovery_queued_job_posts?: number;
        description_recovery_retryable_job_posts?: number;
        description_recovery_unavailable_job_posts?: number;
        active_recoverable_missing_description_job_posts?: number;
        description_recovery_posting_not_found_job_posts?: number;
        description_recovery_adapter_missing_job_posts?: number;
        description_recovery_prohibited_job_posts?: number;
        description_recovery_unmapped_job_posts?: number;
        oldest_missing_description_age_seconds?: number;
    } | null;
}

function formatDateTime(value?: string | null): string {
    if (!value) return 'Not recorded';
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return 'Not recorded';
    return new Intl.DateTimeFormat(undefined, {
        month: 'short',
        day: 'numeric',
        hour: 'numeric',
        minute: '2-digit',
    }).format(date);
}

function formatDuration(seconds?: number | null): string {
    if (seconds == null) return 'Unknown';
    if (seconds < 60) return `${Math.max(Math.round(seconds), 0)}s`;
    const minutes = Math.floor(seconds / 60);
    const remainder = Math.round(seconds % 60);
    return remainder ? `${minutes}m ${remainder}s` : `${minutes}m`;
}

function statusLabel(job: JobInventoryItem): string {
    if (isMissingDescription(job)) return 'Missing description';
    if (job.is_extracted && job.is_embedded) return 'Ready';
    if (job.extraction_status === 'in_progress' || job.extraction_status === 'processing') return 'Extracting';
    if (job.embedding_status === 'in_progress' || job.embedding_status === 'processing') return 'Embedding';
    if (job.extraction_status === 'failed_retryable' || job.embedding_status === 'failed_retryable') return 'Retry queued';
    if (job.extraction_status === 'failed_terminal' || job.extraction_status === 'failed' || job.embedding_status === 'failed_terminal' || job.embedding_status === 'failed') return 'Failed';
    if (!job.is_extracted) return `Extract ${job.extraction_status}`;
    if (!job.is_embedded) return `Embed ${job.embedding_status}`;
    return 'Imported';
}

function statusTone(job: JobInventoryItem): string {
    if (job.is_extracted && job.is_embedded) return 'border-success/40 bg-success-soft text-ink';
    if (isMissingDescription(job)) return 'border-warn/50 bg-warn-soft text-ink';
    if (job.extraction_status === 'failed_terminal' || job.extraction_status === 'failed' || job.embedding_status === 'failed_terminal' || job.embedding_status === 'failed') return 'border-warn/50 bg-warn-soft text-warn';
    if (job.extraction_status === 'failed_retryable' || job.embedding_status === 'failed_retryable') return 'border-warn/50 bg-warn-soft text-ink';
    return 'border-rule bg-surface-sunk text-ink-soft';
}

function processingLine(job: JobInventoryItem): string {
    const extraction = `extract ${job.extraction_status}`;
    const embedding = `embed ${job.embedding_status}`;
    return `${extraction} · ${embedding}`;
}

function retryLine(job: JobInventoryItem): string | null {
    const retryAt = job.description_recovery_next_retry_at || job.extraction_next_retry_at || job.embedding_next_retry_at;
    if (!retryAt) return null;
    return `Retry ${formatDateTime(retryAt)}`;
}

function errorLine(job: JobInventoryItem): string | null {
    return job.description_recovery_last_error || job.extraction_last_error || job.embedding_last_error || null;
}

function isMissingDescription(job: JobInventoryItem): boolean {
    return (
        job.description_completeness === 'missing'
        || job.extraction_status === 'no_description'
        || job.description_recovery_status === 'pending'
        || job.description_recovery_status === 'queued'
        || job.description_recovery_status === 'refreshing'
        || job.description_recovery_status === 'failed_retryable'
        || job.description_recovery_status === 'source_unsupported'
        || job.description_recovery_status === 'source_prohibited'
        || job.description_recovery_status === 'source_unmapped'
        || job.description_recovery_status === 'source_adapter_missing'
    );
}

function recoveryLabel(status?: string | null): string {
    switch (status) {
        case 'queued':
            return 'Checking ATS';
        case 'refreshing':
            return 'Checking ATS';
        case 'description_found':
            return 'Description found';
        case 'posting_not_found':
            return 'Posting gone';
        case 'source_unmapped':
            return 'Configure ATS source';
        case 'source_adapter_missing':
            return 'Adapter missing';
        case 'source_prohibited':
            return 'Unsupported hosted';
        case 'source_unsupported':
            return 'Unsupported';
        case 'failed_retryable':
            return 'Retrying recovery';
        case 'failed_terminal':
            return 'Recovery failed';
        default:
            return 'Recovery pending';
    }
}

function recoveryTone(status?: string | null): string {
    if (status === 'description_found') return 'border-success/40 bg-success-soft text-ink';
    if (status === 'queued' || status === 'refreshing' || status === 'failed_retryable') return 'border-accent bg-accent-soft text-ink';
    if (status === 'posting_not_found' || status === 'source_prohibited' || status === 'source_unsupported' || status === 'source_unmapped' || status === 'source_adapter_missing' || status === 'failed_terminal') {
        return 'border-warn/50 bg-warn-soft text-warn';
    }
    return 'border-rule bg-surface-sunk text-ink-soft';
}

function recoveryDetail(job: JobInventoryItem): string | null {
    const status = job.description_recovery_status;
    if (!isMissingDescription(job) && status !== 'description_found') return null;
    if (status === 'source_unmapped') return 'Connect or map the ATS source, then retry.';
    if (status === 'source_adapter_missing') return 'This source needs a supported API adapter before recovery can run.';
    if (status === 'source_prohibited') return 'Hosted recovery will not scrape this source.';
    if (status === 'posting_not_found') return 'Marked inactive after authoritative ATS check.';
    if (status === 'failed_retryable' && job.description_recovery_next_retry_at) {
        return `Next recovery ${formatDateTime(job.description_recovery_next_retry_at)}`;
    }
    if (status === 'queued' || status === 'refreshing') return 'Background recovery is checking the ATS listing.';
    return job.description_recovery_reason ? job.description_recovery_reason.replace(/_/g, ' ') : null;
}

function disabledRecoveryCopy(job: JobInventoryItem): string {
    if (job.availability_actions?.includes('description_recovery_unavailable_deployment_disabled')) {
        return 'Recovery disabled for this hosted source.';
    }
    if (job.availability_actions?.includes('description_recovery_unavailable_adapter_missing')) {
        return 'This source needs a supported API adapter.';
    }
    return 'Connect a supported ATS source to recover this description.';
}

function InventoryJobRow({
    job,
    onRefreshDescription,
    refreshing,
}: Readonly<{
    job: JobInventoryItem;
    onRefreshDescription: (jobId: string) => void;
    refreshing: boolean;
}>) {
    const error = errorLine(job);
    const retry = retryLine(job);
    const canRefreshDescription = Boolean(job.availability_actions?.includes('refresh_description'));
    const recoveryText = recoveryDetail(job);
    return (
        <li className="grid gap-3 border-t border-rule py-4 md:grid-cols-[minmax(0,1fr)_14rem]">
            <div className="min-w-0">
                <div className="flex flex-wrap items-center gap-2">
                    <span className={`inline-flex min-h-7 items-center border px-2 py-1 text-[11px] uppercase tracking-[0.12em] ${statusTone(job)}`}>
                        {statusLabel(job)}
                    </span>
                    <span className="caption text-[10px] text-ink-muted">{job.status}</span>
                    {job.source_site ? (
                        <span className="caption inline-block max-w-[14rem] truncate text-[10px] text-ink-muted">{job.source_site}</span>
                    ) : null}
                    {job.source_is_active === false ? (
                        <span className="caption text-[10px] text-warn">source inactive</span>
                    ) : null}
                    {job.source_job_id ? (
                        <span className="caption inline-block max-w-[12rem] truncate text-[10px] text-ink-muted">id {job.source_job_id}</span>
                    ) : null}
                    {isMissingDescription(job) || job.description_recovery_status !== 'not_needed' ? (
                        <span className={`inline-flex min-h-6 max-w-full items-center border px-2 text-[10px] uppercase tracking-[0.1em] ${recoveryTone(job.description_recovery_status)}`}>
                            <span className="truncate">{recoveryLabel(job.description_recovery_status)}</span>
                        </span>
                    ) : null}
                </div>
                <h4 className="mt-2 break-words text-[15px] font-medium leading-6 text-ink">{job.title}</h4>
                <p className="mt-1 text-[13px] leading-5 text-ink-soft">
                    {job.company}
                    {job.location ? <span> · {job.location}</span> : null}
                    {job.is_remote ? <span> · Remote</span> : null}
                </p>
                <p className="mt-2 text-[12px] leading-5 text-ink-muted">
                    {processingLine(job)}
                    {retry ? <span> · {retry}</span> : null}
                </p>
                {recoveryText ? (
                    <p className="mt-1 max-w-[44rem] break-words text-[12px] leading-5 text-ink-muted">{recoveryText}</p>
                ) : null}
                {error ? (
                    <p className="mt-2 flex gap-2 text-[12px] leading-5 text-warn">
                        <AlertTriangle className="mt-0.5 h-3.5 w-3.5 flex-none" aria-hidden="true" />
                        <span className="min-w-0 break-words">{error}</span>
                    </p>
                ) : null}
                {canRefreshDescription || isMissingDescription(job) ? (
                    <div className="mt-3 flex flex-wrap items-center gap-2">
                        <button
                            type="button"
                            onClick={() => onRefreshDescription(job.job_id)}
                            disabled={!canRefreshDescription || refreshing}
                            className="inline-flex h-8 items-center justify-center gap-2 border border-rule px-3 text-[12px] text-ink-soft transition-colors hover:border-accent hover:text-accent disabled:cursor-not-allowed disabled:opacity-50"
                        >
                            <RefreshCw className={`h-3.5 w-3.5 ${refreshing ? 'animate-spin' : ''}`} aria-hidden="true" />
                            {refreshing ? 'Checking' : 'Refresh now'}
                        </button>
                        {!canRefreshDescription ? (
                            <span className="min-w-0 break-words text-[11px] text-ink-muted">
                                {disabledRecoveryCopy(job)}
                            </span>
                        ) : null}
                    </div>
                ) : null}
            </div>
            <div className="flex items-start justify-between gap-3 md:justify-end">
                <dl className="grid gap-1 text-[12px] text-ink-muted md:text-right">
                    <div>
                        <dt className="caption text-[10px]">Last seen</dt>
                        <dd className="text-ink-soft">{formatDateTime(job.source_last_seen_at ?? job.last_seen_at)}</dd>
                    </div>
                    <div>
                        <dt className="caption text-[10px]">Availability</dt>
                        <dd className="text-ink-soft">{job.availability_status ?? 'unknown'}</dd>
                    </div>
                    <div>
                        <dt className="caption text-[10px]">Description</dt>
                        <dd className="text-ink-soft">{job.description_completeness}</dd>
                    </div>
                </dl>
                {job.source_url ? (
                    <a
                        href={job.source_url}
                        target="_blank"
                        rel="noreferrer"
                        aria-label={`Open ${job.title}`}
                        className="inline-flex h-9 w-9 flex-none items-center justify-center border border-rule text-ink-soft transition-colors hover:border-accent hover:text-accent"
                    >
                        <ExternalLink className="h-4 w-4" aria-hidden="true" />
                    </a>
                ) : null}
            </div>
        </li>
    );
}

function FilterButton({
    active,
    label,
    onClick,
}: Readonly<{
    active: boolean;
    label: string;
    onClick: () => void;
}>) {
    return (
        <button
            type="button"
            onClick={onClick}
            className={`h-9 border px-3 text-[12px] font-medium transition-colors ${
                active
                    ? 'border-accent bg-accent-soft text-ink'
                    : 'border-rule bg-surface text-ink-soft hover:border-accent hover:text-accent'
            }`}
        >
            {label}
        </button>
    );
}

function runStatusTone(status: string): string {
    if (status === 'completed') return 'border-success/40 bg-success-soft text-ink';
    if (status === 'failed' || status === 'cancelled') return 'border-warn/50 bg-warn-soft text-warn';
    if (status === 'running') return 'border-accent bg-accent-soft text-ink';
    return 'border-rule bg-surface-sunk text-ink-soft';
}

function runTitle(run: PipelineRunSummary): string {
    return run.run_type.replace(/_/g, ' ');
}

function StageCounts({ run }: Readonly<{ run: PipelineRunSummary }>) {
    return (
        <dl className="grid grid-cols-2 gap-x-4 gap-y-1 text-[11px] text-ink-muted sm:grid-cols-4">
            <div>
                <dt>Queued</dt>
                <dd className="num text-ink">{run.queued_count}</dd>
            </div>
            <div>
                <dt>Done</dt>
                <dd className="num text-ink">{run.succeeded_count || run.processed_count}</dd>
            </div>
            <div>
                <dt>Failed</dt>
                <dd className="num text-ink">{run.failed_count}</dd>
            </div>
            <div>
                <dt>Skipped</dt>
                <dd className="num text-ink">{run.skipped_count}</dd>
            </div>
        </dl>
    );
}

function PipelineRunButton({
    run,
    active,
    onSelect,
}: Readonly<{
    run: PipelineRunSummary;
    active: boolean;
    onSelect: () => void;
}>) {
    return (
        <button
            type="button"
            onClick={onSelect}
            className={`grid w-full gap-2 border-t border-rule py-3 text-left transition-colors hover:bg-surface-sunk ${
                active ? 'bg-surface-sunk' : ''
            }`}
        >
            <div className="flex min-w-0 flex-wrap items-center gap-2 px-3">
                <span className={`inline-flex min-h-6 items-center border px-2 text-[10px] uppercase tracking-[0.12em] ${runStatusTone(run.status)}`}>
                    {run.status}
                </span>
                <span className="min-w-0 break-words text-[13px] font-medium capitalize text-ink">
                    {runTitle(run)}
                </span>
                {run.current_stage ? (
                    <span className="caption text-[10px] text-ink-muted">{run.current_stage}</span>
                ) : null}
            </div>
            <div className="px-3">
                <StageCounts run={run} />
                <p className="mt-2 break-all text-[11px] text-ink-muted">{run.task_id}</p>
            </div>
        </button>
    );
}

function PipelineRunDetail({
    run,
    isLoading,
    onCancel,
    onRequeue,
    onRetry,
    actionPending,
}: Readonly<{
    run?: PipelineRunSummary | null;
    isLoading: boolean;
    onCancel: (runId: string) => void;
    onRequeue: (runId: string) => void;
    onRetry: (runId: string) => void;
    actionPending: boolean;
}>) {
    if (isLoading) {
        return <div className="border-t border-rule p-4 text-[13px] text-ink-muted">Loading run</div>;
    }
    if (!run) {
        return <div className="border-t border-rule p-4 text-[13px] text-ink-muted">No pipeline run selected.</div>;
    }
    return (
        <div className="border-t border-rule p-4">
            <div className="flex flex-wrap items-center justify-between gap-2">
                <div className="min-w-0">
                    <p className="caption">Run detail</p>
                    <h4 className="mt-1 break-words text-[14px] font-medium capitalize text-ink">{runTitle(run)}</h4>
                </div>
                <span className={`inline-flex min-h-7 items-center border px-2 text-[10px] uppercase tracking-[0.12em] ${runStatusTone(run.status)}`}>
                    {run.status}
                </span>
            </div>
            <div className="mt-3">
                <StageCounts run={run} />
            </div>
            <dl className="mt-3 grid gap-2 text-[12px] text-ink-muted">
                <div className="flex justify-between gap-3">
                    <dt>Started</dt>
                    <dd className="text-right text-ink-soft">{formatDateTime(run.started_at ?? run.created_at)}</dd>
                </div>
                <div className="flex justify-between gap-3">
                    <dt>Heartbeat</dt>
                    <dd className="text-right text-ink-soft">{formatDateTime(run.heartbeat_at)}</dd>
                </div>
                <div className="flex justify-between gap-3">
                    <dt>Retry</dt>
                    <dd className="text-right text-ink-soft">{run.retry_eligible ? 'Eligible' : 'No'}</dd>
                </div>
            </dl>
            {run.allowed_actions.length > 0 ? (
                <div className="mt-3 flex flex-wrap gap-2">
                    {run.allowed_actions.includes('cancel') ? (
                        <button
                            type="button"
                            onClick={() => onCancel(run.id)}
                            disabled={actionPending}
                            aria-label="Cancel pipeline run"
                            className="inline-flex h-8 items-center justify-center gap-2 border border-rule px-3 text-[12px] text-ink-soft transition-colors hover:border-warn hover:text-warn disabled:cursor-not-allowed disabled:opacity-50"
                        >
                            <XCircle className="h-3.5 w-3.5" aria-hidden="true" />
                            Cancel
                        </button>
                    ) : null}
                    {run.allowed_actions.includes('requeue') ? (
                        <button
                            type="button"
                            onClick={() => onRequeue(run.id)}
                            disabled={actionPending}
                            aria-label="Requeue pipeline run"
                            className="inline-flex h-8 items-center justify-center gap-2 border border-rule px-3 text-[12px] text-ink-soft transition-colors hover:border-accent hover:text-accent disabled:cursor-not-allowed disabled:opacity-50"
                        >
                            <RefreshCw className="h-3.5 w-3.5" aria-hidden="true" />
                            Requeue
                        </button>
                    ) : null}
                    {run.allowed_actions.includes('retry') ? (
                        <button
                            type="button"
                            onClick={() => onRetry(run.id)}
                            disabled={actionPending}
                            aria-label="Retry pipeline run"
                            className="inline-flex h-8 items-center justify-center gap-2 border border-rule px-3 text-[12px] text-ink-soft transition-colors hover:border-accent hover:text-accent disabled:cursor-not-allowed disabled:opacity-50"
                        >
                            <RotateCcw className="h-3.5 w-3.5" aria-hidden="true" />
                            Retry
                        </button>
                    ) : null}
                </div>
            ) : null}
            {run.last_error ? (
                <p className="mt-3 flex gap-2 text-[12px] leading-5 text-warn">
                    <AlertTriangle className="mt-0.5 h-3.5 w-3.5 flex-none" aria-hidden="true" />
                    <span className="min-w-0 break-words">{run.last_error}</span>
                </p>
            ) : null}
            {run.stages.length > 0 ? (
                <ol className="mt-3 border-t border-rule">
                    {run.stages.map((stage) => (
                        <li key={stage.id} className="grid gap-1 border-b border-rule py-2">
                            <div className="flex flex-wrap items-center gap-2">
                                <span className="text-[12px] font-medium text-ink">{stage.stage}</span>
                                <span className={`inline-flex min-h-5 items-center border px-1.5 text-[9px] uppercase tracking-[0.1em] ${runStatusTone(stage.status)}`}>
                                    {stage.status}
                                </span>
                                {stage.retry_eligible ? <span className="text-[11px] text-warn">Retryable</span> : null}
                            </div>
                            <p className="num text-[11px] text-ink-muted">
                                q {stage.queued_count} / ok {stage.succeeded_count || stage.processed_count} / fail {stage.failed_count}
                            </p>
                        </li>
                    ))}
                </ol>
            ) : null}
        </div>
    );
}

function LlmQueueHealth({
    status,
    providerStatus,
    canaryResult,
    isLoading,
    error,
    actionPending,
    canaryPending,
    onPause,
    onResume,
    onRetry,
    onCanary,
    onResetCircuit,
}: Readonly<{
    status?: LlmEvaluationQueueStatusResponse;
    providerStatus?: LlmProviderStatusResponse;
    canaryResult?: LlmProviderCanaryResponse;
    isLoading: boolean;
    error: unknown;
    actionPending: boolean;
    canaryPending: boolean;
    onPause: () => void;
    onResume: () => void;
    onRetry: () => void;
    onCanary: () => void;
    onResetCircuit: (provider: string, model: string) => void;
}>) {
    const failed = status?.failed ?? 0;
    const active = (status?.queued ?? 0) + (status?.started ?? 0) + (status?.deferred ?? 0) + (status?.scheduled ?? 0);
    const retryableFailed = status?.db_retryable_failed ?? 0;
    const pending = status?.db_pending ?? 0;
    const paused = Boolean(status?.paused);
    const oldestPendingAge = status?.oldest_pending_age_seconds ?? status?.oldest_retryable_failed_age_seconds ?? null;
    const providers = providerStatus?.providers ?? [];
    const canaryByProvider = new Map(
        (canaryResult?.results ?? []).map((result) => [`${result.name}:${result.model}`, result]),
    );
    return (
        <div className="border-t border-rule px-4 py-3">
            <div className="flex flex-wrap items-center justify-between gap-3">
                <div className="min-w-0">
                    <p className="caption text-[10px]">LLM queue</p>
                    <p className={`mt-1 text-[12px] ${status?.ready && !paused ? 'text-ink-soft' : 'text-warn'}`}>
                        {isLoading
                            ? 'Checking queue'
                            : error
                                ? 'Queue status unavailable'
                                : paused
                                    ? `Paused${status?.pause_ttl_seconds ? ` for ${formatDuration(status.pause_ttl_seconds)}` : ''}`
                                    : status?.ready
                                    ? 'Ready'
                                    : 'Degraded'}
                    </p>
                </div>
                <dl className="flex flex-wrap gap-x-4 gap-y-1 text-[11px] text-ink-muted">
                    <div className="flex gap-1">
                        <dt>Active</dt>
                        <dd className="num text-ink">{active}</dd>
                    </div>
                    <div className="flex gap-1">
                        <dt>Pending</dt>
                        <dd className="num text-ink">{pending}</dd>
                    </div>
                    <div className="flex gap-1">
                        <dt>Retryable</dt>
                        <dd className={`num ${retryableFailed > 0 ? 'text-warn' : 'text-ink'}`}>{retryableFailed}</dd>
                    </div>
                    <div className="flex gap-1">
                        <dt>Failed</dt>
                        <dd className={`num ${failed > 0 ? 'text-warn' : 'text-ink'}`}>{failed}</dd>
                    </div>
                    {oldestPendingAge != null ? (
                        <div className="flex gap-1">
                            <dt>Oldest</dt>
                            <dd className="num text-ink">{formatDuration(oldestPendingAge)}</dd>
                        </div>
                    ) : null}
                </dl>
            </div>
            <div className="mt-3 flex flex-wrap gap-2">
                <button
                    type="button"
                    onClick={paused ? onResume : onPause}
                    disabled={actionPending}
                    aria-label={paused ? 'Resume LLM queue' : 'Pause LLM queue'}
                    className="inline-flex h-8 items-center justify-center gap-2 border border-rule px-3 text-[12px] text-ink-soft transition-colors hover:border-accent hover:text-accent disabled:cursor-not-allowed disabled:opacity-50"
                >
                    {paused ? 'Resume' : 'Pause'}
                </button>
                <button
                    type="button"
                    onClick={onRetry}
                    disabled={actionPending || retryableFailed <= 0}
                    aria-label="Retry pending LLM evaluations"
                    className="inline-flex h-8 items-center justify-center gap-2 border border-rule px-3 text-[12px] text-ink-soft transition-colors hover:border-accent hover:text-accent disabled:cursor-not-allowed disabled:opacity-50"
                >
                    Retry backlog
                </button>
                <button
                    type="button"
                    onClick={onCanary}
                    disabled={canaryPending}
                    aria-label="Run LLM provider canary"
                    className="inline-flex h-8 items-center justify-center gap-2 border border-rule px-3 text-[12px] text-ink-soft transition-colors hover:border-accent hover:text-accent disabled:cursor-not-allowed disabled:opacity-50"
                >
                    {canaryPending ? 'Checking' : 'Run canary'}
                </button>
            </div>
            {status?.drain_estimate_seconds != null ? (
                <p className="mt-2 text-[11px] leading-5 text-ink-muted">
                    Estimated drain {formatDuration(status.drain_estimate_seconds)} from configured provider RPM.
                </p>
            ) : null}
            {status?.error ? (
                <p className="mt-2 break-words text-[11px] leading-5 text-warn">{status.error}</p>
            ) : null}
            {providers.length > 0 ? (
                <ol className="mt-3 grid gap-2 border-t border-rule pt-3">
                    {providers.map((provider) => {
                        const canary = canaryByProvider.get(`${provider.name}:${provider.model}`)
                            ?? (provider.last_canary_status
                                ? {
                                    status: provider.last_canary_status,
                                    error: provider.last_canary_error ?? null,
                                    error_category: provider.last_canary_error_category ?? null,
                                    elapsed_ms: provider.last_canary_elapsed_ms ?? 0,
                                    checked_at: provider.last_canary_checked_at ?? null,
                                }
                                : null);
                        const circuitOpen = provider.circuit_open;
                        return (
                            <li key={`${provider.name}:${provider.model}`} className="grid gap-1 text-[11px] text-ink-muted">
                                <div className="flex flex-wrap items-center justify-between gap-2">
                                    <span className="min-w-0 break-words text-[12px] font-medium text-ink">
                                        {provider.name} · {provider.model}
                                    </span>
                                    <span className={circuitOpen ? 'text-warn' : 'text-ink-soft'}>
                                        {circuitOpen
                                            ? `Circuit open ${formatDuration(provider.circuit_retry_after_seconds)}`
                                            : 'Circuit closed'}
                                    </span>
                                </div>
                                <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
                                    <span>{provider.requests_per_minute ?? 'No'} rpm</span>
                                    <span>failures {provider.circuit_failure_count}</span>
                                    {canary ? (
                                        <span className={canary.status === 'succeeded' ? 'text-ink-soft' : 'text-warn'}>
                                            canary {canary.status}
                                            {canary.elapsed_ms ? ` ${canary.elapsed_ms}ms` : ''}
                                        </span>
                                    ) : null}
                                    {circuitOpen ? (
                                        <button
                                            type="button"
                                            onClick={() => onResetCircuit(provider.name, provider.model)}
                                            disabled={actionPending}
                                            className="text-accent underline-offset-2 hover:underline disabled:cursor-not-allowed disabled:opacity-50"
                                        >
                                            Reset
                                        </button>
                                    ) : null}
                                </div>
                                {canary?.error ? (
                                    <p className="break-words text-warn">{canary.error}</p>
                                ) : null}
                            </li>
                        );
                    })}
                </ol>
            ) : null}
        </div>
    );
}

function BlockerRow({ blocker }: Readonly<{ blocker: ProcessingBlockerItem }>) {
    return (
        <li className="border-t border-rule py-3">
            <div className="flex flex-wrap items-center gap-2">
                <span className="inline-flex min-h-6 items-center border border-rule bg-surface-sunk px-2 text-[10px] uppercase tracking-[0.12em] text-ink-soft">
                    {blocker.stage}
                </span>
                <span className="break-words text-[12px] font-medium text-ink">{blocker.blocker_code}</span>
                {blocker.recovery_status ? (
                    <span className={`inline-flex min-h-6 max-w-full items-center border px-2 text-[10px] uppercase tracking-[0.1em] ${recoveryTone(blocker.recovery_status)}`}>
                        <span className="truncate">{recoveryLabel(blocker.recovery_status)}</span>
                    </span>
                ) : null}
                {blocker.retry_eligible ? <span className="text-[11px] text-warn">Retryable</span> : null}
            </div>
            <p className="mt-2 text-[12px] leading-5 text-ink-soft">{blocker.blocker_detail}</p>
            {blocker.recovery_reason ? (
                <p className="mt-1 break-words text-[11px] leading-5 text-ink-muted">
                    Recovery: {blocker.recovery_reason.replace(/_/g, ' ')}
                </p>
            ) : null}
            <p className="mt-1 text-[11px] text-ink-muted">
                {blocker.status} · attempts <span className="num">{blocker.attempts}</span> · {formatDateTime(blocker.last_attempt_at ?? blocker.first_seen_at)}
            </p>
        </li>
    );
}

export const JobInventoryPanel: React.FC<JobInventoryPanelProps> = ({ stats }) => {
    const [isOpen, setIsOpen] = useState(false);
    const [processingStatus, setProcessingStatus] = useState<JobProcessingStatus>('all');
    const [jobStatus, setJobStatus] = useState<JobLifecycleStatus>('all');
    const [searchInput, setSearchInput] = useState('');
    const [search, setSearch] = useState('');
    const [offset, setOffset] = useState(0);
    const [processTaskId, setProcessTaskId] = useState<string | null>(null);
    const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
    const queryClient = useQueryClient();
    const pendingExtraction = (stats?.pending_extraction_job_posts ?? 0) + (stats?.retryable_extraction_job_posts ?? 0);
    const pendingEmbedding = (stats?.pending_embedding_job_posts ?? 0) + (stats?.retryable_embedding_job_posts ?? 0);
    const hasExtractionBreakdown = stats?.active_pending_extraction_job_posts != null
        || stats?.active_retryable_extraction_job_posts != null
        || stats?.inactive_pending_extraction_job_posts != null;
    const hasEmbeddingBreakdown = stats?.active_pending_embedding_job_posts != null
        || stats?.active_retryable_embedding_job_posts != null
        || stats?.inactive_pending_embedding_job_posts != null;
    const activePendingExtraction = stats?.active_ready_for_extraction_job_posts ?? (hasExtractionBreakdown
        ? (stats?.active_pending_extraction_job_posts ?? 0) + (stats?.active_retryable_extraction_job_posts ?? 0)
        : pendingExtraction);
    const activePendingEmbedding = hasEmbeddingBreakdown
        ? (stats?.active_pending_embedding_job_posts ?? 0) + (stats?.active_retryable_embedding_job_posts ?? 0)
        : pendingEmbedding;
    const inactivePendingExtraction = hasExtractionBreakdown ? stats?.inactive_pending_extraction_job_posts ?? 0 : 0;
    const inactivePendingEmbedding = hasEmbeddingBreakdown ? stats?.inactive_pending_embedding_job_posts ?? 0 : 0;
    const activeQueuedWork = activePendingExtraction + activePendingEmbedding;
    const inactiveQueuedWork = inactivePendingExtraction + inactivePendingEmbedding;
    const queuedWork = activeQueuedWork;
    const missingDescriptions = stats?.missing_description_job_posts ?? 0;
    const activeMissingDescriptions = stats?.active_missing_description_job_posts ?? missingDescriptions;
    const recoveryQueued = stats?.description_recovery_queued_job_posts ?? 0;
    const recoveryRetryable = stats?.description_recovery_retryable_job_posts ?? 0;
    const recoveryUnavailable = stats?.description_recovery_unavailable_job_posts ?? 0;
    const recoverableMissing = stats?.active_recoverable_missing_description_job_posts ?? activeMissingDescriptions;
    const recoveryGone = stats?.description_recovery_posting_not_found_job_posts ?? 0;
    const recoveryAdapterMissing = stats?.description_recovery_adapter_missing_job_posts ?? 0;
    const recoveryProhibited = stats?.description_recovery_prohibited_job_posts ?? 0;
    const recoveryUnmapped = stats?.description_recovery_unmapped_job_posts ?? 0;
    const { data, isLoading, error, refetch } = useJobs({
        job_status: jobStatus,
        processing_status: processingStatus,
        search: search || undefined,
        limit: PAGE_SIZE,
        offset,
    }, isOpen);
    const processJobs = useMutation({
        mutationFn: async () => {
            const response = await pipelineApi.processJobs();
            return response.data;
        },
        onSuccess: (response) => {
            setProcessTaskId(response.task_id);
            void queryClient.invalidateQueries({ queryKey: ['stats'] });
            void queryClient.invalidateQueries({ queryKey: ['jobs'] });
        },
    });
    const processStatus = useQuery({
        queryKey: ['job-processing-status', processTaskId],
        queryFn: async () => {
            const response = await pipelineApi.getPipelineStatus(processTaskId ?? '');
            return response.data;
        },
        enabled: Boolean(processTaskId),
        refetchInterval: (query) => {
            const status = (query.state.data as PipelineStatusResponse | undefined)?.status;
            return status && TERMINAL_PROCESS_STATUSES.has(status) ? false : 2500;
        },
    });
    const pipelineRuns = useQuery({
        queryKey: ['pipeline-runs', 'latest'],
        queryFn: async () => {
            const response = await pipelineRunsApi.getPipelineRuns({ limit: 5, view: 'compact' });
            return response.data;
        },
        refetchInterval: 5000,
    });
    const llmQueueStatus = useQuery({
        queryKey: ['llm-evaluation-queue'],
        queryFn: async () => {
            const response = await pipelineRunsApi.getLlmEvaluationQueueStatus();
            return response.data;
        },
        refetchInterval: 10000,
    });
    const llmProviderStatus = useQuery({
        queryKey: ['llm-provider-status'],
        queryFn: async () => {
            const response = await pipelineRunsApi.getLlmProviderStatus();
            return response.data;
        },
        refetchInterval: 30000,
    });
    const processingBlockers = useQuery({
        queryKey: ['processing-blockers', 'oldest'],
        queryFn: async () => {
            const response = await jobsApi.getProcessingBlockers({ stage: 'all', limit: 5, view: 'compact' });
            return response.data;
        },
        refetchInterval: 10000,
    });
    const selectedRunDetail = useQuery({
        queryKey: ['pipeline-run-detail', selectedRunId],
        queryFn: async () => {
            const response = await pipelineRunsApi.getPipelineRun(selectedRunId ?? '');
            return response.data;
        },
        enabled: Boolean(selectedRunId),
        refetchInterval: (query) => {
            const status = query.state.data?.run.status;
            return status && TERMINAL_PROCESS_STATUSES.has(status) ? false : 5000;
        },
    });
    const invalidatePipelineOps = () => {
        void queryClient.invalidateQueries({ queryKey: ['pipeline-runs'] });
        void queryClient.invalidateQueries({ queryKey: ['pipeline-run-detail'] });
        void queryClient.invalidateQueries({ queryKey: ['processing-blockers'] });
        void queryClient.invalidateQueries({ queryKey: ['llm-evaluation-queue'] });
        void queryClient.invalidateQueries({ queryKey: ['llm-provider-status'] });
    };
    const cancelRun = useMutation({
        mutationFn: async (runId: string) => {
            const response = await pipelineRunsApi.cancelPipelineRun(runId);
            return response.data;
        },
        onSuccess: invalidatePipelineOps,
    });
    const requeueRun = useMutation({
        mutationFn: async (runId: string) => {
            const response = await pipelineRunsApi.requeuePipelineRun(runId);
            return response.data;
        },
        onSuccess: invalidatePipelineOps,
    });
    const retryRun = useMutation({
        mutationFn: async (runId: string) => {
            const response = await pipelineRunsApi.retryPipelineRun(runId);
            return response.data;
        },
        onSuccess: invalidatePipelineOps,
    });
    const pauseLlmQueue = useMutation({
        mutationFn: async () => {
            const response = await pipelineRunsApi.pauseLlmEvaluationQueue('operator pause', 3600);
            return response.data;
        },
        onSuccess: invalidatePipelineOps,
    });
    const resumeLlmQueue = useMutation({
        mutationFn: async () => {
            const response = await pipelineRunsApi.resumeLlmEvaluationQueue();
            return response.data;
        },
        onSuccess: invalidatePipelineOps,
    });
    const retryLlmQueue = useMutation({
        mutationFn: async () => {
            const response = await pipelineRunsApi.retryLlmEvaluationQueue(100);
            return response.data;
        },
        onSuccess: invalidatePipelineOps,
    });
    const invalidateJobInventory = () => {
        void queryClient.invalidateQueries({ queryKey: ['stats'] });
        void queryClient.invalidateQueries({ queryKey: ['jobs'] });
        void queryClient.invalidateQueries({ queryKey: ['processing-blockers'] });
        void queryClient.invalidateQueries({ queryKey: ['pipeline-runs'] });
    };
    const refreshDescription = useMutation({
        mutationFn: async (jobId: string) => {
            const response = await jobsApi.refreshJobDescriptionNow(jobId);
            return response.data;
        },
        onSuccess: invalidateJobInventory,
    });
    const sweepDescriptionRecovery = useMutation({
        mutationFn: async () => {
            const response = await jobsApi.sweepDescriptionRecovery(25);
            return response.data;
        },
        onSuccess: invalidateJobInventory,
    });
    const runLlmCanary = useMutation({
        mutationFn: async () => {
            const response = await pipelineRunsApi.runLlmProviderCanaries();
            return response.data;
        },
        onSuccess: invalidatePipelineOps,
    });
    const resetLlmCircuit = useMutation({
        mutationFn: async ({ provider, model }: { provider: string; model: string }) => {
            const response = await pipelineRunsApi.resetLlmProviderCircuit(provider, model);
            return response.data;
        },
        onSuccess: invalidatePipelineOps,
    });

    const total = data?.total ?? 0;
    const jobs = data?.jobs ?? [];
    const latestRuns = pipelineRuns.data?.runs ?? [];
    const blockers = processingBlockers.data?.blockers ?? [];
    const firstRunId = latestRuns[0]?.id ?? null;
    const selectedRun = selectedRunDetail.data?.run
        ?? latestRuns.find((run) => run.id === selectedRunId)
        ?? null;
    const pipelineActionPending = cancelRun.isPending || requeueRun.isPending || retryRun.isPending;
    const llmQueueActionPending = (
        pauseLlmQueue.isPending
        || resumeLlmQueue.isPending
        || retryLlmQueue.isPending
        || resetLlmCircuit.isPending
    );
    const canPageBack = offset > 0;
    const canPageForward = offset + PAGE_SIZE < total;
    const processStatusData = processStatus.data;
    const processedJobs = processStatusData?.stats?.jobs_processed ?? processStatusData?.stats?.jobs_embedded ?? 0;
    const importedJobs = processStatusData?.stats?.jobs_imported;
    const processStatusText = processTaskId
        ? processStatusData?.status === 'completed'
            ? importedJobs != null
                ? `Imported ${importedJobs} jobs and processed ${processedJobs} for semantic search.`
                : `Processed ${processedJobs} jobs for semantic search.`
            : processStatusData?.status === 'failed'
                ? 'Queued job processing failed. Check logs, then retry.'
                : 'Processing imported jobs in the background.'
        : null;
    const processingActive = (
        processJobs.isPending
        || Boolean(processTaskId && !TERMINAL_PROCESS_STATUSES.has(processStatusData?.status ?? 'pending'))
    );

    const resetOffset = () => setOffset(0);

    useEffect(() => {
        if (!processStatusData?.status || !TERMINAL_PROCESS_STATUSES.has(processStatusData.status)) {
            return;
        }
        void queryClient.invalidateQueries({ queryKey: ['stats'] });
        void queryClient.invalidateQueries({ queryKey: ['jobs'] });
        void queryClient.invalidateQueries({ queryKey: ['pipeline-runs'] });
        void queryClient.invalidateQueries({ queryKey: ['processing-blockers'] });
    }, [processStatusData?.status, queryClient]);

    useEffect(() => {
        if (!selectedRunId && firstRunId) {
            setSelectedRunId(firstRunId);
        }
    }, [firstRunId, selectedRunId]);

    return (
        <section className="mt-8 border-t border-rule pt-8">
            <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
                <div>
                    <p className="caption">Job inventory</p>
                    <h3 className="mt-2 text-[16px] font-medium text-ink">Imported jobs</h3>
                    <dl className="mt-3 flex flex-wrap gap-x-5 gap-y-2 text-[12px] text-ink-muted">
                        <div className="flex min-w-0 gap-1.5">
                            <dt>Imported</dt>
                            <dd className="num text-ink">{stats?.job_post_total ?? 0}</dd>
                        </div>
                        <div className="flex min-w-0 gap-1.5">
                            <dt>Active</dt>
                            <dd className="num text-ink">{stats?.active_job_posts ?? 0}</dd>
                        </div>
                        <div className="flex min-w-0 gap-1.5">
                            <dt>Ready active</dt>
                            <dd className="num text-ink">
                                {stats?.active_ready_to_score_job_posts ?? stats?.ready_to_score_job_posts ?? 0}
                            </dd>
                        </div>
                        <div className="flex min-w-0 gap-1.5">
                            <dt>Queued active</dt>
                            <dd className="num text-ink">{activeQueuedWork}</dd>
                        </div>
                        <div className="flex min-w-0 gap-1.5">
                            <dt>Inactive queued</dt>
                            <dd className="num text-ink">{inactiveQueuedWork}</dd>
                        </div>
                        <div className="flex min-w-0 gap-1.5">
                            <dt>Missing desc</dt>
                            <dd className="num text-ink">{missingDescriptions}</dd>
                        </div>
                        <div className="flex min-w-0 gap-1.5">
                            <dt>Checking ATS</dt>
                            <dd className="num text-ink">{recoveryQueued}</dd>
                        </div>
                    </dl>
                    {(pendingExtraction + pendingEmbedding + missingDescriptions) > 0 ? (
                        <p className="mt-3 max-w-[44rem] break-words text-[12px] leading-5 text-ink-muted">
                            Ready extraction and embedding jobs can be processed now. Missing-description jobs are recovered through
                            compliant ATS APIs in the background; unsupported sources need a source mapping or remain inactive.
                        </p>
                    ) : null}
                    {missingDescriptions > 0 ? (
                        <p className="mt-2 max-w-[44rem] break-words text-[12px] leading-5 text-ink-muted">
                            <span className="num text-ink">{activeMissingDescriptions}</span> active missing descriptions
                            {recoverableMissing ? <span> · <span className="num text-ink">{recoverableMissing}</span> recoverable</span> : null}
                            {recoveryRetryable ? <span> · <span className="num text-ink">{recoveryRetryable}</span> retrying</span> : null}
                            {recoveryQueued ? <span> · <span className="num text-ink">{recoveryQueued}</span> checking ATS</span> : null}
                            {recoveryGone ? <span> · <span className="num text-ink">{recoveryGone}</span> postings gone</span> : null}
                            {recoveryAdapterMissing ? <span> · <span className="num text-ink">{recoveryAdapterMissing}</span> adapter missing</span> : null}
                            {recoveryUnmapped ? <span> · <span className="num text-ink">{recoveryUnmapped}</span> need source setup</span> : null}
                            {recoveryProhibited ? <span> · <span className="num text-ink">{recoveryProhibited}</span> hosted-disabled</span> : null}
                            {recoveryUnavailable && !(recoveryAdapterMissing || recoveryUnmapped || recoveryProhibited) ? <span> · <span className="num text-ink">{recoveryUnavailable}</span> unavailable</span> : null}
                            {stats?.oldest_missing_description_age_seconds ? (
                                <span> · oldest {formatDuration(stats.oldest_missing_description_age_seconds)}</span>
                            ) : null}
                        </p>
                    ) : null}
                    {processStatusText ? (
                        <p className="mt-3 text-[12px] leading-5 text-ink-muted">{processStatusText}</p>
                    ) : null}
                </div>
                <div className="flex flex-wrap gap-2">
                    <button
                        type="button"
                        onClick={() => processJobs.mutate()}
                        disabled={queuedWork <= 0 || processingActive}
                        aria-label="Process queued imported jobs"
                        className="inline-flex h-10 items-center justify-center gap-2 border border-rule px-4 text-[14px] font-medium text-ink-soft transition-colors hover:border-accent hover:text-accent disabled:cursor-not-allowed disabled:opacity-50"
                    >
                        <RefreshCw className={`h-4 w-4 ${processingActive ? 'animate-pulse text-accent' : ''}`} aria-hidden="true" />
                        {processingActive ? 'Processing queued' : 'Process queued'}
                    </button>
                    <button
                        type="button"
                        onClick={() => sweepDescriptionRecovery.mutate()}
                        disabled={missingDescriptions <= 0 || sweepDescriptionRecovery.isPending}
                        aria-label="Recover missing job descriptions"
                        className="inline-flex h-10 items-center justify-center gap-2 border border-rule px-4 text-[14px] font-medium text-ink-soft transition-colors hover:border-accent hover:text-accent disabled:cursor-not-allowed disabled:opacity-50"
                    >
                        <RefreshCw className={`h-4 w-4 ${sweepDescriptionRecovery.isPending ? 'animate-spin text-accent' : ''}`} aria-hidden="true" />
                        {sweepDescriptionRecovery.isPending ? 'Checking ATS' : 'Recover descriptions'}
                    </button>
                    <button
                        type="button"
                        onClick={() => setIsOpen((value) => !value)}
                        aria-expanded={isOpen}
                        className="inline-flex h-10 items-center justify-center gap-2 border border-accent px-4 text-[14px] font-medium text-accent transition-colors hover:bg-accent-soft"
                    >
                        <ListChecks className="h-4 w-4" aria-hidden="true" />
                        {isOpen ? 'Hide jobs' : 'Browse jobs'}
                    </button>
                </div>
            </div>

            <div className="mt-5 grid gap-4 xl:grid-cols-[minmax(0,1.2fr)_minmax(18rem,0.8fr)]">
                <div className="border border-rule bg-surface">
                    <div className="flex items-center justify-between gap-3 p-4">
                        <div className="min-w-0">
                            <p className="caption">Pipeline runs</p>
                            <h4 className="mt-1 text-[14px] font-medium text-ink">Latest durable runs</h4>
                        </div>
                        <Activity className="h-4 w-4 flex-none text-ink-muted" aria-hidden="true" />
                    </div>
                    <LlmQueueHealth
                        status={llmQueueStatus.data}
                        providerStatus={llmProviderStatus.data}
                        canaryResult={runLlmCanary.data}
                        isLoading={llmQueueStatus.isLoading}
                        error={llmQueueStatus.error}
                        actionPending={llmQueueActionPending}
                        canaryPending={runLlmCanary.isPending}
                        onPause={() => pauseLlmQueue.mutate()}
                        onResume={() => resumeLlmQueue.mutate()}
                        onRetry={() => retryLlmQueue.mutate()}
                        onCanary={() => runLlmCanary.mutate()}
                        onResetCircuit={(provider, model) => resetLlmCircuit.mutate({ provider, model })}
                    />
                    {pipelineRuns.isLoading ? (
                        <div className="border-t border-rule p-4 text-[13px] text-ink-muted">Loading runs</div>
                    ) : pipelineRuns.error ? (
                        <div className="border-t border-rule p-4 text-[13px] text-warn">
                            {pipelineRuns.error instanceof Error ? pipelineRuns.error.message : 'Runs failed to load.'}
                        </div>
                    ) : latestRuns.length === 0 ? (
                        <div className="border-t border-rule p-4 text-[13px] text-ink-muted">No pipeline runs recorded.</div>
                    ) : (
                        <div className="grid lg:grid-cols-[minmax(0,1fr)_minmax(18rem,0.9fr)]">
                            <div className="border-t border-rule lg:border-r">
                                {latestRuns.map((run) => (
                                    <PipelineRunButton
                                        key={run.id}
                                        run={run}
                                        active={run.id === selectedRunId}
                                        onSelect={() => setSelectedRunId(run.id)}
                                    />
                                ))}
                            </div>
                            <PipelineRunDetail
                                run={selectedRun}
                                isLoading={selectedRunDetail.isFetching && Boolean(selectedRunId)}
                                onCancel={(runId) => cancelRun.mutate(runId)}
                                onRequeue={(runId) => requeueRun.mutate(runId)}
                                onRetry={(runId) => retryRun.mutate(runId)}
                                actionPending={pipelineActionPending}
                            />
                        </div>
                    )}
                </div>

                <div className="border border-rule bg-surface">
                    <div className="flex items-center justify-between gap-3 p-4">
                        <div className="min-w-0">
                            <p className="caption">Processing blockers</p>
                            <h4 className="mt-1 text-[14px] font-medium text-ink">Oldest blockers</h4>
                        </div>
                        <Clock3 className="h-4 w-4 flex-none text-ink-muted" aria-hidden="true" />
                    </div>
                    {processingBlockers.isLoading ? (
                        <div className="border-t border-rule p-4 text-[13px] text-ink-muted">Loading blockers</div>
                    ) : processingBlockers.error ? (
                        <div className="border-t border-rule p-4 text-[13px] text-warn">
                            {processingBlockers.error instanceof Error ? processingBlockers.error.message : 'Blockers failed to load.'}
                        </div>
                    ) : blockers.length === 0 ? (
                        <div className="border-t border-rule p-4 text-[13px] text-ink-muted">No blockers found.</div>
                    ) : (
                        <ol className="px-4">
                            {blockers.map((blocker) => (
                                <BlockerRow key={`${blocker.stage}-${blocker.job_id}-${blocker.blocker_code}`} blocker={blocker} />
                            ))}
                        </ol>
                    )}
                </div>
            </div>

            {isOpen ? (
                <div className="mt-5 border border-rule bg-surface">
                    <div className="grid gap-4 border-b border-rule p-4 lg:grid-cols-[1fr_auto] lg:items-end">
                        <div className="flex flex-wrap gap-2">
                            {PROCESSING_FILTERS.map((filter) => (
                                <FilterButton
                                    key={filter.key}
                                    active={processingStatus === filter.key}
                                    label={filter.label}
                                    onClick={() => {
                                        setProcessingStatus(filter.key);
                                        resetOffset();
                                    }}
                                />
                            ))}
                        </div>
                        <div className="flex flex-wrap gap-2 lg:justify-end">
                            {LIFECYCLE_FILTERS.map((filter) => (
                                <FilterButton
                                    key={filter.key}
                                    active={jobStatus === filter.key}
                                    label={filter.label}
                                    onClick={() => {
                                        setJobStatus(filter.key);
                                        resetOffset();
                                    }}
                                />
                            ))}
                        </div>
                        <form
                            className="flex min-w-0 gap-2 lg:col-span-2"
                            onSubmit={(event) => {
                                event.preventDefault();
                                setSearch(searchInput.trim());
                                resetOffset();
                            }}
                        >
                            <label className="relative flex-1">
                                <span className="sr-only">Search imported jobs</span>
                                <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-ink-muted" aria-hidden="true" />
                                <input
                                    value={searchInput}
                                    onChange={(event) => setSearchInput(event.target.value)}
                                    placeholder="Search title, company, location"
                                    className="h-10 w-full border border-rule bg-surface pl-9 pr-3 text-[14px] text-ink outline-none transition-colors placeholder:text-ink-muted focus:border-accent"
                                />
                            </label>
                            <button
                                type="submit"
                                className="inline-flex h-10 items-center justify-center border border-rule px-4 text-[13px] font-medium text-ink-soft transition-colors hover:border-accent hover:text-accent"
                            >
                                Search
                            </button>
                            <button
                                type="button"
                                onClick={() => void refetch()}
                                aria-label="Refresh imported jobs"
                                className="inline-flex h-10 w-10 items-center justify-center border border-rule text-ink-soft transition-colors hover:border-accent hover:text-accent"
                            >
                                <RefreshCw className={`h-4 w-4 ${isLoading ? 'animate-spin' : ''}`} aria-hidden="true" />
                            </button>
                        </form>
                    </div>

                    <div className="px-4">
                        {isLoading ? (
                            <div className="py-8 text-[13px] text-ink-muted">Loading jobs</div>
                        ) : error ? (
                            <div className="py-8 text-[13px] text-warn">
                                {error instanceof Error ? error.message : 'Jobs failed to load.'}
                            </div>
                        ) : jobs.length === 0 ? (
                            <div className="py-8 text-[13px] text-ink-muted">No jobs match this view.</div>
                        ) : (
                            <ol>
                                {jobs.map((job) => (
                                    <InventoryJobRow
                                        key={job.job_id}
                                        job={job}
                                        onRefreshDescription={(jobId) => refreshDescription.mutate(jobId)}
                                        refreshing={refreshDescription.isPending && refreshDescription.variables === job.job_id}
                                    />
                                ))}
                            </ol>
                        )}
                    </div>

                    <div className="flex flex-col gap-3 border-t border-rule px-4 py-3 sm:flex-row sm:items-center sm:justify-between">
                        <p className="text-[12px] text-ink-muted">
                            <span className="num text-ink">{Math.min(offset + 1, total || 0)}</span>
                            {' - '}
                            <span className="num text-ink">{Math.min(offset + PAGE_SIZE, total)}</span>
                            {' of '}
                            <span className="num text-ink">{total}</span>
                        </p>
                        <div className="flex gap-2">
                            <button
                                type="button"
                                disabled={!canPageBack}
                                onClick={() => setOffset(Math.max(0, offset - PAGE_SIZE))}
                                aria-label="Previous jobs page"
                                className="inline-flex h-9 w-9 items-center justify-center border border-rule text-ink-soft transition-colors hover:border-accent hover:text-accent disabled:cursor-not-allowed disabled:opacity-50"
                            >
                                <ChevronLeft className="h-4 w-4" aria-hidden="true" />
                            </button>
                            <button
                                type="button"
                                disabled={!canPageForward}
                                onClick={() => setOffset(offset + PAGE_SIZE)}
                                aria-label="Next jobs page"
                                className="inline-flex h-9 w-9 items-center justify-center border border-rule text-ink-soft transition-colors hover:border-accent hover:text-accent disabled:cursor-not-allowed disabled:opacity-50"
                            >
                                <ChevronRight className="h-4 w-4" aria-hidden="true" />
                            </button>
                        </div>
                    </div>
                </div>
            ) : null}
        </section>
    );
};
