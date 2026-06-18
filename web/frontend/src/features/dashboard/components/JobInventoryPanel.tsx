import React, { useEffect, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import {
    AlertTriangle,
    ChevronLeft,
    ChevronRight,
    ExternalLink,
    ListChecks,
    RefreshCw,
    Search,
} from 'lucide-react';

import { useJobs } from '@/hooks/useJobs';
import { pipelineApi } from '@/services/pipelineApi';
import type {
    JobInventoryItem,
    JobLifecycleStatus,
    JobProcessingStatus,
    PipelineStatusResponse,
} from '@/types/api';

const PAGE_SIZE = 50;

const PROCESSING_FILTERS: Array<{ key: JobProcessingStatus; label: string }> = [
    { key: 'all', label: 'All' },
    { key: 'ready', label: 'Ready' },
    { key: 'pending_extraction', label: 'Pending extract' },
    { key: 'pending_embedding', label: 'Pending embed' },
    { key: 'failed', label: 'Retry or failed' },
];

const LIFECYCLE_FILTERS: Array<{ key: JobLifecycleStatus; label: string }> = [
    { key: 'all', label: 'All jobs' },
    { key: 'active', label: 'Active' },
    { key: 'inactive', label: 'Inactive' },
];

const TERMINAL_PROCESS_STATUSES = new Set(['completed', 'failed', 'cancelled']);

export interface JobInventoryPanelProps {
    stats?: {
        job_post_total?: number;
        ready_to_score_job_posts?: number;
        pending_extraction_job_posts?: number;
        retryable_extraction_job_posts?: number;
        pending_embedding_job_posts?: number;
        retryable_embedding_job_posts?: number;
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

function statusLabel(job: JobInventoryItem): string {
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
    const retryAt = job.extraction_next_retry_at || job.embedding_next_retry_at;
    if (!retryAt) return null;
    return `Retry ${formatDateTime(retryAt)}`;
}

function errorLine(job: JobInventoryItem): string | null {
    return job.extraction_last_error || job.embedding_last_error || null;
}

function InventoryJobRow({ job }: Readonly<{ job: JobInventoryItem }>) {
    const error = errorLine(job);
    const retry = retryLine(job);
    return (
        <li className="grid gap-3 border-t border-rule py-4 md:grid-cols-[minmax(0,1fr)_13rem]">
            <div className="min-w-0">
                <div className="flex flex-wrap items-center gap-2">
                    <span className={`inline-flex min-h-7 items-center border px-2 py-1 text-[11px] uppercase tracking-[0.12em] ${statusTone(job)}`}>
                        {statusLabel(job)}
                    </span>
                    <span className="caption text-[10px] text-ink-muted">{job.status}</span>
                    {job.source_site ? (
                        <span className="caption text-[10px] text-ink-muted">{job.source_site}</span>
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
                {error ? (
                    <p className="mt-2 flex gap-2 text-[12px] leading-5 text-warn">
                        <AlertTriangle className="mt-0.5 h-3.5 w-3.5 flex-none" aria-hidden="true" />
                        <span className="min-w-0 break-words">{error}</span>
                    </p>
                ) : null}
            </div>
            <div className="flex items-start justify-between gap-3 md:justify-end">
                <dl className="grid gap-1 text-[12px] text-ink-muted md:text-right">
                    <div>
                        <dt className="caption text-[10px]">Last seen</dt>
                        <dd className="text-ink-soft">{formatDateTime(job.last_seen_at)}</dd>
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

export const JobInventoryPanel: React.FC<JobInventoryPanelProps> = ({ stats }) => {
    const [isOpen, setIsOpen] = useState(false);
    const [processingStatus, setProcessingStatus] = useState<JobProcessingStatus>('all');
    const [jobStatus, setJobStatus] = useState<JobLifecycleStatus>('all');
    const [searchInput, setSearchInput] = useState('');
    const [search, setSearch] = useState('');
    const [offset, setOffset] = useState(0);
    const [processTaskId, setProcessTaskId] = useState<string | null>(null);
    const queryClient = useQueryClient();
    const pendingExtraction = (stats?.pending_extraction_job_posts ?? 0) + (stats?.retryable_extraction_job_posts ?? 0);
    const pendingEmbedding = (stats?.pending_embedding_job_posts ?? 0) + (stats?.retryable_embedding_job_posts ?? 0);
    const queuedWork = pendingExtraction + pendingEmbedding;
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

    const total = data?.total ?? 0;
    const jobs = data?.jobs ?? [];
    const canPageBack = offset > 0;
    const canPageForward = offset + PAGE_SIZE < total;
    const processStatusData = processStatus.data;
    const processStatusText = processTaskId
        ? processStatusData?.status === 'completed'
            ? `Processed ${processStatusData.stats?.jobs_extracted ?? 0} extracted and ${processStatusData.stats?.jobs_embedded ?? 0} embedded jobs.`
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
    }, [processStatusData?.status, queryClient]);

    return (
        <section className="mt-8 border-t border-rule pt-8">
            <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
                <div>
                    <p className="caption">Job inventory</p>
                    <h3 className="mt-2 text-[16px] font-medium text-ink">Imported jobs</h3>
                    <dl className="mt-3 flex flex-wrap gap-x-5 gap-y-2 text-[12px] text-ink-muted">
                        <div className="flex gap-1.5">
                            <dt>Imported</dt>
                            <dd className="num text-ink">{stats?.job_post_total ?? 0}</dd>
                        </div>
                        <div className="flex gap-1.5">
                            <dt>Ready</dt>
                            <dd className="num text-ink">{stats?.ready_to_score_job_posts ?? 0}</dd>
                        </div>
                        <div className="flex gap-1.5">
                            <dt>Pending extract</dt>
                            <dd className="num text-ink">{pendingExtraction}</dd>
                        </div>
                        <div className="flex gap-1.5">
                            <dt>Pending embed</dt>
                            <dd className="num text-ink">{pendingEmbedding}</dd>
                        </div>
                    </dl>
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
                        onClick={() => setIsOpen((value) => !value)}
                        aria-expanded={isOpen}
                        className="inline-flex h-10 items-center justify-center gap-2 border border-accent px-4 text-[14px] font-medium text-accent transition-colors hover:bg-accent-soft"
                    >
                        <ListChecks className="h-4 w-4" aria-hidden="true" />
                        {isOpen ? 'Hide jobs' : 'Browse jobs'}
                    </button>
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
                                    <InventoryJobRow key={job.job_id} job={job} />
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
