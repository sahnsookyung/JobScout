import React from 'react';
import {
    Bell,
    CheckCircle2,
    CircleSlash,
    Database,
    FileCheck2,
    FileText,
    SearchCheck,
    Sparkles,
    TriangleAlert,
} from 'lucide-react';
import type { PipelineStats, ProcessingFailure, ProcessingProgress, ProcessingWarning } from '@/types/api';

export interface StatusBannerProps {
    status: string;
    step?: string;
    phase?: string | null;
    progress?: ProcessingProgress | null;
    stats?: PipelineStats;
    warnings?: ProcessingWarning[];
    failure?: ProcessingFailure | null;
    matches_count?: number;
    saved_count?: number;
    notified_count?: number;
    execution_time?: number;
    error?: string;
    stale_due_to_newer_upload?: boolean;
    stale_message?: string;
}

const STEP_LABELS: Record<string, string> = {
    loading_resume: 'Loading resume',
    vector_matching: 'Finding candidates',
    scoring: 'Scoring',
    saving_results: 'Saving',
    notifying: 'Notifying',
    initializing: 'Starting up',
    matching: 'Finding candidates',
    extracting: 'Parsing resume',
    embedding: 'Embedding resume',
};

const TIMELINE_STEPS = [
    { key: 'resume_uploaded', label: 'Resume uploaded', icon: FileText },
    { key: 'resume_parsed', label: 'Resume parsed', icon: FileCheck2 },
    { key: 'resume_embedded', label: 'Resume embedded', icon: Sparkles },
    { key: 'jobs_prepared', label: 'Jobs prepared', icon: Database },
    { key: 'matches_scored', label: 'Matches scored', icon: SearchCheck },
    { key: 'results_saved', label: 'Results saved', icon: CheckCircle2 },
    { key: 'notifications_checked', label: 'Notifications checked', icon: Bell },
];

const PHASE_INDEX: Record<string, number> = {
    loading_resume: 0,
    extracting_resume: 1,
    embedding_resume: 2,
    matching_jobs: 3,
    scoring: 4,
    saving: 5,
    notifying: 6,
    completed: 7,
    failed: 7,
    cancelled: 7,
};

function numStat(stats: PipelineStats | undefined, key: string): number {
    const value = stats?.[key];
    return typeof value === 'number' && Number.isFinite(value) ? value : 0;
}

function emptyStateMessage(stats: PipelineStats | undefined, savedCount: number, status: string): string | null {
    if (status !== 'completed' || savedCount > 0) return null;

    const jobsSeen = numStat(stats, 'jobs_seen');
    const ready = numStat(stats, 'jobs_ready_to_score');
    const pending = numStat(stats, 'jobs_pending_extraction') + numStat(stats, 'jobs_pending_embedding');
    const pendingMatching = numStat(stats, 'jobs_pending_matching');
    const candidates = numStat(stats, 'candidates_considered');

    if (jobsSeen === 0) return 'No jobs are currently available from enabled sources.';
    if (ready === 0 && pending > 0) return 'Jobs are still being prepared for matching.';
    if (pendingMatching > 0) return 'Matching is continuing in smaller pages.';
    if (candidates > 0) return 'Matches were found, but none passed the current save threshold.';
    return 'No matching candidates were available for this resume.';
}

export const StatusBanner: React.FC<StatusBannerProps> = ({
    status,
    step,
    phase,
    progress,
    stats,
    warnings = [],
    failure,
    matches_count,
    saved_count,
    notified_count,
    execution_time,
    error,
    stale_due_to_newer_upload,
    stale_message,
}) => {
    const isPending = status === 'pending';
    const isRunning = status === 'running';
    const isCancellationRequested = status === 'cancellation_requested';
    const isPersisting = status === 'persisting';
    const isResumeProcessing = status === 'processing';
    const isCompleted = status === 'completed';
    const isFailed = status === 'failed';
    const isCancelled = status === 'cancelled';
    const isActive = isPending || isRunning || isResumeProcessing || isCancellationRequested || isPersisting;

    const stepLabel = step ? (STEP_LABELS[step] ?? 'Processing') : 'Starting up';
    const currentPhase = phase ?? (isCompleted ? 'completed' : step ?? 'initializing');
    const timelineIndex = PHASE_INDEX[currentPhase] ?? (isActive ? 1 : 0);
    const saved = saved_count ?? numStat(stats, 'matches_saved');
    const selected = matches_count ?? numStat(stats, 'matches_selected');
    const notified = notified_count ?? numStat(stats, 'notifications_sent');
    const emptyMessage = emptyStateMessage(stats, saved, status);
    const warningCount = warnings.length;
    const pendingMatching = numStat(stats, 'jobs_pending_matching');
    const processedFresh = numStat(stats, 'jobs_matching_processed_fresh');
    const pageSize = numStat(stats, 'matching_page_size');
    const pagesCompleted = numStat(stats, 'matching_pages_completed');

    let statusLabel = 'Active';
    let statusTone = 'text-accent';
    if (isCompleted) { statusLabel = 'Complete'; statusTone = 'text-affirm'; }
    else if (isFailed) { statusLabel = 'Failed'; statusTone = 'text-warn'; }
    else if (isCancelled) { statusLabel = 'Stopped'; statusTone = 'text-ink-muted'; }
    else if (isResumeProcessing) { statusLabel = 'Preparing'; statusTone = 'text-accent'; }
    else if (isCancellationRequested) { statusLabel = 'Stopping'; statusTone = 'text-ink-soft'; }
    else if (isPersisting) { statusLabel = 'Finishing'; statusTone = 'text-ink-soft'; }

    let Icon: typeof CheckCircle2 = CheckCircle2;
    if (isFailed) Icon = TriangleAlert;
    else if (isCancelled) Icon = CircleSlash;

    return (
        <div className="mt-6 flex items-start gap-4 border-t border-rule pt-6">
            {isActive ? (
                <span className="relative mt-0.5 flex h-3 w-3 flex-shrink-0">
                    <span className="ember absolute inset-0 rounded-full bg-accent opacity-40" aria-hidden="true" />
                    <span className="relative m-auto h-1.5 w-1.5 rounded-full bg-accent" />
                </span>
            ) : (
                <Icon className={`mt-0.5 h-4 w-4 flex-shrink-0 ${statusTone}`} aria-hidden="true" />
            )}
            <div className="flex-1">
                <div className="flex items-baseline gap-3">
                    <span className={`caption ${statusTone}`}>{statusLabel}</span>
                    {isActive && <span className="text-[13px] text-ink">{stepLabel}</span>}
                </div>

                {isPending && <p className="mt-1 text-[13px] text-ink-soft">Starting your match run.</p>}
                {isRunning && <p className="mt-1 text-[13px] text-ink-soft">Working through your feed.</p>}
                {status === 'processing' && (
                    <p className="mt-1 text-[13px] text-ink-soft">Preparing your resume for matching.</p>
                )}
                {isCancellationRequested && (
                    <p className="mt-1 text-[13px] text-ink-soft">Stopping as soon as it is safe to.</p>
                )}
                {isPersisting && (
                    <p className="mt-1 text-[13px] text-ink-soft">Past the save boundary — finishing safely.</p>
                )}

                <div className="mt-4 grid gap-2 sm:grid-cols-7">
                    {TIMELINE_STEPS.map((item, index) => {
                        const StepIcon = item.icon;
                        const isDone = isCompleted || index < timelineIndex;
                        const isCurrent = !isCompleted && !isFailed && !isCancelled && index === timelineIndex;
                        return (
                            <div
                                key={item.key}
                                className={[
                                    'min-h-[72px] border px-3 py-2',
                                    isDone ? 'border-accent/50 bg-accent-soft' : 'border-rule bg-surface',
                                    isCurrent ? 'ring-1 ring-accent' : '',
                                ].join(' ')}
                            >
                                <StepIcon
                                    className={[
                                        'mb-2 h-4 w-4',
                                        isDone || isCurrent ? 'text-accent' : 'text-ink-muted',
                                    ].join(' ')}
                                    aria-hidden="true"
                                />
                                <p className="text-[12px] leading-snug text-ink">{item.label}</p>
                            </div>
                        );
                    })}
                </div>

                {isCompleted && (
                    <div className="mt-1 flex flex-wrap gap-x-5 gap-y-1 text-[13px] text-ink-soft">
                        <span>
                            Found <span className="num text-ink tabular-nums">{selected}</span>
                        </span>
                        <span>
                            Saved <span className="num text-ink tabular-nums">{saved}</span>
                        </span>
                        <span>
                            Notified <span className="num text-ink tabular-nums">{notified}</span>
                        </span>
                        <span>
                            <span className="num text-ink tabular-nums">{(execution_time ?? 0).toFixed(1)}s</span>
                        </span>
                    </div>
                )}
                {progress && (
                    <div className="mt-4 h-1.5 overflow-hidden bg-rule" aria-label={`Progress ${progress.percent}%`}>
                        <div className="h-full bg-accent" style={{ width: `${progress.percent}%` }} />
                    </div>
                )}
                {emptyMessage && (
                    <p className="mt-3 border border-rule bg-surface px-3 py-2 text-[13px] text-ink">
                        {emptyMessage}
                    </p>
                )}
                {isCompleted && stale_due_to_newer_upload && stale_message && (
                    <p className="mt-3 border border-warn/40 bg-warn-soft px-3 py-2 text-[13px] text-ink">
                        {stale_message}
                    </p>
                )}
                {warningCount > 0 && (
                    <div className="mt-3 space-y-2">
                        {warnings.map((warning) => (
                            <p
                                key={warning.code}
                                className="border border-rule bg-surface px-3 py-2 text-[13px] text-ink-soft"
                            >
                                {warning.message}
                            </p>
                        ))}
                    </div>
                )}
                {isFailed && (
                    <div>
                        <p className="mt-1 text-[13px] text-ink">This run didn't finish.</p>
                        {(failure?.user_message || error) && (
                            <p className="mt-2 border border-warn/40 bg-warn-soft px-3 py-2 text-[13px] text-ink-soft">
                                {failure?.user_message ?? error}
                            </p>
                        )}
                    </div>
                )}
                {isCancelled && (
                    <p className="mt-1 text-[13px] text-ink-soft">
                        You can start another run whenever you're ready.
                    </p>
                )}

                <details className="mt-4 border border-rule bg-surface px-3 py-2">
                    <summary className="cursor-pointer text-[13px] text-ink">Run details</summary>
                    <dl className="mt-3 grid gap-2 text-[12px] text-ink-soft sm:grid-cols-2 lg:grid-cols-4">
                        <div>
                            <dt className="caption">Phase</dt>
                            <dd className="text-ink">{currentPhase}</dd>
                        </div>
                        <div>
                            <dt className="caption">Jobs ready</dt>
                            <dd className="num text-ink">{numStat(stats, 'jobs_ready_to_score')}</dd>
                        </div>
                        <div>
                            <dt className="caption">Jobs preparing</dt>
                            <dd className="num text-ink">
                                {numStat(stats, 'jobs_pending_extraction') + numStat(stats, 'jobs_pending_embedding')}
                            </dd>
                        </div>
                        <div>
                            <dt className="caption">Pending match</dt>
                            <dd className="num text-ink">{pendingMatching}</dd>
                        </div>
                        <div>
                            <dt className="caption">Fresh cached</dt>
                            <dd className="num text-ink">{processedFresh}</dd>
                        </div>
                        <div>
                            <dt className="caption">Match page</dt>
                            <dd className="num text-ink">
                                {pagesCompleted}
                                {pageSize > 0 ? ` x ${pageSize}` : ''}
                            </dd>
                        </div>
                        <div>
                            <dt className="caption">Notifications</dt>
                            <dd className="num text-ink">{notified}</dd>
                        </div>
                    </dl>
                    {failure && (
                        <p className="mt-3 text-[12px] text-ink-soft">
                            Next action: <span className="text-ink">{failure.next_action ?? 'retry'}</span>
                        </p>
                    )}
                </details>
            </div>
        </div>
    );
};
