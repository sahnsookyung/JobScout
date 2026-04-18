import React from 'react';
import { CheckCircle2, CircleSlash, TriangleAlert } from 'lucide-react';

export interface StatusBannerProps {
    status: string;
    step?: string;
    matches_count?: number;
    saved_count?: number;
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

export const StatusBanner: React.FC<StatusBannerProps> = ({
    status,
    step,
    matches_count,
    saved_count,
    execution_time,
    error,
    stale_due_to_newer_upload,
    stale_message,
}) => {
    const isPending = status === 'pending';
    const isRunning = status === 'running';
    const isCancellationRequested = status === 'cancellation_requested';
    const isPersisting = status === 'persisting';
    const isCompleted = status === 'completed';
    const isFailed = status === 'failed';
    const isCancelled = status === 'cancelled';
    const isActive = isPending || isRunning || isCancellationRequested || isPersisting;

    const stepLabel = step ? (STEP_LABELS[step] ?? 'Processing') : 'Starting up';

    let statusLabel = 'Active';
    let statusTone = 'text-accent';
    if (isCompleted) { statusLabel = 'Complete'; statusTone = 'text-affirm'; }
    else if (isFailed) { statusLabel = 'Failed'; statusTone = 'text-warn'; }
    else if (isCancelled) { statusLabel = 'Stopped'; statusTone = 'text-ink-muted'; }
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
                {isCancellationRequested && (
                    <p className="mt-1 text-[13px] text-ink-soft">Stopping as soon as it is safe to.</p>
                )}
                {isPersisting && (
                    <p className="mt-1 text-[13px] text-ink-soft">Past the save boundary — finishing safely.</p>
                )}
                {isCompleted && (
                    <div className="mt-1 flex flex-wrap gap-x-5 gap-y-1 text-[13px] text-ink-soft">
                        <span>
                            Found <span className="num text-ink tabular-nums">{matches_count ?? 0}</span>
                        </span>
                        <span>
                            Saved <span className="num text-ink tabular-nums">{saved_count ?? 0}</span>
                        </span>
                        <span>
                            <span className="num text-ink tabular-nums">{(execution_time ?? 0).toFixed(1)}s</span>
                        </span>
                    </div>
                )}
                {isCompleted && stale_due_to_newer_upload && stale_message && (
                    <p className="mt-3 border border-warn/40 bg-warn-soft px-3 py-2 text-[13px] text-ink">
                        {stale_message}
                    </p>
                )}
                {isFailed && (
                    <div>
                        <p className="mt-1 text-[13px] text-ink">This run didn't finish.</p>
                        {error && (
                            <p className="mt-2 border border-warn/40 bg-warn-soft px-3 py-2 text-[13px] text-ink-soft">
                                {error}
                            </p>
                        )}
                    </div>
                )}
                {isCancelled && (
                    <p className="mt-1 text-[13px] text-ink-soft">
                        You can start another run whenever you're ready.
                    </p>
                )}
            </div>
        </div>
    );
};
