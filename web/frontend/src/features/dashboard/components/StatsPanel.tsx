import React from 'react';
import { ChevronDown } from 'lucide-react';
import { SegmentedCircle } from './SegmentedCircle';
import { CompactScoreBar } from './CompactScoreBar';

export interface StatsPanelProps {
    stats: {
        total_matches?: number;
        active_matches?: number;
        hidden_count?: number;
        below_threshold_count?: number;
        beyond_top_k_count?: number;
        policy_top_k?: number | null;
        score_distribution?: {
            excellent?: number;
            good?: number;
            average?: number;
            poor?: number;
        };
        job_post_total?: number;
        active_job_posts?: number;
        inactive_job_posts?: number;
        expired_job_posts?: number;
        extracted_job_posts?: number;
        embedded_job_posts?: number;
        ready_to_score_job_posts?: number;
        active_ready_to_score_job_posts?: number;
        pending_extraction_job_posts?: number;
        pending_embedding_job_posts?: number;
        retryable_extraction_job_posts?: number;
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
        active_recoverable_missing_description_job_posts?: number;
        description_recovery_posting_not_found_job_posts?: number;
        description_recovery_adapter_missing_job_posts?: number;
        description_recovery_prohibited_job_posts?: number;
        description_recovery_unmapped_job_posts?: number;
        description_recovery_unavailable_job_posts?: number;
    } | null | undefined;
}

export const StatsPanel: React.FC<StatsPanelProps> = ({ stats }) => {
    const backlogDetailsId = React.useId();
    const [showBacklogDetails, setShowBacklogDetails] = React.useState(false);
    const totalMatches = stats?.total_matches ?? 0;
    const activeMatches = stats?.active_matches ?? 0;
    const hiddenMatches = stats?.hidden_count ?? 0;
    const belowThreshold = stats?.below_threshold_count ?? 0;
    const beyondTopK = stats?.beyond_top_k_count ?? 0;
    const jobPostTotal = stats?.job_post_total ?? totalMatches;
    const activeJobs = stats?.active_job_posts ?? 0;
    const inactiveJobs = stats?.inactive_job_posts ?? 0;
    const expiredJobs = stats?.expired_job_posts ?? 0;
    const extractedJobs = stats?.extracted_job_posts ?? 0;
    const embeddedJobs = stats?.embedded_job_posts ?? 0;
    const readyToScoreJobs = stats?.ready_to_score_job_posts ?? 0;
    const activeReadyToScore = stats?.active_ready_to_score_job_posts ?? readyToScoreJobs;
    const pendingExtraction = stats?.pending_extraction_job_posts ?? 0;
    const pendingEmbedding = stats?.pending_embedding_job_posts ?? 0;
    const retryableExtraction = stats?.retryable_extraction_job_posts ?? 0;
    const retryableEmbedding = stats?.retryable_embedding_job_posts ?? 0;
    const hasExtractionBreakdown = stats?.active_pending_extraction_job_posts != null
        || stats?.active_retryable_extraction_job_posts != null
        || stats?.inactive_pending_extraction_job_posts != null;
    const hasEmbeddingBreakdown = stats?.active_pending_embedding_job_posts != null
        || stats?.active_retryable_embedding_job_posts != null
        || stats?.inactive_pending_embedding_job_posts != null;
    const activePendingExtraction = stats?.active_ready_for_extraction_job_posts ?? (hasExtractionBreakdown
        ? (stats?.active_pending_extraction_job_posts ?? 0) + (stats?.active_retryable_extraction_job_posts ?? 0)
        : pendingExtraction + retryableExtraction);
    const inactivePendingExtraction = hasExtractionBreakdown ? stats?.inactive_pending_extraction_job_posts ?? 0 : 0;
    const activePendingEmbedding = hasEmbeddingBreakdown
        ? (stats?.active_pending_embedding_job_posts ?? 0) + (stats?.active_retryable_embedding_job_posts ?? 0)
        : pendingEmbedding + retryableEmbedding;
    const inactivePendingEmbedding = hasEmbeddingBreakdown ? stats?.inactive_pending_embedding_job_posts ?? 0 : 0;
    const missingDescriptions = stats?.missing_description_job_posts ?? 0;
    const activeMissingDescriptions = stats?.active_missing_description_job_posts ?? 0;
    const inactiveMissingDescriptions = stats?.inactive_missing_description_job_posts ?? 0;
    const recoveryQueued = stats?.description_recovery_queued_job_posts ?? 0;
    const recoveryRetryable = stats?.description_recovery_retryable_job_posts ?? 0;
    const recoveryUnavailable = stats?.description_recovery_unavailable_job_posts ?? 0;
    const notActiveJobs = inactiveJobs + expiredJobs;
    const scoreDist = stats?.score_distribution;
    const radius = 36;
    const circumference = 2 * Math.PI * radius;
    const chartTotal = Math.max(
        totalMatches,
        activeMatches + hiddenMatches + belowThreshold + beyondTopK,
    );
    const arcFor = (value: number) => (
        chartTotal > 0 ? (Math.max(value, 0) / chartTotal) * circumference : 0
    );
    const activeArc = arcFor(activeMatches);
    const cappedArc = arcFor(beyondTopK);
    const hiddenArc = arcFor(hiddenMatches);
    const belowArc = arcFor(belowThreshold);
    const backlogDetailTotal = (
        activePendingExtraction
        + activePendingEmbedding
        + inactivePendingExtraction
        + inactivePendingEmbedding
        + activeMissingDescriptions
        + inactiveMissingDescriptions
        + recoveryQueued
        + recoveryRetryable
        + recoveryUnavailable
    );
    const hasBacklogDetails = backlogDetailTotal > 0;
    const showOperationalDetails = hasBacklogDetails && showBacklogDetails;

    return (
        <div className="grid grid-cols-1 gap-7 lg:grid-cols-[minmax(15rem,1.05fr)_minmax(14rem,0.8fr)_minmax(16rem,1fr)] lg:items-stretch lg:gap-x-10">
            <div className="min-w-0 flex flex-col justify-between">
                <p className="caption">Total</p>
                <div className="mt-2 flex flex-wrap items-end gap-x-3 gap-y-1">
                    <span className="display-numeral text-[56px] leading-none sm:text-[64px]">
                        {totalMatches}
                    </span>
                    <span className="max-w-[8.5rem] pb-1 text-[13px] leading-snug text-ink-muted">
                        matched candidates
                    </span>
                </div>
                <div className="relative mt-4 border-t border-rule pt-3">
                    {hasBacklogDetails && (
                        <button
                            type="button"
                            aria-controls={backlogDetailsId}
                            aria-expanded={showOperationalDetails}
                            aria-label={showOperationalDetails ? 'Show inventory metrics' : 'Show backlog details'}
                            title={showOperationalDetails ? 'Show inventory metrics' : 'Show backlog details'}
                            onClick={() => setShowBacklogDetails((isVisible) => !isVisible)}
                            className="absolute right-0 top-2 inline-flex h-6 w-6 items-center justify-center border border-rule bg-surface text-ink-muted transition hover:border-accent hover:text-ink focus:outline-none focus-visible:ring-2 focus-visible:ring-accent"
                        >
                            <ChevronDown
                                className={`h-3.5 w-3.5 transition-transform ${showOperationalDetails ? 'rotate-180' : ''}`}
                                aria-hidden="true"
                            />
                        </button>
                    )}
                    {showOperationalDetails ? (
                        <dl
                            id={backlogDetailsId}
                            className="grid gap-2 pr-8 text-[12px] leading-5 text-ink-muted"
                        >
                            <StatusLine
                                label="Active backlog"
                                value={activePendingExtraction + activePendingEmbedding}
                                detail="eligible for queued processing"
                            />
                            <StatusLine
                                label="Inactive backlog"
                                value={inactivePendingExtraction + inactivePendingEmbedding}
                                detail="not used for matching"
                            />
                            <StatusLine
                                label="Missing descriptions"
                                value={activeMissingDescriptions + inactiveMissingDescriptions}
                                detail="refresh from ATS or retire"
                            />
                            <StatusLine
                                label="Checking ATS"
                                value={recoveryQueued + recoveryRetryable}
                                detail="background description recovery"
                            />
                            <StatusLine
                                label="Needs source setup"
                                value={recoveryUnavailable}
                                detail="unmapped or unsupported sources"
                            />
                        </dl>
                    ) : (
                        <dl
                            id={backlogDetailsId}
                            className="grid grid-cols-1 gap-x-5 gap-y-2 pr-8 text-[12px] min-[520px]:grid-cols-2"
                        >
                            <InventoryItem label="Imported" value={jobPostTotal} />
                            <InventoryItem label="Active" value={activeJobs} />
                            <InventoryItem label="Ready active" value={activeReadyToScore} />
                            <InventoryItem label="Not active" value={notActiveJobs} />
                            <InventoryItem label="Ready extract" value={stats?.ready_for_extraction_job_posts ?? pendingExtraction + retryableExtraction} />
                            <InventoryItem label="Missing desc" value={missingDescriptions} />
                            <InventoryItem label="Extracted all" value={extractedJobs} />
                            <InventoryItem label="Embedded all" value={embeddedJobs} />
                        </dl>
                    )}
                </div>
            </div>

            <div className="min-w-0 border-t border-rule pt-6 md:flex md:items-center md:gap-5 lg:border-t-0 lg:pt-0">
                <SegmentedCircle
                    activeMatches={activeMatches}
                    activeArc={activeArc}
                    cappedArc={cappedArc}
                    hiddenArc={hiddenArc}
                    belowArc={belowArc}
                    circumference={circumference}
                    radius={radius}
                />
                <dl className="mt-4 space-y-1.5 text-[13px] md:mt-0">
                    <Legend swatch="accent" label="Fit" value={activeMatches} />
                    {beyondTopK > 0 && (
                        <Legend swatch="ink-soft" label="Above max" value={beyondTopK} />
                    )}
                    <Legend swatch="ink-muted" label="Below threshold" value={belowThreshold} />
                    <Legend swatch="ink-faint" label="Hidden" value={hiddenMatches} />
                </dl>
            </div>

            <div className="min-w-0 border-t border-rule pt-6 lg:border-t-0 lg:pt-0 lg:flex lg:min-h-0 lg:flex-col lg:justify-center">
                <p className="caption">Score distribution</p>
                <div className="mt-3 space-y-2.5">
                    <CompactScoreBar label="Strong" range="80+" value={scoreDist?.excellent ?? 0} total={totalMatches} tone="accent" />
                    <CompactScoreBar label="Good" range="60–79" value={scoreDist?.good ?? 0} total={totalMatches} tone="ink-soft" />
                    <CompactScoreBar label="Fair" range="40–59" value={scoreDist?.average ?? 0} total={totalMatches} tone="ink-muted" />
                    <CompactScoreBar label="Low" range="<40" value={scoreDist?.poor ?? 0} total={totalMatches} tone="ink-faint" />
                </div>
            </div>
        </div>
    );
};

function InventoryItem({ label, value }: Readonly<{ label: string; value: number }>) {
    return (
        <div className="grid grid-cols-[minmax(0,1fr)_auto] items-baseline gap-3">
            <dt className="caption min-w-0 break-words text-[10px] text-ink-muted">{label}</dt>
            <dd className="num text-[13px] text-ink tabular-nums">{value}</dd>
        </div>
    );
}

function StatusLine({
    label,
    value,
    detail,
}: Readonly<{ label: string; value: number; detail: string }>) {
    return (
        <div className="grid grid-cols-[minmax(0,1fr)_auto] gap-3">
            <dt className="min-w-0">
                <span className="font-medium text-ink-soft">{label}</span>
                <span className="ml-1 break-words text-ink-muted">{detail}</span>
            </dt>
            <dd className="num text-ink tabular-nums">{value}</dd>
        </div>
    );
}

function Legend({ swatch, label, value }: Readonly<{ swatch: 'accent' | 'ink-soft' | 'ink-muted' | 'ink-faint'; label: string; value: number }>) {
    const swatchBg = {
        accent: 'bg-accent',
        'ink-soft': 'bg-ink-soft',
        'ink-muted': 'bg-ink-muted',
        'ink-faint': 'bg-ink-faint',
    }[swatch];

    return (
        <div className="grid grid-cols-[0.5rem_minmax(0,1fr)_auto] items-center gap-2.5">
            <span className={`h-2 w-2 rounded-full ${swatchBg}`} aria-hidden="true" />
            <dt className="text-ink-muted">{label}</dt>
            <dd className="num tabular-nums text-ink">{value}</dd>
        </div>
    );
}
