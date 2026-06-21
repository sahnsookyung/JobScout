import React from 'react';
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
        extracted_job_posts?: number;
        embedded_job_posts?: number;
        ready_to_score_job_posts?: number;
        pending_extraction_job_posts?: number;
        pending_embedding_job_posts?: number;
        retryable_extraction_job_posts?: number;
        retryable_embedding_job_posts?: number;
    } | null | undefined;
}

export const StatsPanel: React.FC<StatsPanelProps> = ({ stats }) => {
    const totalMatches = stats?.total_matches ?? 0;
    const activeMatches = stats?.active_matches ?? 0;
    const hiddenMatches = stats?.hidden_count ?? 0;
    const belowThreshold = stats?.below_threshold_count ?? 0;
    const beyondTopK = stats?.beyond_top_k_count ?? 0;
    const jobPostTotal = stats?.job_post_total ?? totalMatches;
    const extractedJobs = stats?.extracted_job_posts ?? 0;
    const embeddedJobs = stats?.embedded_job_posts ?? 0;
    const readyToScoreJobs = stats?.ready_to_score_job_posts ?? 0;
    const pendingExtraction = stats?.pending_extraction_job_posts ?? 0;
    const pendingEmbedding = stats?.pending_embedding_job_posts ?? 0;
    const retryableExtraction = stats?.retryable_extraction_job_posts ?? 0;
    const retryableEmbedding = stats?.retryable_embedding_job_posts ?? 0;
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
                <dl className="mt-4 grid grid-cols-1 gap-x-5 gap-y-2 border-t border-rule pt-3 text-[12px] min-[520px]:grid-cols-2">
                    <InventoryItem label="Imported" value={jobPostTotal} />
                    <InventoryItem label="Embedded" value={embeddedJobs} />
                    <InventoryItem label="Extracted" value={extractedJobs} />
                    <InventoryItem label="Ready" value={readyToScoreJobs} />
                    <InventoryItem label="Pending extract" value={pendingExtraction + retryableExtraction} />
                    <InventoryItem label="Pending embed" value={pendingEmbedding + retryableEmbedding} />
                </dl>
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

            <div className="min-w-0 border-t border-rule pt-6 lg:border-t-0 lg:pt-0">
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
            <dt className="caption min-w-0 text-[10px] text-ink-muted">{label}</dt>
            <dd className="num text-[13px] text-ink tabular-nums">{value}</dd>
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
        <div className="grid grid-cols-[0.5rem_auto_minmax(0,1fr)] items-center gap-2.5">
            <span className={`h-2 w-2 rounded-full ${swatchBg}`} aria-hidden="true" />
            <span className="num tabular-nums text-ink">{value}</span>
            <span className="text-ink-muted">{label}</span>
        </div>
    );
}
