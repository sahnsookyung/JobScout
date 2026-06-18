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
        <div className="grid grid-cols-1 gap-8 sm:grid-cols-3 sm:divide-x sm:divide-rule">
            <div className="flex flex-col justify-between">
                <p className="caption">Total</p>
                <div className="mt-2 flex items-baseline gap-2">
                    <span className="display-numeral text-[56px] sm:text-[64px]">
                        {totalMatches}
                    </span>
                    <span className="text-[13px] text-ink-muted">
                        matched candidates
                    </span>
                </div>
                <dl className="mt-4 grid grid-cols-2 gap-x-5 gap-y-2 border-t border-rule pt-3 text-[12px]">
                    <InventoryItem label="Imported" value={jobPostTotal} />
                    <InventoryItem label="Embedded" value={embeddedJobs} />
                    <InventoryItem label="Extracted" value={extractedJobs} />
                    <InventoryItem label="Ready" value={readyToScoreJobs} />
                    <InventoryItem label="Pending extract" value={pendingExtraction + retryableExtraction} />
                    <InventoryItem label="Pending embed" value={pendingEmbedding} />
                </dl>
            </div>

            <div className="flex items-center gap-5 sm:pl-8">
                <SegmentedCircle
                    activeMatches={activeMatches}
                    activeArc={activeArc}
                    cappedArc={cappedArc}
                    hiddenArc={hiddenArc}
                    belowArc={belowArc}
                    circumference={circumference}
                    radius={radius}
                />
                <dl className="space-y-1.5 text-[13px]">
                    <Legend swatch="accent" label="Fit" value={activeMatches} />
                    {beyondTopK > 0 && (
                        <Legend swatch="ink-soft" label="Above max" value={beyondTopK} />
                    )}
                    <Legend swatch="ink-muted" label="Below threshold" value={belowThreshold} />
                    <Legend swatch="ink-faint" label="Hidden" value={hiddenMatches} />
                </dl>
            </div>

            <div className="flex flex-col sm:pl-8">
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
        <div className="flex items-baseline justify-between gap-3">
            <dt className="caption text-[10px] text-ink-muted">{label}</dt>
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
        <div className="flex items-center gap-2.5">
            <span className={`h-2 w-2 rounded-full ${swatchBg}`} aria-hidden="true" />
            <span className="num tabular-nums text-ink">{value}</span>
            <span className="text-ink-muted">{label}</span>
        </div>
    );
}
