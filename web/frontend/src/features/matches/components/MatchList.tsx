import React, { useEffect, useState } from 'react';
import { useMatches } from '@/hooks/useMatches';
import { usePolicy } from '@/hooks/usePolicy';
import { useStats } from '@/hooks/useStats';
import { MatchCard } from './MatchCard';
import { MatchFilters } from './MatchFilters';
import { Button } from '@/components/ui/Button';
import type { MatchStatus, PolicyConfig, RankingMode } from '@/types/api';

interface MatchListProps {
    onMatchSelect: (matchId: string) => void;
}

const DEFAULT_POLICY: PolicyConfig = {
    min_fit: 55,
    top_k: 50,
    min_jd_required_coverage: null,
};

function degradedSummary(reason: string): string {
    if (reason === 'remote_unavailable') return 'The remote cross-encoder is unreachable; results used a threshold fallback.';
    if (reason === 'local_unavailable') return 'The local cross-encoder is not available; results used a threshold fallback.';
    if (reason === 'provider_disabled') return 'Semantic scoring is off; results used a threshold fallback.';
    return `Fallback scoring is active (${reason}).`;
}

function llmRerankSummary(rerank: any): string | null {
    if (!rerank) return null;
    if (rerank.applied) {
        return `LLM-applied top ${rerank.window_size}`;
    }
    if (rerank.enabled && rerank.reason) {
        return `LLM not applied: ${String(rerank.reason).replace(/_/g, ' ')}`;
    }
    if (rerank.available === false && rerank.reason) {
        return `LLM unavailable: ${String(rerank.reason).replace(/_/g, ' ')}`;
    }
    return null;
}

function initialShowAllProcessed(): boolean {
    const params = new URLSearchParams(globalThis.location.search);
    return (
        params.get('tier') === 'all'
        || params.get('showExcluded') === 'true'
        || params.get('showAllProcessed') === 'true'
    );
}

export const MatchList: React.FC<MatchListProps> = ({ onMatchSelect }) => {
    const [status, setStatus] = useState<MatchStatus>('active');
    const [remoteOnly, setRemoteOnly] = useState(false);
    const [rankingMode, setRankingMode] = useState<RankingMode>('balanced');
    const [showAllProcessed, setShowAllProcessed] = useState(initialShowAllProcessed);
    const [showHidden, setShowHidden] = useState(() => {
        const saved = localStorage.getItem('jobscout_show_hidden');
        return saved === 'true';
    });
    const { policy } = usePolicy();

    useEffect(() => {
        localStorage.setItem('jobscout_show_hidden', showHidden.toString());
    }, [showHidden]);

    const effectivePolicy = policy ?? DEFAULT_POLICY;
    const { data, isLoading, error, refetch } = useMatches({
        status,
        min_fit: showAllProcessed ? undefined : effectivePolicy.min_fit,
        top_k: showAllProcessed ? undefined : effectivePolicy.top_k,
        remote_only: remoteOnly,
        show_hidden: showHidden,
        ranking_mode: rankingMode,
        tier: 'all',
    });
    const { data: stats } = useStats({
        min_fit: effectivePolicy.min_fit,
        top_k: effectivePolicy.top_k,
    });

    const matches = data?.matches ?? [];
    const degradedReason = matches.find((m) => m.scoring_degraded_reason)?.scoring_degraded_reason ?? null;
    const processedCount = stats?.total_scored ?? stats?.total_matches ?? 0;
    const hiddenByCurrentFilters = Math.max(processedCount - matches.length, 0);
    const processedToggleCount = showAllProcessed || hiddenByCurrentFilters > 0
        ? processedCount
        : 0;
    const llmOrdering = llmRerankSummary(data?.llm_rerank);

    const strongCount = matches.filter((m) => !m.is_hidden && (m.fit_score ?? 0) >= 80).length;

    if (isLoading) {
        return (
            <section aria-busy="true" aria-label="Loading matches" className="py-20">
                <div className="mx-auto max-w-md text-center">
                    <div className="mx-auto h-1.5 w-32 overflow-hidden bg-rule">
                        <div
                            className="h-full w-1/3 bg-accent"
                            style={{
                                animation: 'wm-marquee-indeterminate 1600ms var(--ease-out) infinite',
                            }}
                        />
                    </div>
                    <p className="caption mt-5">Fetching matches</p>
                </div>
            </section>
        );
    }

    if (error) {
        return (
            <section className="border border-rule bg-surface px-6 py-10 text-center">
                <p className="text-[15px] text-ink">Something went wrong loading your matches.</p>
                <p className="mt-2 text-[13px] text-ink-soft">
                    The server may be unreachable. Give it a moment, then try again.
                </p>
                <div className="mt-5 flex justify-center">
                    <Button variant="secondary" size="sm" onClick={() => refetch()}>
                        Try again
                    </Button>
                </div>
            </section>
        );
    }

    return (
        <section className="space-y-6">
            <MatchFilters
                status={status}
                onStatusChange={setStatus}
                remoteOnly={remoteOnly}
                onRemoteOnlyChange={setRemoteOnly}
                rankingMode={rankingMode}
                onRankingModeChange={setRankingMode}
                showHidden={showHidden}
                onShowHiddenChange={setShowHidden}
                showAllProcessed={showAllProcessed}
                onShowAllProcessedChange={setShowAllProcessed}
                processedCount={processedToggleCount}
            />

            {degradedReason && (
                <div className="border border-warn/40 bg-warn-soft px-4 py-3 text-[13px] text-ink">
                    <span className="caption mr-2 text-warn">Degraded</span>
                    {degradedSummary(degradedReason)}
                </div>
            )}

            <header className="flex items-baseline justify-between border-b border-rule pb-3">
                <div className="flex items-baseline gap-3">
                    <span className="num text-[22px] font-medium text-ink tabular-nums">
                        {matches.length}
                    </span>
                    <span className="text-[13px] text-ink-soft">
                        {showAllProcessed
                            ? (matches.length === 1 ? 'matched candidate' : 'matched candidates')
                            : (matches.length === 1 ? 'match' : 'matches')}
                        {strongCount > 0 && (
                            <>
                                <span className="mx-2 text-ink-faint">·</span>
                                <span className="text-accent">{strongCount} strong</span>
                            </>
                        )}
                    </span>
                </div>
                <span className="caption" aria-label={llmOrdering ? `Sorted by ${rankingMode}. ${llmOrdering}` : undefined}>
                    Sorted by {rankingMode}
                    {llmOrdering && (
                        <>
                            <span className="mx-2 text-ink-faint">·</span>
                            <span className="text-accent">{llmOrdering}</span>
                        </>
                    )}
                </span>
            </header>

            {matches.length === 0 ? (
                <EmptyState
                    hiddenByCurrentFilters={hiddenByCurrentFilters}
                    showAllProcessed={showAllProcessed}
                    onShowAllProcessed={() => setShowAllProcessed(true)}
                />
            ) : (
                <div className="stagger border-x border-t border-rule bg-canvas">
                    {matches.map((match, idx) => (
                        <MatchCard
                            key={match.match_id}
                            match={match}
                            onSelect={onMatchSelect}
                            featured={idx === 0 && (match.fit_score ?? 0) >= 80 && !match.is_hidden}
                        />
                    ))}
                </div>
            )}
        </section>
    );
};

function EmptyState({
    hiddenByCurrentFilters,
    showAllProcessed,
    onShowAllProcessed,
}: Readonly<{
    hiddenByCurrentFilters: number;
    showAllProcessed: boolean;
    onShowAllProcessed: () => void;
}>) {
    if (hiddenByCurrentFilters > 0 && !showAllProcessed) {
        return (
            <div className="border border-rule bg-surface px-8 py-12 text-center">
                <p className="caption text-ink-muted">Nothing above your threshold</p>
                <p className="mt-3 text-[16px] text-ink">
                    No matches qualified this run.
                </p>
                <p className="mx-auto mt-2 max-w-md text-[13px] text-ink-soft">
                    {hiddenByCurrentFilters} {hiddenByCurrentFilters === 1 ? 'candidate was' : 'candidates were'} matched but hidden by the current result policy.
                    Loosen the policy, or browse the full candidate set now.
                </p>
                <div className="mt-5">
                    <Button variant="secondary" size="sm" onClick={onShowAllProcessed}>
                        Show all matched candidates
                    </Button>
                </div>
            </div>
        );
    }

    return (
        <div className="border border-rule bg-surface px-8 py-12 text-center">
            <p className="caption text-ink-muted">Nothing to show yet</p>
            <p className="mt-3 text-[16px] text-ink">
                Upload a resume and run matching — your shortlist shows up here.
            </p>
            <p className="mx-auto mt-2 max-w-md text-[13px] text-ink-soft">
                Filters and preferences can always be relaxed if too little is coming through.
            </p>
        </div>
    );
}
