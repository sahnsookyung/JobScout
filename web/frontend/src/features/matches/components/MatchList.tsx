import React, { useEffect, useState } from 'react';
import { useMatches } from '@/hooks/useMatches';
import { usePolicy } from '@/hooks/usePolicy';
import { useStats } from '@/hooks/useStats';
import { MatchCard } from './MatchCard';
import { MatchFilters } from './MatchFilters';
import { Button } from '@/components/ui/Button';
import type { MatchStatus, MatchSummary, PolicyConfig, RankingMode } from '@/types/api';

const ALL_CANDIDATES_PAGE_SIZE = 100;

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

function primaryPageSize(policy: PolicyConfig): number {
    const parsed = Number(policy.top_k ?? DEFAULT_POLICY.top_k);
    if (!Number.isFinite(parsed)) return DEFAULT_POLICY.top_k;
    return Math.max(1, Math.min(Math.floor(parsed), 500));
}

export const MatchList: React.FC<MatchListProps> = ({ onMatchSelect }) => {
    const [status, setStatus] = useState<MatchStatus>('active');
    const [remoteOnly, setRemoteOnly] = useState(false);
    const [rankingMode, setRankingMode] = useState<RankingMode>('balanced');
    const [showAllProcessed, setShowAllProcessed] = useState(false);
    const [allCandidatesCursor, setAllCandidatesCursor] = useState<string | null>(null);
    const [allCandidates, setAllCandidates] = useState<MatchSummary[]>([]);
    const [showHidden, setShowHidden] = useState(() => {
        const saved = localStorage.getItem('jobscout_show_hidden');
        return saved === 'true';
    });
    const [llmOrderingEnabled, setLlmOrderingEnabled] = useState(() => {
        const saved = localStorage.getItem('jobscout_llm_ordering');
        return saved !== 'false';
    });
    const { policy } = usePolicy();

    useEffect(() => {
        localStorage.setItem('jobscout_show_hidden', showHidden.toString());
    }, [showHidden]);
    useEffect(() => {
        localStorage.setItem('jobscout_llm_ordering', llmOrderingEnabled.toString());
    }, [llmOrderingEnabled]);

    const effectivePolicy = policy ?? DEFAULT_POLICY;
    useEffect(() => {
        setAllCandidatesCursor(null);
        setAllCandidates([]);
    }, [status, remoteOnly, rankingMode, showHidden, showAllProcessed, llmOrderingEnabled]);

    const llmTopN = Number(policy?.llm_judge_top_n ?? 0);
    const primaryLimit = Math.max(primaryPageSize(effectivePolicy), Number.isFinite(llmTopN) ? llmTopN : 0);
    const { data, isLoading, isFetching, error, refetch } = useMatches({
        status,
        min_fit: showAllProcessed ? undefined : effectivePolicy.min_fit,
        top_k: showAllProcessed ? undefined : effectivePolicy.top_k,
        remote_only: remoteOnly,
        show_hidden: showHidden,
        ranking_mode: rankingMode,
        tier: showAllProcessed ? 'all' : 'primary',
        limit: showAllProcessed ? ALL_CANDIDATES_PAGE_SIZE : primaryLimit,
        cursor: showAllProcessed ? allCandidatesCursor : null,
        page_mode: 'cursor',
        view: 'compact',
        include: 'llm',
        llm_ordering: llmOrderingEnabled,
    });
    const { data: stats } = useStats({
        min_fit: effectivePolicy.min_fit,
        top_k: effectivePolicy.top_k,
    });

    useEffect(() => {
        if (!showAllProcessed || !data?.matches) return;
        setAllCandidates((previous) => {
            if (!allCandidatesCursor) {
                const samePage = (
                    previous.length === data.matches.length
                    && previous.every((match, index) => match.match_id === data.matches[index]?.match_id)
                );
                return samePage ? previous : data.matches;
            }
            const seen = new Set(previous.map((match) => match.match_id));
            const next = data.matches.filter((match) => !seen.has(match.match_id));
            if (next.length === 0) return previous;
            return [...previous, ...next];
        });
    }, [allCandidatesCursor, data?.matches, showAllProcessed]);

    const matches = showAllProcessed ? allCandidates : (data?.matches ?? []);
    const degradedReason = matches.find((m) => m.scoring_degraded_reason)?.scoring_degraded_reason ?? null;
    const processedCount = stats?.total_scored ?? stats?.total_matches ?? 0;
    const hiddenByCurrentFilters = Math.max(processedCount - matches.length, 0);
    const processedToggleCount = showAllProcessed || processedCount > matches.length
        ? processedCount
        : 0;
    const totalAvailable = data?.total ?? processedCount;
    const hasMoreAllCandidates = showAllProcessed && (
        data?.has_more === true || Boolean(data?.next_cursor)
    );
    const llmOrderingSummary = llmRerankSummary(data?.llm_rerank);

    const strongCount = matches.filter((m) => !m.is_hidden && (m.fit_score ?? 0) >= 80).length;
    const initialLoading = isLoading && (!showAllProcessed || matches.length === 0);

    if (initialLoading) {
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
                llmOrdering={llmOrderingEnabled}
                onLlmOrderingChange={setLlmOrderingEnabled}
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

            <header className="flex flex-col gap-2 border-b border-rule pb-3 sm:flex-row sm:items-baseline sm:justify-between">
                <div className="flex items-baseline gap-3">
                    <span className="num text-[22px] font-medium text-ink tabular-nums">
                        {matches.length}
                    </span>
                    <span className="text-[13px] text-ink-soft">
                        {showAllProcessed && totalAvailable > matches.length
                            ? `${matches.length} of ${totalAvailable} matched candidates`
                            : showAllProcessed
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
                <span
                    className="caption max-w-full text-left sm:max-w-[28rem] sm:text-right"
                    aria-label={llmOrderingSummary ? `Sorted by ${rankingMode}. ${llmOrderingSummary}` : undefined}
                >
                    Sorted by {rankingMode}
                    <span className="mx-2 text-ink-faint">·</span>
                    <span className={llmOrderingEnabled ? 'text-accent' : 'text-ink-muted'}>
                        {llmOrderingEnabled ? 'LLM order' : 'base order'}
                    </span>
                    {llmOrderingSummary && (
                        <>
                            <span className="mx-2 text-ink-faint">·</span>
                            <span className="text-accent">{llmOrderingSummary}</span>
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

            {hasMoreAllCandidates ? (
                <div className="flex justify-center">
                    <Button
                        variant="secondary"
                        size="sm"
                        disabled={isFetching || !data?.next_cursor}
                        onClick={() => setAllCandidatesCursor(data?.next_cursor ?? null)}
                    >
                        {isFetching ? 'Loading candidates' : 'Load more candidates'}
                    </Button>
                </div>
            ) : null}
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
