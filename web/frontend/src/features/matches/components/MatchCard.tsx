import React from 'react';
import { Building2, Eye, EyeOff, MapPin, Sparkles, Wifi } from 'lucide-react';
import type { MatchSummary } from '@/types/api';
import { formatScore } from '@/utils/formatters';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { matchesApi } from '@/services/matchesApi';
import { toast } from '@/components/ui/Toast';

interface MatchCardProps {
    match: MatchSummary;
    onSelect: (matchId: string) => void;
    featured?: boolean;
}

function renderVisibilityToggleIcon(isPending: boolean, isHidden: boolean) {
    if (isPending) {
        return <span className="h-3 w-3 animate-spin rounded-full border border-current border-t-transparent" />;
    }
    return isHidden ? <EyeOff className="h-3.5 w-3.5" /> : <Eye className="h-3.5 w-3.5" />;
}

function llmStatusLabel(status?: string | null): string | null {
    if (!status) return null;
    if (status === 'succeeded') return 'LLM judged';
    if (status === 'pending' || status === 'running') return 'LLM pending';
    if (status === 'failed') return 'LLM failed';
    if (status === 'skipped') return 'LLM skipped';
    return status.replace(/_/g, ' ');
}

function preferenceStatusLabel(status?: Record<string, any> | null): string | null {
    if (!status) return null;
    if (status.applied) return 'Preferences applied';

    const reason = status.reason ?? 'unconfigured';
    const messages: Record<string, string> = {
        disabled: 'Preferences disabled',
        unconfigured: 'Preferences disabled',
        missing_job_offerings: 'Waiting for job offering extraction',
        job_offerings_unavailable: 'Waiting for job offering extraction',
        outside_preference_window: 'Outside preference judging window',
        preference_scorer_unavailable: 'Preference scorer unavailable',
        preference_reranker_unavailable: 'Preference scorer unavailable',
        preference_judge_unavailable: 'Preference scorer unavailable',
        invalid_llm_output: 'Preference scorer returned invalid output',
        preference_scorer_failed: 'Preference scorer failed',
    };
    return messages[reason] ?? 'Preference scorer failed';
}

function llmOrderingLabel(match: MatchSummary): string | null {
    if (match.llm_effective_for_rerank && match.llm_original_rank && match.llm_reranked_rank) {
        if (match.llm_original_rank !== match.llm_reranked_rank) {
            return `LLM-applied #${match.llm_original_rank} -> #${match.llm_reranked_rank}`;
        }
        return 'LLM-applied ordering';
    }
    if (match.llm_ignored_for_rerank_reason) {
        return `LLM not applied: ${match.llm_ignored_for_rerank_reason.replace(/_/g, ' ')}`;
    }
    if (match.llm_evaluation_status === 'pending' || match.llm_evaluation_status === 'running') {
        return 'LLM pending';
    }
    return null;
}

export const MatchCard: React.FC<MatchCardProps> = ({ match, onSelect, featured = false }) => {
    const queryClient = useQueryClient();

    const toggleHiddenMutation = useMutation({
        mutationFn: (matchId: string) => matchesApi.toggleHidden(matchId),
        onSuccess: (response, matchId) => {
            const newlyHidden = response?.data?.is_hidden;
            if (typeof newlyHidden !== 'boolean') {
                toast.error('Could not update job visibility.');
                return;
            }

            queryClient.setQueryData(['matches'], (old: any) => {
                if (!old?.matches) return old;
                return {
                    ...old,
                    matches: old.matches.map((m: MatchSummary) =>
                        m.match_id === matchId ? { ...m, is_hidden: newlyHidden } : m,
                    ),
                };
            });

            queryClient.invalidateQueries({ queryKey: ['matches'] });
            queryClient.invalidateQueries({ queryKey: ['stats'] });

            if (newlyHidden) {
                toast.success('Hidden from your list', {
                    action: {
                        label: 'Undo',
                        onClick: () => toggleHiddenMutation.mutate(matchId),
                    },
                    duration: 5000,
                });
            }
        },
        onError: () => {
            toast.error('Could not update job visibility.');
        },
    });

    const handleToggleHidden = (e: React.MouseEvent) => {
        e.stopPropagation();
        if ((match.selection_tier ?? 'primary') !== 'primary') return;
        toggleHiddenMutation.mutate(match.match_id);
    };

    const fitScore = match.fit_score ?? 0;
    const reqCoverage = Math.round((match.required_coverage ?? 0) * 100);
    const tier = match.selection_tier ?? 'primary';
    const isExcluded = tier !== 'primary';
    const canToggleHidden = !isExcluded;

    const rootMuted = match.is_hidden || isExcluded;
    let scoreColor = 'text-ink';
    if (rootMuted) {
        scoreColor = 'text-ink-muted';
    } else if (fitScore >= 80) {
        scoreColor = 'text-accent';
    }

    const coveragePct = Math.max(0, Math.min(100, reqCoverage));
    const llmOrdering = llmOrderingLabel(match);
    const llmDisplayScore = typeof match.llm_rerank_score === 'number'
        ? match.llm_rerank_score
        : typeof match.llm_score === 'number'
            ? match.llm_score
            : null;
    const llmScoreTone = match.llm_effective_for_rerank ? 'text-accent' : 'text-ink-muted';
    const llmScoreTitle = match.llm_ignored_for_rerank_reason
        ? `LLM score not used for ordering: ${match.llm_ignored_for_rerank_reason.replace(/_/g, ' ')}`
        : 'LLM second-pass score';
    const preferenceStatus = preferenceStatusLabel(match.preference_status);
    const preferenceStatusTone = match.preference_status?.applied ? 'text-accent' : 'text-ink-muted';

    return (
        <article
            className={`group relative border-b border-rule transition-colors duration-200 ease-out ${
                featured ? 'bg-surface border-t border-l border-r rounded-t-sm' : 'bg-canvas hover:bg-surface'
            } ${rootMuted ? 'opacity-60' : ''}`}
        >
            <button
                type="button"
                className="absolute inset-0 z-10 focus-visible:outline focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-[-3px]"
                onClick={() => onSelect(match.match_id)}
                aria-label={`View details for ${match.title} at ${match.company}`}
            />

            <div
                className={`relative grid items-start gap-5 px-5 py-5 sm:px-7 sm:py-6 ${
                    featured
                        ? 'grid-cols-[auto_1fr_auto] gap-6 sm:py-8'
                        : 'grid-cols-[auto_1fr_auto]'
                }`}
            >
                {/* Score — the hero numeral */}
                <div className="flex min-w-[88px] flex-col items-start">
                    <span
                        className={`display-numeral ${featured ? 'text-[72px]' : 'text-[44px]'} ${scoreColor}`}
                        aria-label={`Fit score ${fitScore} out of 100`}
                    >
                        {formatScore(fitScore)}
                    </span>
                    <span className="caption mt-0.5">Fit</span>
                    {llmDisplayScore !== null && (
                        <span
                            className={`num mt-1 text-[12px] leading-tight tabular-nums ${llmScoreTone}`}
                            title={llmScoreTitle}
                            aria-label={`LLM second-pass score ${formatScore(llmDisplayScore)}`}
                        >
                            LLM {formatScore(llmDisplayScore)}
                        </span>
                    )}
                </div>

                {/* Meta */}
                <div className="min-w-0">
                    <h3
                        className={`${
                            featured ? 'text-[22px] sm:text-[26px]' : 'text-[17px]'
                        } font-medium leading-snug tracking-tight text-ink group-hover:text-accent-ink`}
                    >
                        {match.title}
                    </h3>

                    <div className="mt-2 flex flex-wrap items-center gap-x-4 gap-y-1 text-[13px] text-ink-soft">
                        <span className="inline-flex items-center gap-1.5">
                            <Building2 className="h-3.5 w-3.5 text-ink-muted" aria-hidden="true" />
                            <span className="text-ink">{match.company}</span>
                        </span>
                        {match.location && (
                            <span className="inline-flex items-center gap-1.5">
                                <MapPin className="h-3.5 w-3.5 text-ink-muted" aria-hidden="true" />
                                <span>{match.location}</span>
                            </span>
                        )}
                        {match.is_remote && (
                            <span className="inline-flex items-center gap-1.5">
                                <Wifi className="h-3.5 w-3.5 text-ink-muted" aria-hidden="true" />
                                <span>Remote</span>
                            </span>
                        )}
                        <span className="caption">
                            {match.match_type.replace('_', ' ')}
                        </span>
                    </div>

                    {/* Single diagnostic strip — requirement coverage */}
                    <div className="mt-4 flex items-center gap-3">
                        <div className="h-px flex-1 bg-rule relative overflow-hidden">
                            <div
                                className="absolute inset-y-[-1px] left-0 bg-ink-soft"
                                style={{ width: `${coveragePct}%`, height: '3px' }}
                            />
                        </div>
                        <span className="num text-[12px] text-ink-muted tabular-nums">
                            {coveragePct}% covered
                        </span>
                    </div>
                </div>

                {/* Right rail — quiet state + hide */}
                <div className="relative z-20 flex flex-col items-end gap-2">
                    {isExcluded && (
                        <span className="caption text-warn">
                            {(match.excluded_reason ?? 'excluded').replace(/_/g, ' ')}
                        </span>
                    )}
                    {match.is_hidden && !isExcluded && (
                        <span className="caption text-ink-muted">hidden</span>
                    )}
                    {llmStatusLabel(match.llm_evaluation_status) && (
                        <span
                            className="caption inline-flex items-center gap-1 text-accent"
                            title="LLM evaluation status"
                            aria-label={llmStatusLabel(match.llm_evaluation_status) ?? undefined}
                        >
                            <Sparkles className="h-3.5 w-3.5" aria-hidden="true" />
                            {llmStatusLabel(match.llm_evaluation_status)}
                        </span>
                    )}
                    {llmOrdering && (
                        <span
                            className="caption max-w-[12rem] text-right text-ink-muted"
                            title={llmOrdering}
                            aria-label={llmOrdering}
                        >
                            {llmOrdering}
                        </span>
                    )}
                    {preferenceStatus && (
                        <span
                            className={`caption max-w-[14rem] text-right ${preferenceStatusTone}`}
                            title={preferenceStatus}
                            aria-label={preferenceStatus}
                        >
                            {preferenceStatus}
                        </span>
                    )}

                    {canToggleHidden && (
                        <button
                            type="button"
                            onClick={handleToggleHidden}
                            disabled={toggleHiddenMutation.isPending}
                            className="inline-flex h-7 w-7 items-center justify-center rounded-sm text-ink-muted opacity-0 transition-opacity duration-200 hover:bg-surface-sunk hover:text-ink group-hover:opacity-100 focus-visible:opacity-100 disabled:opacity-30"
                            title={match.is_hidden ? 'Unhide' : 'Hide'}
                            aria-label={match.is_hidden ? 'Unhide' : 'Hide'}
                            aria-pressed={match.is_hidden}
                        >
                            {renderVisibilityToggleIcon(toggleHiddenMutation.isPending, match.is_hidden)}
                        </button>
                    )}
                </div>
            </div>

            {featured && (
                <div className="relative border-t border-rule bg-surface-sunk px-5 py-2.5 text-[12px] text-ink-muted sm:px-7">
                    <span className="caption text-accent">Top match</span>
                    <span className="mx-3 text-ink-faint">·</span>
                    <span>Open to see the requirement-by-requirement evidence.</span>
                </div>
            )}
        </article>
    );
};
