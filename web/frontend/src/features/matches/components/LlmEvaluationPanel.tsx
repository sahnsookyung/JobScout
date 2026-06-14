import React from 'react';
import { RefreshCw, Sparkles, RotateCcw, Trash2 } from 'lucide-react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';

import { Badge } from '@/components/ui/Badge';
import { Button } from '@/components/ui/Button';
import { toast } from '@/components/ui/Toast';
import { usePolicy } from '@/hooks/usePolicy';
import { matchesApi } from '@/services/matchesApi';
import type { MatchLlmEvaluation, MatchLlmEvaluationListResponse } from '@/types/api';
import { formatScore } from '@/utils/formatters';

type Props = Readonly<{
    matchId: string;
    markerStatus?: string | null;
}>;

type LlmActionButtonProps = Readonly<{
    label: string;
    tooltip: string;
    children: React.ReactNode;
    disabled?: boolean;
    active?: boolean;
    tone?: 'default' | 'danger';
    variant?: React.ComponentProps<typeof Button>['variant'];
    onClick: () => void;
}>;

function statusBadgeVariant(status?: string | null): 'success' | 'warning' | 'error' | 'info' | 'default' {
    if (status === 'succeeded') return 'success';
    if (status === 'pending' || status === 'running') return 'info';
    if (status === 'failed') return 'error';
    if (status === 'skipped') return 'warning';
    return 'default';
}

function statusLabel(status?: string | null): string {
    if (!status) return 'Not judged';
    return status.replace(/_/g, ' ');
}

function latestEvaluation(data?: MatchLlmEvaluationListResponse): MatchLlmEvaluation | null {
    return data?.evaluations?.[0] ?? null;
}

function isEvaluationInFlight(status?: string | null): boolean {
    return status === 'pending' || status === 'running';
}

function progressMessage(status?: string | null): string {
    if (status === 'pending') return 'Queued for Cerebras review. This panel will update automatically.';
    if (status === 'running') {
        return 'Cerebras is reviewing the full resume and job description. This panel will update automatically.';
    }
    return '';
}

function cleanLabel(value?: string | null): string {
    if (!value) return '';
    return value.replace(/_/g, ' ');
}

function availabilityMessage(reason?: string | null): string {
    if (reason === 'credentials_missing') return 'Cerebras API key missing.';
    if (reason === 'disabled') return 'LLM judging is disabled.';
    if (reason === 'base_url_missing') return 'LLM judge provider URL missing.';
    if (reason === 'model_missing') return 'LLM judge model missing.';
    if (reason === 'runtime_missing') return 'LLM judge runtime missing.';
    if (reason === 'config_missing') return 'LLM judge configuration missing.';
    if (!reason || reason === 'available') return '';
    return cleanLabel(reason);
}

function stringList(value: unknown): string[] {
    return Array.isArray(value)
        ? value.filter((item): item is string => typeof item === 'string' && item.trim().length > 0)
        : [];
}

const actionButtonClasses = [
    'h-9 w-9 px-0 transition-all duration-150',
    'hover:-translate-y-0.5 hover:border-accent hover:bg-accent-soft hover:text-accent',
    'focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent',
    'disabled:hover:translate-y-0 disabled:hover:border-rule disabled:hover:bg-surface disabled:hover:text-ink-muted',
].join(' ');

const dangerActionButtonClasses = [
    actionButtonClasses,
    'text-warn hover:border-warn hover:bg-warn-soft hover:text-warn',
].join(' ');

const activeActionButtonClasses = [
    actionButtonClasses,
    'border-accent bg-accent-soft text-accent shadow-[0_0_18px_rgba(70,130,110,0.35)] disabled:opacity-100',
].join(' ');

const LlmActionButton: React.FC<LlmActionButtonProps> = ({
    label,
    tooltip,
    children,
    disabled,
    active,
    tone = 'default',
    variant = 'secondary',
    onClick,
}) => {
    const tooltipId = React.useId();
    const className = active
        ? activeActionButtonClasses
        : tone === 'danger'
            ? dangerActionButtonClasses
            : actionButtonClasses;
    return (
        <span className="group relative inline-flex">
            <Button
                type="button"
                variant={variant}
                size="sm"
                className={className}
                onClick={onClick}
                title={tooltip}
                aria-label={label}
                aria-describedby={tooltipId}
                disabled={disabled}
                aria-busy={active || undefined}
            >
                <span
                    className={active ? 'inline-flex animate-pulse drop-shadow-[0_0_6px_rgba(70,130,110,0.65)]' : 'inline-flex'}
                >
                    {children}
                </span>
            </Button>
            <span
                id={tooltipId}
                role="tooltip"
                className="pointer-events-none absolute right-0 top-full z-10 mt-2 w-max max-w-[14rem] border border-rule bg-ink px-2.5 py-1.5 text-[11px] font-medium normal-case tracking-normal text-canvas opacity-0 shadow-sm transition-opacity duration-150 group-hover:opacity-100 group-focus-within:opacity-100"
            >
                {tooltip}
            </span>
        </span>
    );
};

export const LlmEvaluationPanel: React.FC<Props> = ({ matchId, markerStatus }) => {
    const queryClient = useQueryClient();
    const { policy } = usePolicy();
    const queryKey = ['match-llm-evaluations', matchId] as const;

    const { data, isLoading, refetch } = useQuery({
        queryKey,
        queryFn: async () => {
            const response = await matchesApi.getLlmEvaluations(matchId);
            return response.data;
        },
        enabled: Boolean(matchId),
        refetchInterval: (query) => {
            const latest = latestEvaluation(query.state.data as MatchLlmEvaluationListResponse | undefined);
            return isEvaluationInFlight(latest?.status ?? markerStatus) ? 2500 : false;
        },
        refetchIntervalInBackground: true,
        staleTime: 30000,
    });

    const evaluation = latestEvaluation(data);
    const activeStatus = evaluation?.status ?? markerStatus ?? null;
    const evaluationInFlight = isEvaluationInFlight(activeStatus);
    const currentProgressMessage = progressMessage(activeStatus);

    const invalidate = () => {
        queryClient.invalidateQueries({ queryKey });
        queryClient.invalidateQueries({ queryKey: ['match', matchId] });
        queryClient.invalidateQueries({ queryKey: ['matches'] });
    };

    const generateMutation = useMutation({
        mutationFn: (force: boolean) => matchesApi.generateLlmEvaluation(matchId, force),
        onSuccess: (response) => {
            queryClient.setQueryData<MatchLlmEvaluationListResponse>(queryKey, (old) => {
                const nextEvaluation = response.data.evaluation;
                if (!nextEvaluation) return old;
                return {
                    success: true,
                    count: 1,
                    evaluations: [nextEvaluation],
                };
            });
            invalidate();
            const nextStatus = response.data.evaluation?.status;
            const isQueued = Boolean(response.data.accepted) || isEvaluationInFlight(nextStatus);
            toast.success(
                response.data.reused
                    ? 'Reused LLM evaluation'
                    : isQueued
                        ? 'LLM evaluation started'
                        : 'LLM evaluation ready',
            );
        },
        onError: (error: any) => {
            toast.error(error?.message ?? 'Could not generate LLM evaluation.');
        },
    });

    const deleteMutation = useMutation({
        mutationFn: (evaluationId: string) => matchesApi.deleteLlmEvaluation(matchId, evaluationId),
        onMutate: async () => {
            await queryClient.cancelQueries({ queryKey });
            const previous = queryClient.getQueryData<MatchLlmEvaluationListResponse>(queryKey);
            queryClient.setQueryData<MatchLlmEvaluationListResponse>(queryKey, {
                success: true,
                count: 0,
                evaluations: [],
            });
            return { previous };
        },
        onError: (error: any, _evaluationId, context) => {
            if (context?.previous) {
                queryClient.setQueryData(queryKey, context.previous);
            }
            toast.error(error?.message ?? 'Could not delete LLM evaluation.');
        },
        onSuccess: () => {
            invalidate();
            toast.success('Deleted LLM evaluation');
        },
    });

    const previousStatusRef = React.useRef<string | null>(null);
    React.useEffect(() => {
        const previousStatus = previousStatusRef.current;
        if (activeStatus === 'succeeded' && isEvaluationInFlight(previousStatus)) {
            invalidate();
            toast.success('LLM evaluation ready');
        } else if (activeStatus === 'failed' && isEvaluationInFlight(previousStatus)) {
            invalidate();
            toast.error('LLM evaluation failed.');
        }
        previousStatusRef.current = activeStatus;
    }, [activeStatus]);

    const isBusy = generateMutation.isPending || deleteMutation.isPending || evaluationInFlight;
    const hasEvaluation = Boolean(evaluation);
    const score = typeof evaluation?.llm_score === 'number' ? formatScore(evaluation.llm_score) : null;
    const analysis = evaluation?.analysis ?? {};
    const transferableStrengths = stringList(analysis.transferable_strengths);
    const gaps = stringList(analysis.gaps);
    const rankingRationale = typeof analysis.ranking_rationale === 'string' ? analysis.ranking_rationale : '';
    const requirementVerdicts = Array.isArray(evaluation?.requirement_verdicts)
        ? evaluation.requirement_verdicts
        : [];
    const truncation = evaluation?.input_truncation ?? analysis.input_truncation ?? {};
    const hasTruncation = Boolean((truncation as any)?.truncated);
    const ignoredReason = evaluation?.ignored_for_rerank_reason;
    const staleStatus = evaluation?.stale_status;
    const judgeAvailable = policy?.llm_judge_available ?? true;
    const unavailableMessage = judgeAvailable
        ? ''
        : availabilityMessage(policy?.llm_judge_unavailable_reason);

    return (
        <section className="border border-rule bg-surface p-5">
            <div className="flex flex-wrap items-start justify-between gap-3">
                <div>
                    <p className="caption">LLM evaluation</p>
                    <h4 className="mt-1 text-[18px] font-medium text-ink">Second-pass relevance review</h4>
                </div>
                <div className="flex items-center gap-2">
                    <LlmActionButton
                        variant="ghost"
                        onClick={() => refetch()}
                        label="Refresh LLM evaluation"
                        tooltip="Refresh the latest LLM evaluation status"
                    >
                        <RefreshCw className="h-4 w-4" aria-hidden="true" />
                    </LlmActionButton>
                    <LlmActionButton
                        active={(generateMutation.isPending && !hasEvaluation) || evaluationInFlight}
                        onClick={() => generateMutation.mutate(false)}
                        label="Generate LLM evaluation"
                        tooltip={currentProgressMessage || unavailableMessage || 'Generate a second-pass relevance review'}
                        disabled={isBusy || !judgeAvailable}
                    >
                        <Sparkles className="h-4 w-4" aria-hidden="true" />
                    </LlmActionButton>
                    <LlmActionButton
                        active={generateMutation.isPending && hasEvaluation}
                        onClick={() => generateMutation.mutate(true)}
                        label="Regenerate LLM evaluation"
                        tooltip={currentProgressMessage || unavailableMessage || 'Regenerate and replace the cached review'}
                        disabled={isBusy || !hasEvaluation || !judgeAvailable}
                    >
                        <RotateCcw className="h-4 w-4" aria-hidden="true" />
                    </LlmActionButton>
                    <LlmActionButton
                        variant="ghost"
                        tone="danger"
                        onClick={() => evaluation && deleteMutation.mutate(evaluation.id)}
                        label="Delete LLM evaluation"
                        tooltip="Delete this cached LLM review"
                        disabled={isBusy || !evaluation}
                    >
                        <Trash2 className="h-4 w-4" aria-hidden="true" />
                    </LlmActionButton>
                </div>
            </div>

            <div className="mt-4 flex flex-wrap items-center gap-2">
                <Badge variant={statusBadgeVariant(activeStatus)}>{statusLabel(activeStatus)}</Badge>
                {score && (
                    <span className="caption">
                        Score <span className="text-ink">{score}</span>
                    </span>
                )}
                {typeof evaluation?.confidence === 'number' && (
                    <span className="caption">
                        Confidence <span className="text-ink">{formatScore(evaluation.confidence * 100)}</span>
                    </span>
                )}
                {evaluation?.verdict && <span className="caption">{evaluation.verdict}</span>}
                {staleStatus && staleStatus !== 'current' && (
                    <span className="caption text-warn">{cleanLabel(staleStatus)}</span>
                )}
            </div>

            {unavailableMessage && (
                <p className="mt-4 border-l-2 border-warn/60 pl-3 text-[13px] leading-relaxed text-ink-soft">
                    LLM judge unavailable: {unavailableMessage}
                </p>
            )}

            {currentProgressMessage && (
                <p
                    className="mt-4 border-l-2 border-accent/60 pl-3 text-[13px] leading-relaxed text-ink-soft"
                    aria-live="polite"
                >
                    {currentProgressMessage}
                </p>
            )}

            {isLoading && !evaluation ? (
                <p className="mt-4 text-[13px] text-ink-muted">Loading evaluation status.</p>
            ) : evaluation?.summary ? (
                <p className="mt-4 text-[14px] leading-relaxed text-ink-soft">{evaluation.summary}</p>
            ) : currentProgressMessage ? (
                <p className="mt-4 text-[13px] text-ink-muted">
                    Waiting for the review to finish.
                </p>
            ) : (
                <p className="mt-4 text-[13px] text-ink-muted">
                    Generate an LLM review for this job to add a second-pass relevance explanation.
                </p>
            )}

            {ignoredReason && (
                <p className="mt-4 border-l-2 border-warn/60 pl-3 text-[13px] leading-relaxed text-ink-soft">
                    Not used for ordering: {cleanLabel(ignoredReason)}.
                </p>
            )}

            {hasTruncation && (
                <p className="mt-4 border-l-2 border-warn/60 pl-3 text-[13px] leading-relaxed text-ink-soft">
                    Judge input was truncated for this review.
                </p>
            )}

            {rankingRationale && (
                <div className="mt-5 border-t border-rule pt-4">
                    <p className="caption">Ranking rationale</p>
                    <p className="mt-2 text-[13px] leading-relaxed text-ink-soft">{rankingRationale}</p>
                </div>
            )}

            {(transferableStrengths.length > 0 || gaps.length > 0) && (
                <div className="mt-5 grid gap-4 md:grid-cols-2">
                    {transferableStrengths.length > 0 && (
                        <div>
                            <p className="caption">Transferable strengths</p>
                            <ul className="mt-2 space-y-2 text-[13px] leading-relaxed text-ink-soft">
                                {transferableStrengths.map((item) => (
                                    <li key={item} className="border-l-2 border-affirm/50 pl-3">{item}</li>
                                ))}
                            </ul>
                        </div>
                    )}
                    {gaps.length > 0 && (
                        <div>
                            <p className="caption">Gaps</p>
                            <ul className="mt-2 space-y-2 text-[13px] leading-relaxed text-ink-soft">
                                {gaps.map((item) => (
                                    <li key={item} className="border-l-2 border-warn/50 pl-3">{item}</li>
                                ))}
                            </ul>
                        </div>
                    )}
                </div>
            )}

            {requirementVerdicts.length > 0 && (
                <div className="mt-5 border-t border-rule pt-4">
                    <p className="caption">LLM requirement verdicts</p>
                    <div className="mt-3 grid gap-2">
                        {requirementVerdicts.slice(0, 12).map((verdict, index) => (
                            <div key={`${verdict.requirement_id ?? index}`} className="border border-rule bg-surface-sunk px-3 py-2">
                                <div className="flex flex-wrap items-center gap-2">
                                    <Badge variant={verdict.verdict === 'strong' ? 'success' : verdict.verdict === 'missing' ? 'warning' : 'info'}>
                                        {cleanLabel(verdict.verdict ?? 'unknown')}
                                    </Badge>
                                    <span className="caption">{verdict.requirement_id ?? `req ${index + 1}`}</span>
                                </div>
                                {typeof verdict.reason === 'string' && (
                                    <p className="mt-2 text-[13px] leading-relaxed text-ink-soft">{verdict.reason}</p>
                                )}
                            </div>
                        ))}
                    </div>
                </div>
            )}

            {evaluation?.reason_codes?.length ? (
                <div className="mt-4 flex flex-wrap gap-2">
                    {evaluation.reason_codes.map((code) => (
                        <span key={code} className="caption border border-rule px-2 py-1">
                            {cleanLabel(code)}
                        </span>
                    ))}
                </div>
            ) : null}
        </section>
    );
};
