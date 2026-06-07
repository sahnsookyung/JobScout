import React from 'react';
import { RefreshCw, Sparkles, RotateCcw, Trash2 } from 'lucide-react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';

import { Badge } from '@/components/ui/Badge';
import { Button } from '@/components/ui/Button';
import { toast } from '@/components/ui/Toast';
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
    isLoading?: boolean;
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

const LlmActionButton: React.FC<LlmActionButtonProps> = ({
    label,
    tooltip,
    children,
    disabled,
    isLoading,
    tone = 'default',
    variant = 'secondary',
    onClick,
}) => {
    const tooltipId = React.useId();
    return (
        <span className="group relative inline-flex">
            <Button
                type="button"
                variant={variant}
                size="sm"
                className={tone === 'danger' ? dangerActionButtonClasses : actionButtonClasses}
                isLoading={isLoading}
                onClick={onClick}
                title={tooltip}
                aria-label={label}
                aria-describedby={tooltipId}
                disabled={disabled}
            >
                {children}
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
    const queryKey = ['match-llm-evaluations', matchId] as const;

    const { data, isLoading, refetch } = useQuery({
        queryKey,
        queryFn: async () => {
            const response = await matchesApi.getLlmEvaluations(matchId);
            return response.data;
        },
        enabled: Boolean(matchId),
        staleTime: 30000,
    });

    const evaluation = latestEvaluation(data);
    const activeStatus = evaluation?.status ?? markerStatus ?? null;

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
            toast.success(response.data.reused ? 'Reused LLM evaluation' : 'LLM evaluation ready');
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

    const isBusy = generateMutation.isPending || deleteMutation.isPending;
    const hasEvaluation = Boolean(evaluation);
    const score = typeof evaluation?.llm_score === 'number' ? formatScore(evaluation.llm_score) : null;

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
                        isLoading={generateMutation.isPending && !hasEvaluation}
                        onClick={() => generateMutation.mutate(false)}
                        label="Generate LLM evaluation"
                        tooltip="Generate a second-pass relevance review"
                        disabled={isBusy}
                    >
                        <Sparkles className="h-4 w-4" aria-hidden="true" />
                    </LlmActionButton>
                    <LlmActionButton
                        isLoading={generateMutation.isPending && hasEvaluation}
                        onClick={() => generateMutation.mutate(true)}
                        label="Regenerate LLM evaluation"
                        tooltip="Regenerate and replace the cached review"
                        disabled={isBusy || !hasEvaluation}
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
            </div>

            {isLoading && !evaluation ? (
                <p className="mt-4 text-[13px] text-ink-muted">Loading evaluation status.</p>
            ) : evaluation?.summary ? (
                <p className="mt-4 text-[14px] leading-relaxed text-ink-soft">{evaluation.summary}</p>
            ) : (
                <p className="mt-4 text-[13px] text-ink-muted">
                    Generate an LLM review for this job to add a second-pass relevance explanation.
                </p>
            )}

            {evaluation?.reason_codes?.length ? (
                <div className="mt-4 flex flex-wrap gap-2">
                    {evaluation.reason_codes.map((code) => (
                        <span key={code} className="caption border border-rule px-2 py-1">
                            {code.replace(/_/g, ' ')}
                        </span>
                    ))}
                </div>
            ) : null}
        </section>
    );
};
