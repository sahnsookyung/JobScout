import { useCallback, useEffect, useRef, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Minus, Plus } from 'lucide-react';

import { usePolicy } from '@/hooks/usePolicy';
import { pipelineRunsApi } from '@/services/pipelineRunsApi';
import type { PolicyUpdatePayload, RankingMode } from '@/types/api';
import { RANKING_MODE_OPTIONS } from '@/utils/constants';

export function PreferenceRankingSettings() {
    const { policy, isLoading, updatePolicy, updatePolicyAsync, isUpdatingPolicy } = usePolicy();
    const llmQueueStatus = useQuery({
        queryKey: ['llm-evaluation-queue'],
        queryFn: async () => {
            const response = await pipelineRunsApi.getLlmEvaluationQueueStatus();
            return response.data;
        },
        enabled: Boolean(policy?.llm_judge_available),
    });
    const [rankingMode, setRankingMode] = useState<RankingMode>('balanced');
    const [balancedPreferencePercent, setBalancedPreferencePercent] = useState(60);
    const [llmJudgeEnabled, setLlmJudgeEnabled] = useState(false);
    const [llmJudgeAutoEnqueueEnabled, setLlmJudgeAutoEnqueueEnabled] = useState(false);
    const [llmJudgeTopN, setLlmJudgeTopN] = useState(5);
    const [llmApplyStatus, setLlmApplyStatus] = useState<'idle' | 'saving' | 'saved' | 'failed'>('idle');
    const [llmApplyMessage, setLlmApplyMessage] = useState('');
    const hasUserAdjustedRankingSettings = useRef(false);
    const hasUserAdjustedLlmSettings = useRef(false);
    const pendingRankingPolicy = useRef<PolicyUpdatePayload | null>(null);
    const rankingUpdateTimeout = useRef<ReturnType<typeof setTimeout> | null>(null);
    const updatePolicyRef = useRef(updatePolicy);
    updatePolicyRef.current = updatePolicy;

    useEffect(() => {
        if (!policy) return;

        if (!hasUserAdjustedRankingSettings.current) {
            setRankingMode(policy.active_default_mode ?? 'balanced');
            setBalancedPreferencePercent(Math.round((policy.balanced_w_pref ?? 0.6) * 100));
        }
        if (!hasUserAdjustedLlmSettings.current) {
            setLlmJudgeEnabled(Boolean(policy.llm_judge_enabled));
            setLlmJudgeAutoEnqueueEnabled(Boolean(policy.llm_judge_auto_enqueue_enabled));
            setLlmJudgeTopN(policy.llm_judge_top_n ?? 5);
        }
    }, [policy]);

    const flushPendingRankingPolicy = useCallback(() => {
        if (rankingUpdateTimeout.current !== null) {
            clearTimeout(rankingUpdateTimeout.current);
            rankingUpdateTimeout.current = null;
        }
        const pendingPolicy = pendingRankingPolicy.current;
        pendingRankingPolicy.current = null;
        if (pendingPolicy === null) return;

        hasUserAdjustedRankingSettings.current = false;
        updatePolicyRef.current(pendingPolicy);
    }, []);

    const queueRankingPolicyUpdate = useCallback((
        nextRankingMode: RankingMode,
        nextBalancedPreferencePercent: number,
    ) => {
        pendingRankingPolicy.current = {
            min_fit: policy?.min_fit,
            top_k: policy?.top_k,
            min_jd_required_coverage: policy?.min_jd_required_coverage ?? null,
            active_default_mode: nextRankingMode,
            balanced_w_pref: nextBalancedPreferencePercent / 100,
            balanced_w_fit: 1 - nextBalancedPreferencePercent / 100,
        };
        if (rankingUpdateTimeout.current !== null) {
            clearTimeout(rankingUpdateTimeout.current);
        }
        rankingUpdateTimeout.current = setTimeout(flushPendingRankingPolicy, 250);
    }, [flushPendingRankingPolicy, policy]);

    useEffect(() => {
        return flushPendingRankingPolicy;
    }, [flushPendingRankingPolicy]);

    const llmDraftDirty = Boolean(policy) && (
        Boolean(policy?.llm_judge_enabled) !== llmJudgeEnabled ||
        Boolean(policy?.llm_judge_auto_enqueue_enabled) !== llmJudgeAutoEnqueueEnabled ||
        Number(policy?.llm_judge_top_n ?? 5) !== llmJudgeTopN
    );
    const queueBacklog = Number(llmQueueStatus.data?.queued ?? 0)
        + Number(llmQueueStatus.data?.scheduled ?? 0)
        + Number(llmQueueStatus.data?.db_pending ?? 0)
        + Number(llmQueueStatus.data?.db_retryable_failed ?? 0);

    const handleRankingModeChange = (value: RankingMode) => {
        hasUserAdjustedRankingSettings.current = true;
        setRankingMode(value);
        queueRankingPolicyUpdate(value, balancedPreferencePercent);
    };

    const handleBalancedPreferenceChange = (value: number) => {
        const nextValue = Math.max(0, Math.min(100, value));
        hasUserAdjustedRankingSettings.current = true;
        setBalancedPreferencePercent(nextValue);
        queueRankingPolicyUpdate(rankingMode, nextValue);
    };

    const resetLlmApplyStatus = () => {
        hasUserAdjustedLlmSettings.current = true;
        setLlmApplyStatus('idle');
        setLlmApplyMessage('');
    };

    const handleLlmEnabledChange = (value: boolean) => {
        resetLlmApplyStatus();
        setLlmJudgeEnabled(value);
        if (!value) setLlmJudgeAutoEnqueueEnabled(false);
    };

    const handleLlmAutoEnqueueChange = (value: boolean) => {
        resetLlmApplyStatus();
        setLlmJudgeAutoEnqueueEnabled(value);
        if (value) setLlmJudgeEnabled(true);
    };

    const handleLlmTopNChange = (value: number) => {
        resetLlmApplyStatus();
        const maxTopN = policy?.llm_judge_top_n_max ?? 10;
        setLlmJudgeTopN(Math.max(1, Math.min(maxTopN, value)));
    };

    const handleApplyLlmSettings = async () => {
        if (!policy) return;
        setLlmApplyStatus('saving');
        setLlmApplyMessage('');
        try {
            const response = await updatePolicyAsync({
                min_fit: policy.min_fit,
                top_k: policy.top_k,
                min_jd_required_coverage: policy.min_jd_required_coverage ?? null,
                llm_judge_enabled: llmJudgeEnabled,
                llm_judge_auto_enqueue_enabled: llmJudgeAutoEnqueueEnabled,
                llm_judge_top_n: llmJudgeTopN,
            });
            const updated = response.data;
            hasUserAdjustedLlmSettings.current = false;
            setLlmApplyStatus('saved');
            if (updated.llm_judge_enqueue_state === 'scheduled') {
                setLlmApplyMessage('Scheduled');
            } else if (updated.llm_judge_enqueue_state === 'reused') {
                setLlmApplyMessage('Already queued');
            } else if (updated.llm_judge_enqueue_state === 'failed' || updated.degraded) {
                setLlmApplyStatus('failed');
                setLlmApplyMessage('Queue unavailable');
            } else {
                setLlmApplyMessage('Saved');
            }
        } catch {
            setLlmApplyStatus('failed');
            setLlmApplyMessage('Save failed');
        }
    };

    if (isLoading) {
        return <div className="h-52 animate-pulse border border-rule bg-surface-sunk" />;
    }

    return (
        <section
            data-testid="preference-ranking-settings"
            className="border border-rule bg-surface"
            aria-labelledby="preference-ranking-title"
        >
            <div className="border-b border-rule px-5 py-4">
                <p className="caption">Preference ranking</p>
                <h3 id="preference-ranking-title" className="mt-1 text-[15px] font-medium text-ink">
                    Order your best matches
                </h3>
                <p className="mt-1 text-[13px] text-ink-soft">
                    Choose how preference and fit combine after your must-haves pass.
                </p>
            </div>

            <div className="grid gap-px bg-rule lg:grid-cols-3">
                <div className="bg-surface px-5 py-5">
                    <label className="block" htmlFor="ranking-default-mode">
                        <span className="caption">Default order</span>
                        <select
                            id="ranking-default-mode"
                            value={rankingMode}
                            onChange={(event) => handleRankingModeChange(event.target.value as RankingMode)}
                            className="mt-2 w-full border border-rule bg-surface px-3 py-2.5 text-[13px] text-ink outline-none focus:border-accent"
                        >
                            {RANKING_MODE_OPTIONS.map((option) => (
                                <option key={option.value} value={option.value}>{option.label}</option>
                            ))}
                        </select>
                    </label>
                    <div className="mt-4 space-y-1 text-[12px] leading-relaxed text-ink-muted">
                        <p className={rankingMode === 'preference_first' ? 'text-accent' : ''}>
                            Preference first: preference → fit → similarity
                        </p>
                        <p className={rankingMode === 'fit_first' ? 'text-accent' : ''}>
                            Fit first: fit → preference → similarity
                        </p>
                        <p className={rankingMode === 'balanced' ? 'text-accent' : ''}>
                            Balanced: weighted preference + normalized fit
                        </p>
                    </div>
                </div>

                <div className="bg-surface px-5 py-5">
                    <div className="flex items-baseline justify-between gap-3">
                        <label htmlFor="balanced-preference-weight" className="caption">Balanced split</label>
                        <span className="num text-[13px] text-ink tabular-nums">
                            {balancedPreferencePercent}% preference · {100 - balancedPreferencePercent}% fit
                        </span>
                    </div>
                    <p className="mt-2 text-[13px] leading-relaxed text-ink-muted">
                        Used only when the default order is Balanced.
                    </p>
                    <input
                        id="balanced-preference-weight"
                        type="range"
                        min="0"
                        max="100"
                        step="5"
                        value={balancedPreferencePercent}
                        onChange={(event) => handleBalancedPreferenceChange(Number(event.target.value))}
                        className="wm-slider mt-7 w-full"
                        aria-label="Balanced preference weight"
                        aria-valuemin={0}
                        aria-valuemax={100}
                        aria-valuenow={balancedPreferencePercent}
                    />
                    <div className="mt-1 flex justify-between text-[11px] text-ink-muted">
                        <span>Fit</span>
                        <span>Preference</span>
                    </div>
                </div>

                <div className="bg-surface px-5 py-5">
                    <div className="flex items-start justify-between gap-4">
                        <div>
                            <p className="caption">LLM second pass</p>
                            <p className="mt-1 text-[13px] leading-relaxed text-ink-muted">
                                Review the top matches with the configured judge.
                            </p>
                            {!policy?.llm_judge_available && (
                                <p className="mt-1 text-[12px] leading-relaxed text-warn">
                                    {String(policy?.llm_judge_unavailable_reason ?? 'unavailable').replace(/_/g, ' ')}
                                </p>
                            )}
                        </div>
                        <label className="inline-flex cursor-pointer items-center gap-2 text-[13px] text-ink">
                            <input
                                type="checkbox"
                                checked={llmJudgeEnabled}
                                disabled={!policy?.llm_judge_available}
                                onChange={(event) => handleLlmEnabledChange(event.target.checked)}
                                className="h-4 w-4 accent-accent"
                                aria-label="Enable LLM judging"
                            />
                            <span>{policy?.llm_judge_available ? 'On' : 'Unavailable'}</span>
                        </label>
                    </div>

                    <div className="mt-4 flex items-center justify-between gap-4">
                        <label className="caption" htmlFor="llm-auto-enqueue">Auto top-N</label>
                        <label className="inline-flex cursor-pointer items-center gap-2 text-[13px] text-ink">
                            <input
                                id="llm-auto-enqueue"
                                type="checkbox"
                                checked={llmJudgeAutoEnqueueEnabled}
                                disabled={!policy?.llm_judge_available}
                                onChange={(event) => handleLlmAutoEnqueueChange(event.target.checked)}
                                className="h-4 w-4 accent-accent"
                                aria-label="Automatically queue top N LLM judging"
                            />
                            <span>{llmJudgeAutoEnqueueEnabled ? 'On' : 'Off'}</span>
                        </label>
                    </div>

                    <div className="mt-4 flex items-center justify-between gap-4">
                        <label htmlFor="llm-top-n" className="caption">Top N</label>
                        <div className="inline-flex items-center border border-rule bg-surface">
                            <button
                                type="button"
                                className="h-8 w-8 text-ink-muted hover:text-ink disabled:opacity-40"
                                onClick={() => handleLlmTopNChange(llmJudgeTopN - 1)}
                                disabled={!policy?.llm_judge_available || llmJudgeTopN <= 1}
                                aria-label="Decrease LLM judge top N"
                            >
                                <Minus className="mx-auto h-3.5 w-3.5" aria-hidden="true" />
                            </button>
                            <input
                                id="llm-top-n"
                                type="number"
                                min={1}
                                max={policy?.llm_judge_top_n_max ?? 10}
                                value={llmJudgeTopN}
                                disabled={!policy?.llm_judge_available}
                                onChange={(event) => handleLlmTopNChange(Number(event.target.value))}
                                className="h-8 w-12 border-x border-rule bg-surface text-center text-[13px] text-ink outline-none"
                                aria-label="LLM judge top N"
                            />
                            <button
                                type="button"
                                className="h-8 w-8 text-ink-muted hover:text-ink disabled:opacity-40"
                                onClick={() => handleLlmTopNChange(llmJudgeTopN + 1)}
                                disabled={!policy?.llm_judge_available || llmJudgeTopN >= (policy?.llm_judge_top_n_max ?? 10)}
                                aria-label="Increase LLM judge top N"
                            >
                                <Plus className="mx-auto h-3.5 w-3.5" aria-hidden="true" />
                            </button>
                        </div>
                    </div>

                    <div className="mt-4 flex items-center justify-between gap-3">
                        <span className="text-[12px] text-ink-muted">
                            {llmApplyMessage || (llmQueueStatus.data ? `${queueBacklog} queued` : '')}
                        </span>
                        <button
                            type="button"
                            className="min-h-9 border border-rule px-3 text-[13px] text-ink transition-colors hover:border-rule-strong disabled:cursor-not-allowed disabled:opacity-50"
                            disabled={!policy?.llm_judge_available || !llmDraftDirty || Boolean(isUpdatingPolicy) || llmApplyStatus === 'saving'}
                            onClick={handleApplyLlmSettings}
                        >
                            {llmApplyStatus === 'saving' ? 'Saving' : 'Apply'}
                        </button>
                    </div>
                </div>
            </div>
        </section>
    );
}
