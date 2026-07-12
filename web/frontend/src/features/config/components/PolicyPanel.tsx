import React, { useState, useEffect, useCallback, useRef } from 'react';
import { Minus, Plus } from 'lucide-react';
import { useQuery } from '@tanstack/react-query';
import { usePolicy } from '@/hooks/usePolicy';
import { pipelineRunsApi } from '@/services/pipelineRunsApi';
import { POLICY_PRESETS, POLICY_PRESET_VALUES, RANKING_MODE_OPTIONS } from '@/utils/constants';
import type { PolicyConfig, PolicyPreset, RankingMode } from '@/types/api';

function presetForPolicy(policy: PolicyConfig): PolicyPreset | null {
    const coverage = policy.min_jd_required_coverage ?? null;
    for (const [presetKey, presetPolicy] of Object.entries(POLICY_PRESET_VALUES)) {
        if (
            policy.min_fit === presetPolicy.min_fit &&
            policy.top_k === presetPolicy.top_k &&
            coverage === presetPolicy.min_jd_required_coverage
        ) {
            return presetKey as PolicyPreset;
        }
    }
    return null;
}

export const PolicyPanel: React.FC = () => {
    const { policy, isLoading, updatePolicy, updatePolicyAsync, isUpdatingPolicy, applyPreset } = usePolicy();
    const llmQueueStatus = useQuery({
        queryKey: ['llm-evaluation-queue'],
        queryFn: async () => {
            const response = await pipelineRunsApi.getLlmEvaluationQueueStatus();
            return response.data;
        },
        enabled: Boolean(policy?.llm_judge_available),
    });
    const [minFit, setMinFit] = useState(55);
    const [topK, setTopK] = useState(50);
    const [rankingMode, setRankingMode] = useState<RankingMode>('balanced');
    const [balancedPreferencePercent, setBalancedPreferencePercent] = useState(60);
    const [llmJudgeEnabled, setLlmJudgeEnabled] = useState(false);
    const [llmJudgeAutoEnqueueEnabled, setLlmJudgeAutoEnqueueEnabled] = useState(false);
    const [llmJudgeTopN, setLlmJudgeTopN] = useState(5);
    const [llmApplyStatus, setLlmApplyStatus] = useState<'idle' | 'saving' | 'saved' | 'failed'>('idle');
    const [llmApplyMessage, setLlmApplyMessage] = useState('');
    const [preset, setPreset] = useState<PolicyPreset>('balanced');
    const hasHydratedPolicy = useRef(false);
    const hasUserAdjustedResultSettings = useRef(false);
    const hasUserAdjustedLlmSettings = useRef(false);

    useEffect(() => {
        if (policy) {
            setMinFit(policy.min_fit);
            setTopK(policy.top_k);
            setRankingMode(policy.active_default_mode ?? 'balanced');
            setBalancedPreferencePercent(Math.round((policy.balanced_w_pref ?? 0.6) * 100));
            if (!hasUserAdjustedLlmSettings.current) {
                setLlmJudgeEnabled(Boolean(policy.llm_judge_enabled));
                setLlmJudgeAutoEnqueueEnabled(Boolean(policy.llm_judge_auto_enqueue_enabled));
                setLlmJudgeTopN(policy.llm_judge_top_n ?? 5);
            }
            setPreset(presetForPolicy(policy) ?? 'balanced');
            hasHydratedPolicy.current = true;
        }
    }, [policy]);

    const autoApplyResultPolicy = useCallback(() => {
        const timeoutId = setTimeout(() => {
            updatePolicy({
                min_fit: minFit,
                top_k: topK,
                min_jd_required_coverage: policy?.min_jd_required_coverage ?? null,
                active_default_mode: rankingMode,
                balanced_w_pref: balancedPreferencePercent / 100,
                balanced_w_fit: 1 - balancedPreferencePercent / 100,
            });
        }, 250);

        return () => clearTimeout(timeoutId);
    }, [
        minFit,
        topK,
        policy?.min_jd_required_coverage,
        rankingMode,
        balancedPreferencePercent,
        updatePolicy,
    ]);

    useEffect(() => {
        if (!hasHydratedPolicy.current || !hasUserAdjustedResultSettings.current) {
            return;
        }

        const cleanup = autoApplyResultPolicy();
        return cleanup;
    }, [autoApplyResultPolicy, balancedPreferencePercent, minFit, rankingMode, topK]);

    const llmDraftDirty = Boolean(policy) && (
        Boolean(policy?.llm_judge_enabled) !== llmJudgeEnabled ||
        Boolean(policy?.llm_judge_auto_enqueue_enabled) !== llmJudgeAutoEnqueueEnabled ||
        Number(policy?.llm_judge_top_n ?? 5) !== llmJudgeTopN
    );
    const queueBacklog = Number(llmQueueStatus.data?.queued ?? 0)
        + Number(llmQueueStatus.data?.scheduled ?? 0)
        + Number(llmQueueStatus.data?.db_pending ?? 0)
        + Number(llmQueueStatus.data?.db_retryable_failed ?? 0);

    const handlePresetChange = (newPreset: PolicyPreset) => {
        const presetPolicy = POLICY_PRESET_VALUES[newPreset];
        setPreset(newPreset);
        setMinFit(presetPolicy.min_fit);
        setTopK(presetPolicy.top_k);
        hasHydratedPolicy.current = true;
        hasUserAdjustedResultSettings.current = false;
        applyPreset(newPreset);
    };

    const handleMinFitChange = (value: number) => {
        hasUserAdjustedResultSettings.current = true;
        hasHydratedPolicy.current = true;
        setMinFit(value);
        setPreset('balanced');
    };

    const handleTopKChange = (value: number) => {
        hasUserAdjustedResultSettings.current = true;
        hasHydratedPolicy.current = true;
        setTopK(value);
        setPreset('balanced');
    };

    const handleRankingModeChange = (value: RankingMode) => {
        hasUserAdjustedResultSettings.current = true;
        hasHydratedPolicy.current = true;
        setRankingMode(value);
    };

    const handleBalancedPreferenceChange = (value: number) => {
        hasUserAdjustedResultSettings.current = true;
        hasHydratedPolicy.current = true;
        setBalancedPreferencePercent(Math.max(0, Math.min(100, value)));
    };

    const handleLlmEnabledChange = (value: boolean) => {
        hasUserAdjustedLlmSettings.current = true;
        hasHydratedPolicy.current = true;
        setLlmApplyStatus('idle');
        setLlmApplyMessage('');
        setLlmJudgeEnabled(value);
        if (!value) {
            setLlmJudgeAutoEnqueueEnabled(false);
        }
        setPreset('balanced');
    };

    const handleLlmAutoEnqueueChange = (value: boolean) => {
        hasUserAdjustedLlmSettings.current = true;
        hasHydratedPolicy.current = true;
        setLlmApplyStatus('idle');
        setLlmApplyMessage('');
        setLlmJudgeAutoEnqueueEnabled(value);
        if (value) {
            setLlmJudgeEnabled(true);
        }
        setPreset('balanced');
    };

    const handleLlmTopNChange = (value: number) => {
        const maxTopN = policy?.llm_judge_top_n_max ?? 10;
        hasUserAdjustedLlmSettings.current = true;
        hasHydratedPolicy.current = true;
        setLlmApplyStatus('idle');
        setLlmApplyMessage('');
        setLlmJudgeTopN(Math.max(1, Math.min(maxTopN, value)));
        setPreset('balanced');
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
        return (
            <div className="h-64 border border-rule bg-surface xl:max-w-sidebar">
                <div className="h-full w-full animate-pulse bg-surface-sunk" />
            </div>
        );
    }

    return (
        <div className="border border-rule bg-surface xl:max-w-sidebar">
            <div className="border-b border-rule px-5 py-4">
                <p className="caption">Result policy</p>
                <h3 className="mt-1 text-[15px] font-medium text-ink">Shape your shortlist</h3>
            </div>

            <div className="space-y-6 px-5 py-5">
                <section>
                    <p className="caption mb-2">Quick presets</p>
                    <div className="grid grid-cols-1 gap-px border border-rule bg-rule">
                        {Object.entries(POLICY_PRESETS).map(([key, { label }]) => {
                            const active = preset === key;
                            return (
                                <button
                                    type="button"
                                    key={key}
                                    onClick={() => handlePresetChange(key as PolicyPreset)}
                                    className={`min-h-10 w-full px-3 py-2 text-center text-[13px] leading-tight whitespace-normal transition-colors duration-200 ${
                                        active
                                            ? 'bg-accent-soft text-accent'
                                            : 'bg-surface text-ink-soft hover:text-ink'
                                    }`}
                                    aria-pressed={active}
                                >
                                    {label}
                                </button>
                            );
                        })}
                    </div>
                </section>

                <section>
                    <div className="mb-2 flex items-baseline justify-between">
                        <label htmlFor="min-fit" className="caption">
                            Min fit score
                        </label>
                        <span className="display-numeral text-[28px] text-ink tabular-nums">
                            {minFit}
                        </span>
                    </div>
                    <input
                        id="min-fit"
                        type="range"
                        min="0"
                        max="100"
                        value={minFit}
                        onChange={(e) => handleMinFitChange(Number(e.target.value))}
                        className="wm-slider w-full"
                        aria-label="Minimum fit score"
                        aria-valuemin={0}
                        aria-valuemax={100}
                        aria-valuenow={minFit}
                    />
                    <div className="mt-1 flex justify-between text-[11px] text-ink-muted tabular-nums">
                        <span>0</span>
                        <span>100</span>
                    </div>
                </section>

                <section>
                    <div className="mb-2 flex items-baseline justify-between">
                        <label htmlFor="top-k" className="caption">
                            Max results
                        </label>
                        <span className="display-numeral text-[28px] text-ink tabular-nums">
                            {topK}
                        </span>
                    </div>
                    <input
                        id="top-k"
                        type="range"
                        min="10"
                        max="200"
                        step="10"
                        value={topK}
                        onChange={(e) => handleTopKChange(Number(e.target.value))}
                        className="wm-slider w-full"
                        aria-label="Maximum number of results"
                        aria-valuemin={10}
                        aria-valuemax={200}
                        aria-valuenow={topK}
                    />
                    <div className="mt-1 flex justify-between text-[11px] text-ink-muted tabular-nums">
                        <span>10</span>
                        <span>200</span>
                    </div>
                </section>

                <section className="border-t border-rule pt-5">
                    <p className="caption">Ranking</p>
                    <p className="mt-1 text-[13px] leading-relaxed text-ink-muted">
                        Choose the default ordering. You can still switch modes on the match list.
                    </p>

                    <label className="mt-4 block" htmlFor="ranking-default-mode">
                        <span className="caption">Default order</span>
                        <select
                            id="ranking-default-mode"
                            value={rankingMode}
                            onChange={(event) => handleRankingModeChange(event.target.value as RankingMode)}
                            className="mt-2 w-full border border-rule bg-surface px-3 py-2 text-[13px] text-ink outline-none focus:border-accent"
                        >
                            {RANKING_MODE_OPTIONS.map((option) => (
                                <option key={option.value} value={option.value}>
                                    {option.label}
                                </option>
                            ))}
                        </select>
                    </label>

                    <div className="mt-4 space-y-1 text-[12px] text-ink-muted">
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

                    <div className="mt-5">
                        <div className="mb-2 flex items-baseline justify-between gap-3">
                            <label htmlFor="balanced-preference-weight" className="caption">
                                Balanced split
                            </label>
                            <span className="num text-[13px] text-ink tabular-nums">
                                {balancedPreferencePercent}% preference · {100 - balancedPreferencePercent}% fit
                            </span>
                        </div>
                        <input
                            id="balanced-preference-weight"
                            type="range"
                            min="0"
                            max="100"
                            step="5"
                            value={balancedPreferencePercent}
                            onChange={(event) => handleBalancedPreferenceChange(Number(event.target.value))}
                            className="wm-slider w-full"
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
                </section>

                <section className="border-t border-rule pt-5">
                    <div className="flex items-start justify-between gap-4">
                        <div>
                            <p className="caption">LLM judge</p>
                            <p className="mt-1 text-[13px] leading-relaxed text-ink-muted">
                                Second-pass review for top matches.
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
                        <label className="caption" htmlFor="llm-auto-enqueue">
                            Auto top-N
                        </label>
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
                        <label htmlFor="llm-top-n" className="caption">
                            Top N
                        </label>
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
                                disabled={
                                    !policy?.llm_judge_available ||
                                    llmJudgeTopN >= (policy?.llm_judge_top_n_max ?? 10)
                                }
                                aria-label="Increase LLM judge top N"
                            >
                                <Plus className="mx-auto h-3.5 w-3.5" aria-hidden="true" />
                            </button>
                        </div>
                    </div>

                    <div className="mt-4 flex items-center justify-between gap-3">
                        <span className="text-[12px] text-ink-muted">
                            {llmApplyMessage || (
                                llmQueueStatus.data
                                    ? `${queueBacklog} queued`
                                    : ''
                            )}
                        </span>
                        <button
                            type="button"
                            className="min-h-9 border border-rule px-3 text-[13px] text-ink transition-colors hover:border-rule-strong disabled:cursor-not-allowed disabled:opacity-50"
                            disabled={
                                !policy?.llm_judge_available ||
                                !llmDraftDirty ||
                                Boolean(isUpdatingPolicy) ||
                                llmApplyStatus === 'saving'
                            }
                            onClick={handleApplyLlmSettings}
                        >
                            {llmApplyStatus === 'saving' ? 'Saving' : 'Apply'}
                        </button>
                    </div>
                </section>
            </div>
        </div>
    );
};
