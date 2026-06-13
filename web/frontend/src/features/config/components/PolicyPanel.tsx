import React, { useState, useEffect, useCallback, useRef } from 'react';
import { Minus, Plus } from 'lucide-react';
import { usePolicy } from '@/hooks/usePolicy';
import { POLICY_PRESETS, POLICY_PRESET_VALUES } from '@/utils/constants';
import type { PolicyConfig, PolicyPreset } from '@/types/api';

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
    const { policy, isLoading, updatePolicy, applyPreset } = usePolicy();
    const [minFit, setMinFit] = useState(55);
    const [topK, setTopK] = useState(50);
    const [llmJudgeEnabled, setLlmJudgeEnabled] = useState(false);
    const [llmJudgeTopN, setLlmJudgeTopN] = useState(5);
    const [preset, setPreset] = useState<PolicyPreset>('balanced');
    const hasHydratedPolicy = useRef(false);
    const hasUserAdjustedSettings = useRef(false);

    useEffect(() => {
        if (policy) {
            setMinFit(policy.min_fit);
            setTopK(policy.top_k);
            setLlmJudgeEnabled(Boolean(policy.llm_judge_enabled));
            setLlmJudgeTopN(policy.llm_judge_top_n ?? 5);
            setPreset(presetForPolicy(policy) ?? 'balanced');
            hasHydratedPolicy.current = true;
        }
    }, [policy]);

    const autoApplySettings = useCallback(() => {
        const timeoutId = setTimeout(() => {
            updatePolicy({
                min_fit: minFit,
                top_k: topK,
                min_jd_required_coverage: policy?.min_jd_required_coverage ?? null,
                llm_judge_enabled: llmJudgeEnabled,
                llm_judge_top_n: llmJudgeTopN,
            });
        }, 250);

        return () => clearTimeout(timeoutId);
    }, [
        minFit,
        topK,
        llmJudgeEnabled,
        llmJudgeTopN,
        policy?.min_jd_required_coverage,
        updatePolicy,
    ]);

    useEffect(() => {
        if (!hasHydratedPolicy.current || !hasUserAdjustedSettings.current) {
            return;
        }

        const cleanup = autoApplySettings();
        return cleanup;
    }, [autoApplySettings, minFit, topK]);

    const handlePresetChange = (newPreset: PolicyPreset) => {
        const presetPolicy = POLICY_PRESET_VALUES[newPreset];
        setPreset(newPreset);
        setMinFit(presetPolicy.min_fit);
        setTopK(presetPolicy.top_k);
        hasHydratedPolicy.current = true;
        hasUserAdjustedSettings.current = false;
        applyPreset(newPreset);
    };

    const handleMinFitChange = (value: number) => {
        hasUserAdjustedSettings.current = true;
        hasHydratedPolicy.current = true;
        setMinFit(value);
        setPreset('balanced');
    };

    const handleTopKChange = (value: number) => {
        hasUserAdjustedSettings.current = true;
        hasHydratedPolicy.current = true;
        setTopK(value);
        setPreset('balanced');
    };

    const handleLlmEnabledChange = (value: boolean) => {
        hasUserAdjustedSettings.current = true;
        hasHydratedPolicy.current = true;
        setLlmJudgeEnabled(value);
        setPreset('balanced');
    };

    const handleLlmTopNChange = (value: number) => {
        const maxTopN = policy?.llm_judge_top_n_max ?? 10;
        hasUserAdjustedSettings.current = true;
        hasHydratedPolicy.current = true;
        setLlmJudgeTopN(Math.max(1, Math.min(maxTopN, value)));
        setPreset('balanced');
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
                    <div className="grid grid-cols-3 gap-px overflow-hidden border border-rule">
                        {Object.entries(POLICY_PRESETS).map(([key, { label }]) => {
                            const active = preset === key;
                            return (
                                <button
                                    key={key}
                                    onClick={() => handlePresetChange(key as PolicyPreset)}
                                    className={`px-3 py-2 text-[13px] transition-colors duration-200 ${
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
                </section>
            </div>
        </div>
    );
};
