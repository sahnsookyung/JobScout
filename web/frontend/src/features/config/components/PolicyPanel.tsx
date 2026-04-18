import React, { useState, useEffect, useCallback, useRef } from 'react';
import { usePolicy } from '@/hooks/usePolicy';
import { POLICY_PRESETS } from '@/utils/constants';
import type { PolicyPreset } from '@/types/api';

export const PolicyPanel: React.FC = () => {
    const { policy, isLoading, updatePolicy, applyPreset } = usePolicy();
    const [minFit, setMinFit] = useState(55);
    const [topK, setTopK] = useState(50);
    const [preset, setPreset] = useState<PolicyPreset>('balanced');
    const hasHydratedPolicy = useRef(false);
    const hasUserAdjustedSettings = useRef(false);

    useEffect(() => {
        if (policy) {
            setMinFit(policy.min_fit);
            setTopK(policy.top_k);
            hasHydratedPolicy.current = true;
        }
    }, [policy]);

    const autoApplySettings = useCallback(() => {
        const timeoutId = setTimeout(() => {
            updatePolicy({
                min_fit: minFit,
                top_k: topK,
                min_jd_required_coverage: policy?.min_jd_required_coverage || null,
            });
        }, 250);

        return () => clearTimeout(timeoutId);
    }, [minFit, topK, policy?.min_jd_required_coverage, updatePolicy]);

    useEffect(() => {
        if (!hasHydratedPolicy.current || !hasUserAdjustedSettings.current) {
            return;
        }

        const cleanup = autoApplySettings();
        return cleanup;
    }, [autoApplySettings, minFit, topK]);

    const handlePresetChange = (newPreset: PolicyPreset) => {
        setPreset(newPreset);
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
            </div>
        </div>
    );
};
