import React, { useState, useEffect, useCallback } from 'react';
import { usePolicy } from '@/hooks/usePolicy';
import { POLICY_PRESETS } from '@/utils/constants';
import type { PolicyPreset } from '@/types/api';
import { Sliders } from 'lucide-react';

export const PolicyPanel: React.FC = () => {
    const { policy, isLoading, updatePolicy, applyPreset } = usePolicy();
    const [minFit, setMinFit] = useState(55);
    const [topK, setTopK] = useState(50);
    const [preset, setPreset] = useState<PolicyPreset>('balanced');

    useEffect(() => {
        if (policy) {
            setMinFit(policy.min_fit);
            setTopK(policy.top_k);
        }
    }, [policy]);

    // Debounced auto-apply
    const autoApplySettings = useCallback(() => {
        const timeoutId = setTimeout(() => {
            updatePolicy({
                min_fit: minFit,
                top_k: topK,
                min_jd_required_coverage: policy?.min_jd_required_coverage || null,
            });
        }, 250); // Apply after 250ms of no changes

        return () => clearTimeout(timeoutId);
    }, [minFit, topK, policy?.min_jd_required_coverage, updatePolicy]);

    useEffect(() => {
        const cleanup = autoApplySettings();
        return cleanup;
    }, [minFit, topK]);

    const handlePresetChange = (newPreset: PolicyPreset) => {
        setPreset(newPreset);
        applyPreset(newPreset);
    };

    const handleMinFitChange = (value: number) => {
        setMinFit(value);
        setPreset('balanced'); // Reset preset when manually adjusting
    };

    const handleTopKChange = (value: number) => {
        setTopK(value);
        setPreset('balanced'); // Reset preset when manually adjusting
    };

    if (isLoading) {
        return <div className="animate-pulse bg-gradient-to-br from-gray-100 to-gray-200 h-64 rounded-3xl xl:max-w-sidebar" />;
    }

    return (
        <div className="relative bg-gradient-to-br from-slate-50 via-blue-50 to-indigo-50 rounded-3xl overflow-hidden xl:max-w-sidebar">
            {/* Decorative Background */}
            <div className="absolute top-0 right-0 w-32 h-32 bg-blue-400/10 rounded-full blur-3xl" />
            <div className="absolute bottom-0 left-0 w-24 h-24 bg-indigo-400/10 rounded-full blur-3xl" />

            <div className="relative p-6">
                {/* Header */}
                <div className="flex items-center gap-2 mb-5">
                    <div className="p-2 bg-gradient-to-br from-blue-500 to-indigo-600 rounded-xl shadow-lg">
                        <Sliders className="w-5 h-5 text-white" />
                    </div>
                    <h3 className="text-lg font-black text-gray-900">Result Policy</h3>
                </div>

                {/* Presets */}
                <div className="mb-5">
                    <label className="block text-xs font-black text-gray-600 mb-3 uppercase tracking-wider">
                        Quick Presets
                    </label>
                    <div className="grid grid-cols-3 gap-2">
                        {Object.entries(POLICY_PRESETS).map(([key, { label }]) => (
                            <button
                                key={key}
                                onClick={() => handlePresetChange(key as PolicyPreset)}
                                className={`relative px-3 py-2.5 text-xs font-black rounded-xl border-2 transition-all duration-200 overflow-hidden group ${preset === key
                                    ? 'bg-gradient-to-br from-blue-500 to-indigo-600 text-white border-transparent shadow-lg scale-105'
                                    : 'bg-white/60 backdrop-blur-sm text-gray-700 border-gray-200 hover:border-blue-300 hover:scale-105 hover:shadow-md'
                                    }`}
                                aria-pressed={preset === key}
                            >
                                {preset === key && (
                                    <div className="absolute inset-0 bg-gradient-to-br from-white/20 to-transparent" />
                                )}
                                <span className="relative">{label}</span>
                            </button>
                        ))}
                    </div>
                </div>

                {/* Min Fit Slider */}
                <div className="mb-5">
                    <div className="flex justify-between items-end mb-2">
                        <label htmlFor="min-fit" className="text-xs font-black text-gray-600 uppercase tracking-wider">
                            Min Fit Score
                        </label>
                        <div className="text-right">
                            <div className="text-3xl font-black bg-gradient-to-br from-blue-600 to-indigo-600 bg-clip-text text-transparent">
                                {minFit}
                            </div>
                        </div>
                    </div>
                    <div className="relative h-3 bg-white/60 backdrop-blur-sm rounded-full border-2 border-gray-200 overflow-hidden shadow-inner">
                        <div
                            className="absolute inset-y-0 left-0 bg-gradient-to-r from-blue-500 via-indigo-500 to-purple-500 transition-all duration-300 rounded-full"
                            style={{ width: `${minFit}%` }}
                        />
                        <input
                            id="min-fit"
                            type="range"
                            min="0"
                            max="100"
                            value={minFit}
                            onChange={(e) => handleMinFitChange(Number(e.target.value))}
                            className="absolute inset-0 w-full h-full opacity-0 cursor-pointer"
                            aria-label="Minimum fit score"
                            aria-valuemin={0}
                            aria-valuemax={100}
                            aria-valuenow={minFit}
                        />
                    </div>
                    <div className="flex justify-between text-xs font-bold text-gray-500 mt-1">
                        <span>0</span>
                        <span>100</span>
                    </div>
                </div>

                {/* Top K Slider */}
                <div>
                    <div className="flex justify-between items-end mb-2">
                        <label htmlFor="top-k" className="text-xs font-black text-gray-600 uppercase tracking-wider">
                            Max Results
                        </label>
                        <div className="text-right">
                            <div className="text-3xl font-black bg-gradient-to-br from-blue-600 to-indigo-600 bg-clip-text text-transparent">
                                {topK}
                            </div>
                        </div>
                    </div>
                    <div className="relative h-3 bg-white/60 backdrop-blur-sm rounded-full border-2 border-gray-200 overflow-hidden shadow-inner">
                        <div
                            className="absolute inset-y-0 left-0 bg-gradient-to-r from-blue-500 via-indigo-500 to-purple-500 transition-all duration-300 rounded-full"
                            style={{ width: `${((topK - 10) / 190) * 100}%` }}
                        />
                        <input
                            id="top-k"
                            type="range"
                            min="10"
                            max="200"
                            step="10"
                            value={topK}
                            onChange={(e) => handleTopKChange(Number(e.target.value))}
                            className="absolute inset-0 w-full h-full opacity-0 cursor-pointer"
                            aria-label="Maximum number of results"
                            aria-valuemin={10}
                            aria-valuemax={200}
                            aria-valuenow={topK}
                        />
                    </div>
                    <div className="flex justify-between text-xs font-bold text-gray-500 mt-1">
                        <span>10</span>
                        <span>200</span>
                    </div>
                </div>
            </div>
        </div>
    );
};
