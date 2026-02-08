import React, { useState, useEffect } from 'react';
import { usePolicy } from '@/hooks/usePolicy';
import { Button } from '@/components/ui/Button';
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

    const handlePresetChange = (newPreset: PolicyPreset) => {
        setPreset(newPreset);
        applyPreset(newPreset);
    };

    const handleCustomUpdate = () => {
        updatePolicy({
            min_fit: minFit,
            top_k: topK,
            min_jd_required_coverage: policy?.min_jd_required_coverage || null,
        });
    };

    if (isLoading) {
        return <div className="animate-pulse bg-gray-100 h-64 rounded-lg" />;
    }

    return (
        <div className="bg-white p-5 rounded-lg border border-gray-200 shadow-sm">
            <div className="flex items-center gap-2 mb-4">
                <Sliders className="w-5 h-5 text-orange-600" />
                <h3 className="text-base font-semibold text-gray-900">Result Policy</h3>
            </div>

            {/* Presets */}
            <div className="mb-5">
                <label className="block text-xs font-semibold text-gray-700 mb-2 uppercase">
                    Preset
                </label>
                <div className="grid grid-cols-3 gap-2 mb-2">
                    {Object.entries(POLICY_PRESETS).map(([key, { label }]) => (
                        <button
                            key={key}
                            onClick={() => handlePresetChange(key as PolicyPreset)}
                            className={`px-3 py-2 text-xs font-medium rounded-lg border-2 transition-all ${preset === key
                                    ? 'bg-blue-600 text-white border-blue-600'
                                    : 'bg-white text-gray-700 border-gray-300 hover:border-blue-300'
                                }`}
                        >
                            {label}
                        </button>
                    ))}
                </div>
            </div>

            {/* Min Fit Slider */}
            <div className="mb-4">
                <div className="flex justify-between items-center mb-2">
                    <label className="text-xs font-semibold text-gray-700 uppercase">
                        Min Fit Score
                    </label>
                    <span className="text-lg font-bold text-blue-600">{minFit}</span>
                </div>
                <input
                    type="range"
                    min="0"
                    max="100"
                    value={minFit}
                    onChange={(e) => setMinFit(Number(e.target.value))}
                    className="w-full h-2 bg-gray-200 rounded-lg appearance-none cursor-pointer slider-thumb"
                />
            </div>

            {/* Top K Slider */}
            <div className="mb-5">
                <div className="flex justify-between items-center mb-2">
                    <label className="text-xs font-semibold text-gray-700 uppercase">
                        Max Results
                    </label>
                    <span className="text-lg font-bold text-blue-600">{topK}</span>
                </div>
                <input
                    type="range"
                    min="10"
                    max="200"
                    step="10"
                    value={topK}
                    onChange={(e) => setTopK(Number(e.target.value))}
                    className="w-full h-2 bg-gray-200 rounded-lg appearance-none cursor-pointer slider-thumb"
                />
            </div>

            <Button
                onClick={handleCustomUpdate}
                variant="primary"
                size="sm"
                className="w-full bg-orange-600 hover:bg-orange-700"
            >
                Apply Settings
            </Button>
        </div>
    );
};
