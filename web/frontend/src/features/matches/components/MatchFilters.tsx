// MatchFilters.tsx
import React from 'react';
import type { MatchStatus, RankingMode } from '@/types/api';
import { MATCH_STATUSES, RANKING_MODE_OPTIONS } from '@/utils/constants';
import { Filter, SortDesc, Laptop, Eye } from 'lucide-react';

interface MatchFiltersProps {
    status: MatchStatus;
    onStatusChange: (status: MatchStatus) => void;
    remoteOnly: boolean;
    onRemoteOnlyChange: (value: boolean) => void;
    rankingMode: RankingMode;
    onRankingModeChange: (value: RankingMode) => void;
    showHidden: boolean;
    onShowHiddenChange: (value: boolean) => void;
}

export const MatchFilters: React.FC<MatchFiltersProps> = ({
    status,
    onStatusChange,
    remoteOnly,
    onRemoteOnlyChange,
    rankingMode,
    onRankingModeChange,
    showHidden,
    onShowHiddenChange,
}) => {
    const statusId = 'match-filter-status';
    const rankingModeId = 'match-filter-ranking-mode';

    return (
        <div className="relative bg-gradient-to-br from-slate-50 via-blue-50 to-indigo-50 rounded-3xl overflow-hidden">
            {/* Decorative background */}
            <div className="absolute top-0 right-0 w-48 h-48 bg-blue-400/10 rounded-full blur-3xl" />

            <div className="relative p-6">
                <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                    {/* Status Filter */}
                    <div>
                        <label htmlFor={statusId} className="flex items-center gap-2 text-xs font-black text-gray-600 uppercase tracking-wider mb-3">
                            <Filter className="w-4 h-4" aria-hidden="true" />
                            Status
                        </label>
                        <select
                            id={statusId}
                            value={status}
                            onChange={(e) => onStatusChange(e.target.value as MatchStatus)}
                            className="w-full px-4 py-3 bg-white border-2 border-gray-200 rounded-xl font-semibold text-gray-900 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all shadow-sm hover:shadow-md"
                        >
                            {MATCH_STATUSES.map((opt) => (
                                <option key={opt.value} value={opt.value}>
                                    {opt.label}
                                </option>
                            ))}
                        </select>
                    </div>

                    {/* Ranking Mode */}
                    <div>
                        <label htmlFor={rankingModeId} className="flex items-center gap-2 text-xs font-black text-gray-600 uppercase tracking-wider mb-3">
                            <SortDesc className="w-4 h-4" aria-hidden="true" />
                            Ranking
                        </label>
                        <select
                            id={rankingModeId}
                            value={rankingMode}
                            onChange={(e) => onRankingModeChange(e.target.value as RankingMode)}
                            className="w-full px-4 py-3 bg-white border-2 border-gray-200 rounded-xl font-semibold text-gray-900 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all shadow-sm hover:shadow-md"
                        >
                            {RANKING_MODE_OPTIONS.map((opt) => (
                                <option key={opt.value} value={opt.value}>
                                    {opt.label}
                                </option>
                            ))}
                        </select>
                    </div>

                    {/* Toggle Options */}
                    <div>
                        <p className="text-xs font-black text-gray-600 uppercase tracking-wider mb-3 block">
                            Display Options
                        </p>
                        <div className="space-y-2">
                            <ToggleOption
                                icon={<Laptop className="w-4 h-4" />}
                                label="Remote Only"
                                checked={remoteOnly}
                                onChange={onRemoteOnlyChange}
                            />
                            <ToggleOption
                                icon={<Eye className="w-4 h-4" />}
                                label="Show Hidden"
                                checked={showHidden}
                                onChange={onShowHiddenChange}
                            />
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
};

interface ToggleOptionProps {
    icon: React.ReactNode;
    label: string;
    checked: boolean;
    onChange: (value: boolean) => void;
}

const ToggleOption: React.FC<ToggleOptionProps> = ({ icon, label, checked, onChange }) => (
    <label className="flex items-center gap-3 cursor-pointer group">
        <div className="relative">
            <input
                type="checkbox"
                checked={checked}
                onChange={(e) => onChange(e.target.checked)}
                className="sr-only"
            />
            <div className={`w-11 h-6 rounded-full transition-all duration-200 ${checked ? 'bg-gradient-to-r from-blue-500 to-indigo-500' : 'bg-gray-300'
                }`}>
                <div className={`absolute top-0.5 left-0.5 w-5 h-5 bg-white rounded-full shadow-md transition-transform duration-200 ${checked ? 'translate-x-5' : 'translate-x-0'
                    }`} />
            </div>
        </div>
        <div className="flex items-center gap-2 text-sm font-bold text-gray-700 group-hover:text-gray-900">
            {icon}
            <span>{label}</span>
        </div>
    </label>
);
