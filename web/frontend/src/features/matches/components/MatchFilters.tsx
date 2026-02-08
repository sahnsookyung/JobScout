import React from 'react';
import type { MatchStatus, SortBy } from '@/types/api';
import { MATCH_STATUSES, SORT_OPTIONS } from '@/utils/constants';

interface MatchFiltersProps {
    status: MatchStatus;
    onStatusChange: (status: MatchStatus) => void;
    remoteOnly: boolean;
    onRemoteOnlyChange: (value: boolean) => void;
    showWantScore: boolean;
    onShowWantScoreChange: (value: boolean) => void;
    sortBy: SortBy;
    onSortByChange: (value: SortBy) => void;
    showHidden: boolean;
    onShowHiddenChange: (value: boolean) => void;
}

export const MatchFilters: React.FC<MatchFiltersProps> = ({
    status,
    onStatusChange,
    remoteOnly,
    onRemoteOnlyChange,
    showWantScore,
    onShowWantScoreChange,
    sortBy,
    onSortByChange,
    showHidden,
    onShowHiddenChange,
}) => {
    return (
        <div className="bg-white p-4 rounded-lg border border-gray-200 shadow-sm">
            <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                {/* Status Filter */}
                <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                        Status
                    </label>
                    <select
                        value={status}
                        onChange={(e) => onStatusChange(e.target.value as MatchStatus)}
                        className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                    >
                        {MATCH_STATUSES.map((opt) => (
                            <option key={opt.value} value={opt.value}>
                                {opt.label}
                            </option>
                        ))}
                    </select>
                </div>

                {/* Sort By */}
                <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                        Sort By
                    </label>
                    <select
                        value={sortBy}
                        onChange={(e) => onSortByChange(e.target.value as SortBy)}
                        className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                    >
                        {SORT_OPTIONS.map((opt) => (
                            <option key={opt.value} value={opt.value}>
                                {opt.label}
                            </option>
                        ))}
                    </select>
                </div>

                {/* Checkboxes */}
                <div className="flex flex-col gap-2">
                    <label className="flex items-center gap-2 cursor-pointer">
                        <input
                            type="checkbox"
                            checked={remoteOnly}
                            onChange={(e) => onRemoteOnlyChange(e.target.checked)}
                            className="w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
                        />
                        <span className="text-sm text-gray-700">Remote Only</span>
                    </label>
                    <label className="flex items-center gap-2 cursor-pointer">
                        <input
                            type="checkbox"
                            checked={showWantScore}
                            onChange={(e) => onShowWantScoreChange(e.target.checked)}
                            className="w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
                        />
                        <span className="text-sm text-gray-700">Show Want Score</span>
                    </label>
                    <label className="flex items-center gap-2 cursor-pointer">
                        <input
                            type="checkbox"
                            checked={showHidden}
                            onChange={(e) => onShowHiddenChange(e.target.checked)}
                            className="w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
                        />
                        <span className="text-sm text-gray-700">Show Hidden</span>
                    </label>
                </div>
            </div>
        </div>
    );
};
