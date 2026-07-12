import React from 'react';
import type { MatchStatus, RankingMode } from '@/types/api';
import { MATCH_STATUSES, RANKING_MODE_OPTIONS } from '@/utils/constants';

interface MatchFiltersProps {
    status: MatchStatus;
    onStatusChange: (status: MatchStatus) => void;
    remoteOnly: boolean;
    onRemoteOnlyChange: (value: boolean) => void;
    rankingMode: RankingMode;
    onRankingModeChange: (value: RankingMode) => void;
    showHidden: boolean;
    onShowHiddenChange: (value: boolean) => void;
    llmOrdering: boolean;
    onLlmOrderingChange: (value: boolean) => void;
    showAllProcessed?: boolean;
    onShowAllProcessedChange?: (value: boolean) => void;
    processedCount?: number;
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
    llmOrdering,
    onLlmOrderingChange,
    showAllProcessed = false,
    onShowAllProcessedChange = () => undefined,
    processedCount = 0,
}) => {
    return (
        <div className="grid grid-cols-1 gap-5 border-b border-rule pb-5 md:grid-cols-[minmax(9rem,14rem)_minmax(9rem,14rem)_minmax(0,1fr)] md:items-end md:gap-8">
            <SelectField
                id="match-filter-status"
                label="Status"
                value={status}
                onChange={(value) => onStatusChange(value as MatchStatus)}
                options={MATCH_STATUSES}
            />
            <SelectField
                id="match-filter-ranking-mode"
                label="Ranking"
                value={rankingMode}
                onChange={(value) => onRankingModeChange(value as RankingMode)}
                options={RANKING_MODE_OPTIONS}
            />
            <div className="flex min-w-0 flex-wrap items-center gap-x-5 gap-y-2 md:justify-end">
                <Toggle
                    label="Judge boost"
                    checked={llmOrdering}
                    onChange={onLlmOrderingChange}
                />
                <Toggle
                    label="Remote only"
                    checked={remoteOnly}
                    onChange={onRemoteOnlyChange}
                />
                <Toggle
                    label="Hidden"
                    checked={showHidden}
                    onChange={onShowHiddenChange}
                />
                {processedCount > 0 && (
                    <Toggle
                        label={`All matched candidates (${processedCount})`}
                        checked={showAllProcessed}
                        onChange={onShowAllProcessedChange}
                    />
                )}
            </div>
        </div>
    );
};

interface SelectFieldProps {
    id: string;
    label: string;
    value: string;
    onChange: (value: string) => void;
    options: readonly { value: string; label: string }[];
}

const SelectField: React.FC<SelectFieldProps> = ({ id, label, value, onChange, options }) => (
    <label htmlFor={id} className="block">
        <span className="caption mb-1.5 block">{label}</span>
        <select
            id={id}
            value={value}
            onChange={(e) => onChange(e.target.value)}
            className="w-full appearance-none border-0 border-b border-rule bg-transparent py-1.5 pr-6 text-[14px] text-ink focus-visible:outline-none focus-visible:border-accent"
            style={{
                backgroundImage:
                    "url(\"data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='10' height='6' viewBox='0 0 10 6'%3E%3Cpath d='M1 1l4 4 4-4' stroke='%238A7F6E' stroke-width='1.2' fill='none'/%3E%3C/svg%3E\")",
                backgroundRepeat: 'no-repeat',
                backgroundPosition: 'right 2px center',
            }}
        >
            {options.map((opt) => (
                <option key={opt.value} value={opt.value}>
                    {opt.label}
                </option>
            ))}
        </select>
    </label>
);

interface ToggleProps {
    label: string;
    checked: boolean;
    onChange: (value: boolean) => void;
}

const Toggle: React.FC<ToggleProps> = ({ label, checked, onChange }) => (
    <label className="inline-flex min-w-0 cursor-pointer items-center gap-2 text-[13px] text-ink-soft">
        <span className="relative">
            <input
                type="checkbox"
                checked={checked}
                onChange={(e) => onChange(e.target.checked)}
                className="sr-only"
            />
            <span
                className={`block h-4 w-7 rounded-full border transition-colors duration-200 ${
                    checked ? 'border-accent bg-accent' : 'border-rule-strong bg-surface-sunk'
                }`}
            >
                <span
                    className={`absolute top-[3px] block h-2.5 w-2.5 rounded-full transition-transform duration-200 ${
                        checked ? 'translate-x-[14px] bg-white' : 'translate-x-[3px] bg-ink-muted'
                    }`}
                />
            </span>
        </span>
        <span className={`min-w-0 ${checked ? 'text-ink' : ''}`}>{label}</span>
    </label>
);
