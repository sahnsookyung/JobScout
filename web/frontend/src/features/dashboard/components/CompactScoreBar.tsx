import React from 'react';

export interface ScoreBarProps {
    label: string;
    range: string;
    value: number;
    total: number;
    gradient: string;
}

export const CompactScoreBar: React.FC<ScoreBarProps> = ({ label, range, value, total, gradient }) => {
    const percentage = total > 0 ? (value / total) * 100 : 0;
    return (
        <div>
            <div className="flex items-center justify-between mb-1.5">
                <div className="flex items-center gap-2">
                    <div className={`w-2 h-2 sm:w-2.5 sm:h-2.5 rounded-full bg-gradient-to-r ${gradient}`} />
                    <span className="text-xs sm:text-sm font-black text-gray-900">{label}</span>
                    <span className="text-[10px] sm:text-xs text-gray-500 font-semibold">({range})</span>
                </div>
                <span className="text-sm sm:text-base font-black text-gray-900">{value}</span>
            </div>
            <div className="h-1.5 sm:h-2 bg-gray-200 rounded-full overflow-hidden">
                <div className={`h-full bg-gradient-to-r ${gradient} transition-all duration-500 ease-out rounded-full`} style={{ width: `${percentage}%` }} />
            </div>
        </div>
    );
};
