import React from 'react';

export interface ScoreBarProps {
    label: string;
    range: string;
    value: number;
    total: number;
    tone?: 'accent' | 'ink-soft' | 'ink-muted' | 'ink-faint';
}

const TONE_BG: Record<NonNullable<ScoreBarProps['tone']>, string> = {
    accent: 'bg-accent',
    'ink-soft': 'bg-ink-soft',
    'ink-muted': 'bg-ink-muted',
    'ink-faint': 'bg-ink-faint',
};

export const CompactScoreBar: React.FC<ScoreBarProps> = ({ label, range, value, total, tone = 'ink-soft' }) => {
    const percentage = total > 0 ? (value / total) * 100 : 0;
    return (
        <div>
            <div className="flex items-baseline justify-between gap-3 text-[12px]">
                <div className="flex items-baseline gap-2">
                    <span className="text-ink">{label}</span>
                    <span className="caption">{range}</span>
                </div>
                <span className="num tabular-nums text-ink-soft">{value}</span>
            </div>
            <div className="mt-1 h-px bg-rule">
                <div
                    className={`h-full ${TONE_BG[tone]} transition-[width] duration-700 ease-out`}
                    style={{ width: `${percentage}%`, height: '2px', marginTop: '-1px' }}
                />
            </div>
        </div>
    );
};
