import React from 'react';

export interface CircleChartProps {
    activeMatches: number;
    activeArc: number;
    hiddenArc: number;
    belowArc: number;
    circumference: number;
    radius: number;
}

export const SegmentedCircle: React.FC<CircleChartProps> = ({
    activeMatches, activeArc, hiddenArc, belowArc, circumference, radius,
}) => (
    <div className="relative h-24 w-24 flex-shrink-0 sm:h-28 sm:w-28">
        <svg className="h-full w-full -rotate-90" viewBox="0 0 96 96">
            <circle
                cx="48"
                cy="48"
                r={radius}
                stroke="var(--rule)"
                strokeWidth="4"
                fill="none"
            />
            <circle
                cx="48"
                cy="48"
                r={radius}
                stroke="var(--accent)"
                strokeWidth="4"
                fill="none"
                strokeLinecap="butt"
                className="transition-[stroke-dasharray] duration-700 ease-out"
                style={{ strokeDasharray: `${activeArc} ${circumference - activeArc}` }}
            />
            <circle
                cx="48"
                cy="48"
                r={radius}
                stroke="var(--ink-muted)"
                strokeWidth="4"
                fill="none"
                strokeDashoffset={-activeArc}
                strokeLinecap="butt"
                className="transition-[stroke-dasharray] duration-700 ease-out"
                style={{ strokeDasharray: `${hiddenArc} ${circumference - hiddenArc}` }}
            />
            <circle
                cx="48"
                cy="48"
                r={radius}
                stroke="var(--ink-faint)"
                strokeWidth="4"
                fill="none"
                strokeDashoffset={-(activeArc + hiddenArc)}
                strokeLinecap="butt"
                className="transition-[stroke-dasharray] duration-700 ease-out"
                style={{ strokeDasharray: `${belowArc} ${circumference - belowArc}` }}
            />
        </svg>
        <div className="absolute inset-0 flex items-center justify-center">
            <div className="text-center">
                <div className="display-numeral text-[24px] sm:text-[28px]">{activeMatches}</div>
                <div className="caption mt-0.5">fit</div>
            </div>
        </div>
    </div>
);
