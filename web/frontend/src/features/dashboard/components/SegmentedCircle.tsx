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
    activeMatches, activeArc, hiddenArc, belowArc, circumference, radius
}) => (
    <div className="relative w-28 h-28 sm:w-32 sm:h-32 lg:w-36 lg:h-36 flex-shrink-0">
        <svg className="transform -rotate-90 w-full h-full" viewBox="0 0 96 96">
            <circle cx="48" cy="48" r={radius} stroke="currentColor" strokeWidth="8" fill="none" className="text-gray-200" />
            <circle cx="48" cy="48" r={radius} stroke="url(#gradient-active)" strokeWidth="8" fill="none" strokeDasharray={circumference} strokeDashoffset={0} className="transition-all duration-1000 ease-out" strokeLinecap="round" style={{ strokeDasharray: `${activeArc} ${circumference - activeArc}` }} />
            <circle cx="48" cy="48" r={radius} stroke="#9ca3af" strokeWidth="8" fill="none" strokeDasharray={circumference} strokeDashoffset={-activeArc} className="transition-all duration-1000 ease-out" strokeLinecap="round" style={{ strokeDasharray: `${hiddenArc} ${circumference - hiddenArc}` }} />
            <circle cx="48" cy="48" r={radius} stroke="#d1d5db" strokeWidth="8" fill="none" strokeDasharray={circumference} strokeDashoffset={-(activeArc + hiddenArc)} className="transition-all duration-1000 ease-out" strokeLinecap="round" style={{ strokeDasharray: `${belowArc} ${circumference - belowArc}` }} />
            <defs>
                <linearGradient id="gradient-active" x1="0%" y1="0%" x2="100%" y2="100%">
                    <stop offset="0%" stopColor="#3b82f6" />
                    <stop offset="100%" stopColor="#8b5cf6" />
                </linearGradient>
            </defs>
        </svg>
        <div className="absolute inset-0 flex items-center justify-center">
            <div className="text-center">
                <div className="text-3xl sm:text-4xl font-black text-gray-800">{activeMatches}</div>
                <div className="text-[10px] sm:text-xs font-bold text-gray-500 uppercase">Fits</div>
            </div>
        </div>
    </div>
);
