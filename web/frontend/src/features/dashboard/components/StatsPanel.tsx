import React from 'react';
import { TrendingUp, Award } from 'lucide-react';
import { SegmentedCircle } from './SegmentedCircle';
import { CompactScoreBar } from './CompactScoreBar';

export interface StatsPanelProps {
    stats: {
        total_matches?: number;
        active_matches?: number;
        hidden_count?: number;
        below_threshold_count?: number;
        score_distribution?: {
            excellent?: number;
            good?: number;
            average?: number;
            poor?: number;
        };
    } | null | undefined;
    activeMatches: number;
    activeArc: number;
    hiddenArc: number;
    belowArc: number;
    circumference: number;
    radius: number;
}

export const StatsPanel: React.FC<StatsPanelProps> = ({ stats, ...chartProps }) => {
    const totalMatches = stats?.total_matches ?? 0;
    const activeMatches = stats?.active_matches ?? 0;
    const hiddenMatches = stats?.hidden_count ?? 0;
    const belowThreshold = stats?.below_threshold_count ?? 0;
    const scoreDist = stats?.score_distribution;

    return (
        <div className="flex flex-col sm:flex-row gap-6 lg:flex-1 sm:items-stretch">
            {/* Total Matches */}
            <div className="relative flex-1">
                <div className="absolute -inset-3 bg-gradient-to-r from-blue-500/20 to-indigo-500/20 rounded-2xl blur-xl" />
                <div className="relative bg-white/80 backdrop-blur-sm rounded-2xl p-5 sm:p-6 shadow-lg border border-white/50 h-full flex items-center justify-center">
                    <div className="flex items-center gap-3 sm:gap-4">
                        <div className="p-2.5 sm:p-3 bg-gradient-to-br from-blue-500 to-indigo-600 rounded-xl shadow-lg">
                            <TrendingUp className="w-6 h-6 sm:w-7 sm:h-7 text-white" aria-hidden="true" />
                        </div>
                        <div className="text-center">
                            <div className="text-4xl sm:text-5xl lg:text-6xl font-black bg-gradient-to-br from-gray-900 via-gray-800 to-gray-600 bg-clip-text text-transparent leading-none">{totalMatches}</div>
                            <div className="text-[11px] sm:text-xs font-bold text-gray-500 uppercase tracking-wider mt-1 sm:mt-1.5">Total Matches</div>
                        </div>
                    </div>
                </div>
            </div>

            {/* Segmented Circle */}
            <div className="relative flex-1">
                <div className="bg-white/60 backdrop-blur-sm rounded-2xl p-5 sm:p-6 border border-white/50 h-full flex items-center justify-center">
                    <div className="flex items-center justify-center gap-4 sm:gap-5">
                        <SegmentedCircle {...chartProps} activeMatches={activeMatches} />
                        <div className="space-y-2 sm:space-y-2.5 flex-1">
                            <div className="flex items-center gap-2">
                                <div className="w-3 h-3 sm:w-3.5 sm:h-3.5 rounded-full bg-gradient-to-r from-blue-500 to-purple-500" />
                                <span className="text-xs sm:text-sm font-bold text-gray-700">{activeMatches} Fit</span>
                            </div>
                            <div className="flex items-center gap-2">
                                <div className="w-3 h-3 sm:w-3.5 sm:h-3.5 rounded-full bg-gray-300" />
                                <span className="text-xs sm:text-sm font-bold text-gray-700">{belowThreshold} Misfit</span>
                            </div>
                            <div className="flex items-center gap-2">
                                <div className="w-3 h-3 sm:w-3.5 sm:h-3.5 rounded-full bg-gray-400" />
                                <span className="text-xs sm:text-sm font-bold text-gray-700">{hiddenMatches} Hidden</span>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            {/* Score Distribution */}
            <div className="relative flex-1 lg:flex-[1.2]">
                <div className="bg-white/60 backdrop-blur-sm rounded-2xl p-5 sm:p-6 border border-white/50 h-full flex flex-col justify-center">
                    <div className="flex items-center gap-2.5 mb-3 sm:mb-4">
                        <Award className="w-5 h-5 sm:w-6 sm:h-6 text-blue-600" aria-hidden="true" />
                        <h4 className="text-xs sm:text-sm font-black text-gray-900 uppercase tracking-wider">Score Distribution</h4>
                    </div>
                    <div className="space-y-2.5 sm:space-y-3">
                        <CompactScoreBar label="Excellent" range="80+" value={scoreDist?.excellent ?? 0} total={totalMatches} gradient="from-blue-500 to-indigo-600" />
                        <CompactScoreBar label="Good" range="60-79" value={scoreDist?.good ?? 0} total={totalMatches} gradient="from-blue-400 to-blue-500" />
                        <CompactScoreBar label="Average" range="40-59" value={scoreDist?.average ?? 0} total={totalMatches} gradient="from-gray-400 to-gray-500" />
                        <CompactScoreBar label="Poor" range="<40" value={scoreDist?.poor ?? 0} total={totalMatches} gradient="from-gray-300 to-gray-400" />
                    </div>
                </div>
            </div>
        </div>
    );
};
