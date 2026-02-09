// StatsPanel.tsx
import React from 'react';
import { useStats } from '@/hooks/useStats';
import { Award } from 'lucide-react';

export const StatsPanel: React.FC = () => {
    const { data: stats, isLoading } = useStats();

    if (isLoading) {
        return <div className="animate-pulse bg-gradient-to-br from-gray-100 to-gray-200 h-80 rounded-3xl" />;
    }

    if (!stats) return null;

    const totalMatches = stats.total_matches;

    return (
        <div className="relative bg-gradient-to-br from-slate-50 via-blue-50 to-indigo-50 rounded-3xl overflow-hidden">
            {/* Decorative background */}
            <div className="absolute top-0 right-0 w-48 h-48 bg-blue-400/10 rounded-full blur-3xl" />
            <div className="absolute bottom-0 left-0 w-32 h-32 bg-indigo-400/10 rounded-full blur-3xl" />

            <div className="relative p-8">
                {/* Score Distribution */}
                <div>
                    <div className="flex items-center gap-3 mb-6">
                        <div className="p-3 bg-gradient-to-br from-blue-500 to-indigo-600 rounded-xl shadow-lg">
                            <Award className="w-6 h-6 text-white" aria-hidden="true" />
                        </div>
                        <h3 className="text-xl font-black text-gray-900">Score Distribution</h3>
                    </div>

                    <div className="space-y-4">
                        <ScoreDistributionBar
                            label="Excellent"
                            range="80+"
                            value={stats.score_distribution?.excellent ?? 0}
                            total={totalMatches}
                            gradient="from-blue-500 to-indigo-600"
                        />
                        <ScoreDistributionBar
                            label="Good"
                            range="60-79"
                            value={stats.score_distribution?.good ?? 0}
                            total={totalMatches}
                            gradient="from-blue-400 to-blue-500"
                        />
                        <ScoreDistributionBar
                            label="Average"
                            range="40-59"
                            value={stats.score_distribution?.average ?? 0}
                            total={totalMatches}
                            gradient="from-gray-400 to-gray-500"
                        />
                        <ScoreDistributionBar
                            label="Poor"
                            range="<40"
                            value={stats.score_distribution?.poor ?? 0}
                            total={totalMatches}
                            gradient="from-gray-300 to-gray-400"
                        />
                    </div>
                </div>
            </div>
        </div>
    );
};

interface ScoreDistributionBarProps {
    label: string;
    range: string;
    value: number;
    total: number;
    gradient: string;
}

const ScoreDistributionBar: React.FC<ScoreDistributionBarProps> = ({ label, range, value, total, gradient }) => {
    const percentage = total > 0 ? (value / total) * 100 : 0;

    return (
        <div className="bg-white/60 backdrop-blur-sm rounded-xl p-4 border border-gray-200 hover:border-gray-300 transition-all">
            <div className="flex items-center justify-between mb-2">
                <div className="flex items-center gap-3">
                    <div className={`w-2 h-2 rounded-full bg-gradient-to-r ${gradient}`} />
                    <span className="font-black text-gray-900">{label}</span>
                    <span className="text-xs text-gray-500 font-semibold">({range})</span>
                </div>
                <div className="flex items-baseline gap-2">
                    <span className="text-2xl font-black text-gray-900">{value}</span>
                    {percentage > 0 && (
                        <span className="text-xs font-bold text-gray-500">{percentage.toFixed(0)}%</span>
                    )}
                </div>
            </div>
            <div className="h-1.5 bg-gray-200 rounded-full overflow-hidden">
                <div
                    className={`h-full bg-gradient-to-r ${gradient} transition-all duration-500 ease-out rounded-full`}
                    style={{ width: `${percentage}%` }}
                />
            </div>
        </div>
    );
};
