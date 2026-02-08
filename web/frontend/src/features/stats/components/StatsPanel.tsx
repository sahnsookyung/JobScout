import React from 'react';
import { useStats } from '@/hooks/useStats';
import { Badge } from '@/components/ui/Badge';
import { Target } from 'lucide-react';

export const StatsPanel: React.FC = () => {
    const { data: stats, isLoading } = useStats();

    if (isLoading) {
        return <div className="animate-pulse bg-gray-100 h-48 rounded-lg" />;
    }

    if (!stats) return null;

    return (
        <div className="bg-white p-5 rounded-lg border border-gray-200 shadow-sm">
            <div className="flex items-center gap-2 mb-4">
                <Target className="w-5 h-5 text-blue-600" />
                <h3 className="text-base font-semibold text-gray-900">Score Distribution</h3>
            </div>

            <div className="space-y-2">
                <div className="flex items-center justify-between p-3 bg-green-50 rounded-lg border border-green-200">
                    <span className="text-sm font-medium text-green-900">Excellent (80+)</span>
                    <span className="text-xl font-bold text-green-900">
                        {stats.score_distribution.excellent}
                    </span>
                </div>

                <div className="flex items-center justify-between p-3 bg-blue-50 rounded-lg border border-blue-200">
                    <span className="text-sm font-medium text-blue-900">Good (60-79)</span>
                    <span className="text-xl font-bold text-blue-900">
                        {stats.score_distribution.good}
                    </span>
                </div>

                <div className="flex items-center justify-between p-3 bg-yellow-50 rounded-lg border border-yellow-200">
                    <span className="text-sm font-medium text-yellow-900">Average (40-59)</span>
                    <span className="text-xl font-bold text-yellow-900">
                        {stats.score_distribution.average}
                    </span>
                </div>

                <div className="flex items-center justify-between p-3 bg-red-50 rounded-lg border border-red-200">
                    <span className="text-sm font-medium text-red-900">Poor (&lt;40)</span>
                    <span className="text-xl font-bold text-red-900">
                        {stats.score_distribution.poor}
                    </span>
                </div>
            </div>
        </div>
    );
};
