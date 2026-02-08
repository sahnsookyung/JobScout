import React from 'react';
import { TrendingUp, Zap, CheckCircle, XCircle, Loader } from 'lucide-react';
import { usePipeline } from '@/hooks/usePipeline';
import { useStats } from '@/hooks/useStats';
import { Button } from '@/components/ui/Button';
import { Badge } from '@/components/ui/Badge';

export const CompactControls: React.FC = () => {
    const { runPipeline, stopPipeline, isRunning, isStopping, status } = usePipeline();
    const { data: stats } = useStats();

    const statusData = status as {
        status: string;
        step?: string;
        saved_count?: number;
        matches_count?: number;
        execution_time?: number;
        error?: string;
    } | null | undefined;

    const hasStatus = statusData !== null && statusData !== undefined;
    const isRunningStatus = hasStatus && statusData?.status === 'running';
    const isCompletedStatus = hasStatus && statusData?.status === 'completed';
    const isFailedStatus = hasStatus && statusData?.status === 'failed';

    const getStepLabel = (step?: string): string => {
        const labels: Record<string, string> = {
            loading_resume: 'Loading Resume...',
            vector_matching: 'Finding Potential Matches...',
            scoring: 'Scoring Candidates...',
            saving_results: 'Saving Results...',
            notifying: 'Sending Notifications...',
            initializing: 'Initializing...',
        };
        return step ? labels[step] || 'Processing...' : 'Initializing...';
    };

    const formatTime = (time?: number): string => (time ?? 0).toFixed(2);

    if (!hasStatus) {
        return (
            <div className="bg-white rounded-lg border border-gray-200 shadow-sm p-4">
                <div className="flex flex-wrap items-center justify-between gap-4">
                    <div className="flex items-center gap-6">
                        <StatItem
                            icon={<TrendingUp className="w-5 h-5 text-blue-600" />}
                            value={stats?.total_matches ?? 0}
                            label="Total Matches"
                            valueColor="text-gray-900"
                        />
                        <div className="hidden sm:block w-px h-12 bg-gray-200" />
                        <div className="hidden sm:flex items-center gap-2">
                            <StatItem
                                icon={<TrendingUp className="w-5 h-5 text-green-600" />}
                                value={stats?.active_matches ?? 0}
                                label="Active"
                                valueColor="text-green-900"
                            />
                        </div>
                    </div>
                    <Button onClick={() => runPipeline()} disabled={isRunning} isLoading={isRunning} className="bg-purple-600 hover:bg-purple-700">
                        <Zap className="w-4 h-4 mr-2" />
                        Run Matching
                    </Button>
                </div>
            </div>
        );
    }

    return (
        <div className="bg-white rounded-lg border border-gray-200 shadow-sm p-4">
            <div className="flex flex-wrap items-center justify-between gap-4">
                <div className="flex items-center gap-6">
                    <StatItem
                        icon={<TrendingUp className="w-5 h-5 text-blue-600" />}
                        value={stats?.total_matches ?? 0}
                        label="Total Matches"
                        valueColor="text-gray-900"
                    />
                    <div className="hidden sm:block w-px h-12 bg-gray-200" />
                    <div className="hidden sm:flex items-center gap-2">
                        <StatItem
                            icon={<TrendingUp className="w-5 h-5 text-green-600" />}
                            value={stats?.active_matches ?? 0}
                            label="Active"
                            valueColor="text-green-900"
                        />
                    </div>
                </div>

                <div className="flex items-center gap-3">
                    {isRunningStatus && (
                        <div className="flex items-center gap-2 text-sm text-blue-600">
                            <div className="animate-pulse w-2 h-2 bg-blue-600 rounded-full" />
                            <span className="font-medium">{getStepLabel(statusData?.step)}</span>
                        </div>
                    )}
                    {isCompletedStatus && (
                        <span className="text-sm text-green-600 font-medium">
                            {statusData?.saved_count ?? 0} matches saved
                        </span>
                    )}
                    <Button
                        onClick={isRunningStatus ? () => stopPipeline() : () => runPipeline()}
                        disabled={isRunningStatus ? isStopping : isRunning}
                        isLoading={isRunningStatus ? isStopping : isRunning}
                        variant={isRunningStatus ? 'secondary' : undefined}
                        className={isRunningStatus ? 'border-red-200 text-red-700 hover:bg-red-50' : 'bg-purple-600 hover:bg-purple-700'}
                    >
                        {isRunningStatus ? 'Stop' : 'Run Matching'}
                    </Button>
                </div>
            </div>

            <div className="mt-4 pt-4 border-t border-gray-200 bg-gray-50 p-4 rounded-lg">
                <div className="flex items-center gap-3 mb-3">
                    {statusData?.status === 'running' && <Loader className="w-5 h-5 animate-spin text-blue-600" />}
                    {statusData?.status === 'completed' && <CheckCircle className="w-5 h-5 text-green-600" />}
                    {statusData?.status === 'failed' && <XCircle className="w-5 h-5 text-red-600" />}
                    <Badge variant={statusData?.status === 'running' ? 'info' : statusData?.status === 'completed' ? 'success' : statusData?.status === 'failed' ? 'error' : 'default'}>
                        {statusData?.status?.toUpperCase() ?? 'UNKNOWN'}
                    </Badge>
                </div>

                {isRunningStatus && (
                    <div className="text-sm text-gray-600">
                        <p className="font-medium">{getStepLabel(statusData?.step)}</p>
                        <p className="text-xs text-gray-500 mt-1">This may take a few minutes.</p>
                    </div>
                )}

                {isCompletedStatus && (
                    <div className="text-sm space-y-1">
                        <p className="text-green-700 font-semibold">Pipeline completed successfully!</p>
                        <p className="text-gray-700">Found: {statusData?.matches_count ?? 0} | Saved: {statusData?.saved_count ?? 0} | Time: {formatTime(statusData?.execution_time)}s</p>
                    </div>
                )}

                {isFailedStatus && (
                    <div className="text-sm space-y-2">
                        <p className="text-red-700 font-semibold">Pipeline failed</p>
                        {statusData?.error && <p className="text-gray-700 bg-red-50 p-2 rounded text-xs">{statusData.error}</p>}
                    </div>
                )}
            </div>
        </div>
    );
};

const StatItem: React.FC<{ icon: React.ReactNode; value: number; label: string; valueColor: string }> = ({
    icon, value, label, valueColor
}) => (
    <div className="flex items-center gap-2">
        <div className="p-2 bg-blue-100 rounded-lg">{icon}</div>
        <div>
            <div className={`text-2xl font-bold ${valueColor}`}>{value}</div>
            <div className="text-xs text-gray-600">{label}</div>
        </div>
    </div>
);
