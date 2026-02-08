import React from 'react';
import { Play, TrendingUp, Zap, CheckCircle, XCircle, Loader } from 'lucide-react';
import { usePipeline } from '@/hooks/usePipeline';
import { useStats } from '@/hooks/useStats';
import { Button } from '@/components/ui/Button';
import { Badge } from '@/components/ui/Badge';

export const CompactControls: React.FC = () => {
    const { runPipeline, stopPipeline, isRunning, isStopping, status, clearTask } = usePipeline();
    const { data: stats } = useStats();

    const getStatusIcon = () => {
        if (!status) return null;

        switch (status.status) {
            case 'pending':
            case 'running':
                return <Loader className="w-5 h-5 animate-spin text-blue-600" />;
            case 'completed':
                return <CheckCircle className="w-5 h-5 text-green-600" />;
            case 'failed':
                return <XCircle className="w-5 h-5 text-red-600" />;
            default:
                return null;
        }
    };

    const getStatusBadge = () => {
        if (!status) return null;

        const variants = {
            pending: 'default',
            running: 'info',
            completed: 'success',
            failed: 'error',
        } as const;

        return (
            <Badge variant={variants[status.status]} className="font-medium">
                {status.status.toUpperCase()}
            </Badge>
        );
    };

    return (
        <div className="bg-white rounded-lg border border-gray-200 shadow-sm p-4">
            <div className="flex flex-wrap items-center justify-between gap-4">
                {/* Stats Summary */}
                <div className="flex items-center gap-6">
                    <div className="flex items-center gap-2">
                        <div className="p-2 bg-blue-100 rounded-lg">
                            <TrendingUp className="w-5 h-5 text-blue-600" />
                        </div>
                        <div>
                            <div className="text-2xl font-bold text-gray-900">
                                {stats?.total_matches || 0}
                            </div>
                            <div className="text-xs text-gray-600">Total Matches</div>
                        </div>
                    </div>

                    <div className="hidden sm:block w-px h-12 bg-gray-200" />

                    <div className="hidden sm:flex items-center gap-2">
                        <div className="p-2 bg-green-100 rounded-lg">
                            <TrendingUp className="w-5 h-5 text-green-600" />
                        </div>
                        <div>
                            <div className="text-2xl font-bold text-green-900">
                                {stats?.active_matches || 0}
                            </div>
                            <div className="text-xs text-gray-600">Active</div>
                        </div>
                    </div>

                    {/* Score Distribution Pills */}
                    <div className="hidden lg:flex items-center gap-2">
                        <div className="w-px h-12 bg-gray-200" />
                        <div className="flex gap-2">
                            <div className="px-3 py-1 bg-green-100 text-green-800 rounded-full text-sm font-medium">
                                {stats?.score_distribution.excellent || 0} Excellent
                            </div>
                            <div className="px-3 py-1 bg-blue-100 text-blue-800 rounded-full text-sm font-medium">
                                {stats?.score_distribution.good || 0} Good
                            </div>
                            <div className="px-3 py-1 bg-yellow-100 text-yellow-800 rounded-full text-sm font-medium">
                                {stats?.score_distribution.average || 0} Average
                            </div>
                        </div>
                    </div>
                </div>

                {/* Pipeline Runner */}
                <div className="flex items-center gap-3">
                    {status && status.status === 'running' && (
                        <div className="flex items-center gap-2 text-sm text-blue-600">
                            <div className="animate-pulse w-2 h-2 bg-blue-600 rounded-full" />
                            <span className="font-medium">
                                {status.step === 'loading_resume' && 'Loading Resume...'}
                                {status.step === 'vector_matching' && 'Finding Potential Matches...'}
                                {status.step === 'scoring' && 'Scoring Candidates...'}
                                {status.step === 'saving_results' && 'Saving Results...'}
                                {status.step === 'notifying' && 'Sending Notifications...'}
                                {(!status.step || status.step === 'initializing') && 'Initializing...'}
                            </span>
                        </div>
                    )}

                    {status && status.status === 'completed' && (
                        <div className="flex items-center gap-2 text-sm text-green-600">
                            <div className="w-2 h-2 bg-green-600 rounded-full" />
                            <span className="font-medium">{status.saved_count} matches saved</span>
                        </div>
                    )}

                    {status && status.status === 'running' ? (
                        <Button
                            onClick={() => stopPipeline()}
                            disabled={isStopping}
                            isLoading={isStopping}
                            variant="secondary"
                            className="border-red-200 text-red-700 hover:bg-red-50 hover:text-red-800"
                        >
                            Stop
                        </Button>
                    ) : (
                        <Button
                            onClick={() => runPipeline()}
                            disabled={isRunning}
                            isLoading={isRunning}
                            className="bg-purple-600 hover:bg-purple-700"
                        >
                            <Zap className="w-4 h-4 mr-2" />
                            Run Matching
                        </Button>
                    )}
                </div>
            </div>

            {/* Detailed Status Panel */}
            {status && (
                <div className="mt-4 pt-4 border-t border-gray-200 space-y-3 bg-gray-50 p-4 rounded-lg">
                    <div className="flex items-center gap-3">
                        {getStatusIcon()}
                        {getStatusBadge()}
                    </div>

                    {status.status === 'running' && (
                        <div className="text-sm text-gray-600">
                            <div className="flex items-center gap-2 mb-2">
                                <div className="animate-pulse w-2 h-2 bg-blue-600 rounded-full" />
                                <span className="font-medium">
                                    {status.step === 'loading_resume' && 'Loading Resume...'}
                                    {status.step === 'vector_matching' && 'Finding Potential Matches...'}
                                    {status.step === 'scoring' && 'Scoring Candidates...'}
                                    {status.step === 'saving_results' && 'Saving Results...'}
                                    {status.step === 'notifying' && 'Sending Notifications...'}
                                    {(!status.step || status.step === 'initializing') && 'Pipeline Running...'}
                                </span>
                            </div>
                            <p className="text-xs text-gray-500">This may take a few minutes.</p>
                        </div>
                    )}

                    {status.status === 'completed' && (
                        <div className="text-sm space-y-2">
                            <div className="text-green-700 font-semibold flex items-center gap-2">
                                <CheckCircle className="w-4 h-4" />
                                Pipeline completed successfully!
                            </div>
                            <div className="space-y-1 text-gray-700">
                                <div className="flex justify-between">
                                    <span>Matches found:</span>
                                    <span className="font-bold">{status.matches_count || 0}</span>
                                </div>
                                <div className="flex justify-between">
                                    <span>Saved to database:</span>
                                    <span className="font-bold">{status.saved_count || 0}</span>
                                </div>
                                <div className="flex justify-between">
                                    <span>Execution time:</span>
                                    <span className="font-bold">{status.execution_time?.toFixed(2)}s</span>
                                </div>
                            </div>
                            <Button
                                variant="ghost"
                                size="sm"
                                onClick={clearTask}
                                className="w-full mt-2"
                            >
                                Clear Status
                            </Button>
                        </div>
                    )}

                    {status.status === 'failed' && (
                        <div className="text-sm space-y-2">
                            <div className="text-red-700 font-semibold flex items-center gap-2">
                                <XCircle className="w-4 h-4" />
                                Pipeline failed
                            </div>
                            {status.error && (
                                <div className="text-gray-700 bg-red-50 p-3 rounded border border-red-200 text-xs">
                                    {status.error}
                                </div>
                            )}
                            <Button
                                variant="ghost"
                                size="sm"
                                onClick={clearTask}
                                className="w-full mt-2"
                            >
                                Clear Status
                            </Button>
                        </div>
                    )}
                </div>
            )}
        </div>
    );
};
