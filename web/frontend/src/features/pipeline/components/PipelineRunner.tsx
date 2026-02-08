import React from 'react';
import { Play, CheckCircle, XCircle, Loader, Zap } from 'lucide-react';
import { usePipeline } from '@/hooks/usePipeline';
import { Button } from '@/components/ui/Button';
import { Badge } from '@/components/ui/Badge';

export const PipelineRunner: React.FC = () => {
    const { runPipeline, stopPipeline, isRunning, isStopping, status, clearTask } = usePipeline();

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
        <div className="bg-white p-6 rounded-lg border border-gray-200 shadow-sm hover:shadow-md transition-shadow">
            <div className="flex items-center justify-between mb-4">
                <div className="flex items-center gap-2">
                    <div className="p-2 bg-purple-100 rounded-lg">
                        <Zap className="w-5 h-5 text-purple-600" />
                    </div>
                    <h3 className="text-lg font-semibold text-gray-900">Matching Pipeline</h3>

                </div>
                {status && status.status === 'running' ? (
                    <Button
                        onClick={() => stopPipeline()}
                        disabled={isStopping}
                        isLoading={isStopping}
                        size="sm"
                        variant="secondary"
                        className="bg-red-100 text-red-700 hover:bg-red-200"
                    >
                        Stop Matching
                    </Button>
                ) : (
                    <Button
                        onClick={() => runPipeline()}
                        disabled={isRunning}
                        isLoading={isRunning}
                        size="sm"
                        className="bg-purple-600 hover:bg-purple-700"
                    >
                        <Play className="w-4 h-4 mr-2" />
                        Run Matching
                    </Button>
                )}
            </div>

            {
                status && (
                    <div className="space-y-3 bg-gray-50 p-4 rounded-lg">
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
                )
            }
        </div >
    );
};
