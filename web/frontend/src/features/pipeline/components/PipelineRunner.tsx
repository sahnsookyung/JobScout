import React from 'react';
import { Play, CheckCircle, XCircle, Loader, Zap, ArrowUpRight, Sparkles } from 'lucide-react';
import { usePipeline } from '@/hooks/usePipeline';
import { Button } from '@/components/ui/Button';
import { Badge } from '@/components/ui/Badge';

export const PipelineRunner: React.FC = () => {
    const { runPipeline, stopPipeline, isRunning, isStopping, status, clearTask } = usePipeline();

    const getStepLabel = (step?: string): string => {
        const labels: Record<string, string> = {
            loading_resume: 'Loading Resume',
            vector_matching: 'Finding Potential Matches',
            scoring: 'Scoring Candidates',
            saving_results: 'Saving Results',
            notifying: 'Sending Notifications',
            initializing: 'Initializing Pipeline',
        };
        return step ? labels[step] || 'Processing' : 'Pipeline Running';
    };

    const isRunningStatus = status?.status === 'running' || status?.status === 'pending';
    const isCompletedStatus = status?.status === 'completed';
    const isFailedStatus = status?.status === 'failed';

    return (
        <div className="relative bg-gradient-to-br from-slate-50 via-purple-50 to-indigo-50 rounded-3xl overflow-hidden">
            {/* Decorative background */}
            <div className="absolute top-0 right-0 w-64 h-64 bg-purple-400/10 rounded-full blur-3xl" />
            <div className="absolute bottom-0 left-0 w-48 h-48 bg-indigo-400/10 rounded-full blur-3xl" />

            <div className="relative p-8">
                {/* Header */}
                <div className="flex items-center justify-between mb-6">
                    <div className="flex items-center gap-4">
                        <div className="relative">
                            <div className="absolute inset-0 bg-gradient-to-br from-purple-400 to-indigo-400 blur-lg opacity-50" />
                            <div className="relative p-4 bg-gradient-to-br from-purple-500 to-indigo-600 rounded-2xl shadow-xl">
                                <Zap className="w-7 h-7 text-white" aria-hidden="true" />
                            </div>
                        </div>
                        <div>
                            <h3 className="text-2xl font-black text-gray-900">Matching Pipeline</h3>
                            <p className="text-sm font-semibold text-gray-600">
                                {isRunningStatus ? 'Processing...' : 'Ready to match'}
                            </p>
                        </div>
                    </div>

                    {/* Action Button */}
                    {isRunningStatus ? (
                        <button
                            onClick={() => stopPipeline()}
                            disabled={isStopping}
                            className="group relative px-6 py-3 bg-red-500 text-white font-bold rounded-2xl shadow-lg hover:shadow-2xl hover:scale-105 active:scale-95 transition-all duration-200 disabled:opacity-50 disabled:cursor-not-allowed overflow-hidden"
                        >
                            <div className="absolute inset-0 bg-red-400 opacity-0 group-hover:opacity-100 transition-opacity duration-200" />
                            <div className="relative flex items-center gap-2">
                                {isStopping && <Loader className="w-4 h-4 animate-spin" />}
                                <span>Stop Matching</span>
                            </div>
                        </button>
                    ) : (
                        <button
                            onClick={() => runPipeline()}
                            disabled={isRunning}
                            className="group relative px-6 py-3 bg-gradient-to-r from-purple-600 to-indigo-600 text-white font-bold rounded-2xl shadow-lg hover:shadow-2xl hover:scale-105 active:scale-95 transition-all duration-200 disabled:opacity-50 disabled:cursor-not-allowed overflow-hidden"
                        >
                            <div className="absolute inset-0 bg-gradient-to-r from-purple-400 to-indigo-400 opacity-0 group-hover:opacity-100 transition-opacity duration-200" />
                            <div className="relative flex items-center gap-2">
                                {isRunning ? (
                                    <Loader className="w-5 h-5 animate-spin" />
                                ) : (
                                    <Play className="w-5 h-5" />
                                )}
                                <span>Run Matching</span>
                                {!isRunning && (
                                    <ArrowUpRight className="w-4 h-4 group-hover:translate-x-0.5 group-hover:-translate-y-0.5 transition-transform" />
                                )}
                            </div>
                        </button>
                    )}
                </div>

                {/* Status Panel */}
                {status && (
                    <div className="bg-white/60 backdrop-blur-sm rounded-2xl p-6 border border-white/50 shadow-lg">
                        {/* Running State */}
                        {isRunningStatus && (
                            <div>
                                <div className="flex items-center gap-3 mb-4">
                                    <div className="p-3 bg-blue-100 rounded-xl">
                                        <Loader className="w-6 h-6 animate-spin text-blue-600" aria-hidden="true" />
                                    </div>
                                    <Badge variant="info" className="font-bold text-sm">
                                        {status.status.toUpperCase()}
                                    </Badge>
                                </div>

                                <div className="space-y-3">
                                    <div className="flex items-center gap-3">
                                        <div className="relative w-2 h-2">
                                            <div className="absolute inset-0 bg-blue-500 rounded-full animate-ping" />
                                            <div className="relative bg-blue-600 rounded-full w-2 h-2" />
                                        </div>
                                        <span className="font-bold text-gray-900">
                                            {getStepLabel(status.step)}
                                        </span>
                                    </div>
                                    <p className="text-sm text-gray-600 ml-5">
                                        This may take a few minutes. Please wait...
                                    </p>

                                    {/* Progress bar (indeterminate) */}
                                    <div className="mt-4 h-2 bg-gray-200 rounded-full overflow-hidden">
                                        <div className="h-full bg-gradient-to-r from-blue-500 via-indigo-500 to-purple-500 rounded-full animate-pulse"
                                            style={{ width: '60%' }} />
                                    </div>
                                </div>
                            </div>
                        )}

                        {/* Completed State */}
                        {isCompletedStatus && (
                            <div>
                                <div className="flex items-center gap-3 mb-4">
                                    <div className="relative">
                                        <div className="absolute inset-0 bg-green-400 blur-lg opacity-50 animate-pulse" />
                                        <div className="relative p-3 bg-green-100 rounded-xl">
                                            <CheckCircle className="w-6 h-6 text-green-600" aria-hidden="true" />
                                        </div>
                                    </div>
                                    <div>
                                        <Badge variant="success" className="font-bold text-sm mb-1">
                                            COMPLETED
                                        </Badge>
                                        <p className="text-sm font-bold text-green-700 flex items-center gap-2">
                                            <Sparkles className="w-4 h-4" aria-hidden="true" />
                                            Pipeline completed successfully!
                                        </p>
                                    </div>
                                </div>

                                <div className="grid grid-cols-3 gap-4 mb-4">
                                    <div className="bg-gradient-to-br from-blue-50 to-indigo-50 p-4 rounded-xl border border-blue-200">
                                        <div className="text-xs font-bold text-gray-600 uppercase tracking-wider mb-1">
                                            Matches Found
                                        </div>
                                        <div className="text-3xl font-black text-gray-900">
                                            {status.matches_count || 0}
                                        </div>
                                    </div>

                                    <div className="bg-gradient-to-br from-blue-50 to-indigo-50 p-4 rounded-xl border border-blue-200">
                                        <div className="text-xs font-bold text-gray-600 uppercase tracking-wider mb-1">
                                            Saved
                                        </div>
                                        <div className="text-3xl font-black text-gray-900">
                                            {status.saved_count || 0}
                                        </div>
                                    </div>

                                    <div className="bg-gradient-to-br from-blue-50 to-indigo-50 p-4 rounded-xl border border-blue-200">
                                        <div className="text-xs font-bold text-gray-600 uppercase tracking-wider mb-1">
                                            Time
                                        </div>
                                        <div className="text-3xl font-black text-gray-900">
                                            {status.execution_time?.toFixed(1)}s
                                        </div>
                                    </div>
                                </div>

                                <button
                                    onClick={() => clearTask()}
                                    className="w-full px-4 py-3 bg-white border-2 border-gray-200 text-gray-700 font-bold rounded-xl hover:bg-gray-50 hover:border-gray-300 transition-all"
                                >
                                    Clear Status
                                </button>
                            </div>
                        )}

                        {/* Failed State */}
                        {isFailedStatus && (
                            <div>
                                <div className="flex items-center gap-3 mb-4">
                                    <div className="p-3 bg-red-100 rounded-xl">
                                        <XCircle className="w-6 h-6 text-red-600" aria-hidden="true" />
                                    </div>
                                    <div>
                                        <Badge variant="error" className="font-bold text-sm mb-1">
                                            FAILED
                                        </Badge>
                                        <p className="text-sm font-bold text-red-700">
                                            Pipeline execution failed
                                        </p>
                                    </div>
                                </div>

                                {status.error && (
                                    <div className="mb-4 p-4 bg-red-50 border-2 border-red-200 rounded-xl">
                                        <div className="text-xs font-bold text-red-600 uppercase tracking-wider mb-2">
                                            Error Details
                                        </div>
                                        <div className="text-sm text-gray-700 font-medium">
                                            {status.error}
                                        </div>
                                    </div>
                                )}

                                <button
                                    onClick={() => clearTask()}
                                    className="w-full px-4 py-3 bg-white border-2 border-gray-200 text-gray-700 font-bold rounded-xl hover:bg-gray-50 hover:border-gray-300 transition-all"
                                >
                                    Clear Status
                                </button>
                            </div>
                        )}
                    </div>
                )}

                {/* No Status - Ready State */}
                {!status && (
                    <div className="bg-white/40 backdrop-blur-sm rounded-2xl p-6 border border-white/50">
                        <p className="text-center text-gray-600 font-semibold">
                            Ready to start matching. Click "Run Matching" to begin.
                        </p>
                    </div>
                )}
            </div>
        </div>
    );
};
