import React from 'react';
import { Loader, CheckCircle, XCircle } from 'lucide-react';
import { Badge } from '@/components/ui/Badge';

export interface StatusBannerProps {
    status: string;
    step?: string;
    matches_count?: number;
    saved_count?: number;
    execution_time?: number;
    error?: string;
    stale_due_to_newer_upload?: boolean;
    stale_message?: string;
}

export const StatusBanner: React.FC<StatusBannerProps> = ({
    status,
    step,
    matches_count,
    saved_count,
    execution_time,
    error,
    stale_due_to_newer_upload,
    stale_message,
}) => {
    const isPendingStatus = status === 'pending';
    const isRunningStatus = status === 'running';
    const isCancellationRequested = status === 'cancellation_requested';
    const isPersistingStatus = status === 'persisting';
    const isCompletedStatus = status === 'completed';
    const isFailedStatus = status === 'failed';
    const isCancelledStatus = status === 'cancelled';

    const getStepLabel = (step?: string): string => {
        const labels: Record<string, string> = {
            loading_resume: 'Loading Resume',
            vector_matching: 'Finding Matches',
            scoring: 'Scoring Candidates',
            saving_results: 'Saving Results',
            notifying: 'Notifying',
            initializing: 'Initializing',
            matching: 'Finding Matches',
            extracting: 'Extracting Resume',
            embedding: 'Embedding Resume',
        };
        return step ? labels[step] || 'Processing' : 'Initializing';
    };

    const formatTime = (time?: number): string => (time ?? 0).toFixed(2);
    const isActiveStatus = isPendingStatus || isRunningStatus;
    let iconBackground = 'bg-slate-100';
    if (isActiveStatus) {
        iconBackground = 'bg-blue-100';
    } else if (isCancellationRequested) {
        iconBackground = 'bg-orange-100';
    } else if (isPersistingStatus) {
        iconBackground = 'bg-amber-100';
    } else if (isCompletedStatus) {
        iconBackground = 'bg-green-100';
    } else if (isFailedStatus) {
        iconBackground = 'bg-red-100';
    }

    let badgeVariant: 'info' | 'success' | 'error' | 'warning' | 'default' = 'default';
    if (isActiveStatus) {
        badgeVariant = 'info';
    } else if (isCancellationRequested || isPersistingStatus) {
        badgeVariant = 'warning';
    } else if (isCompletedStatus) {
        badgeVariant = 'success';
    } else if (isFailedStatus) {
        badgeVariant = 'error';
    }

    return (
        <div className="mt-6 bg-white/60 backdrop-blur-sm rounded-2xl p-6 border border-white/50">
            <div className="flex items-start gap-4">
                <div className={`p-3 rounded-xl ${iconBackground}`}>
                    {isActiveStatus && <Loader className="w-6 h-6 animate-spin text-blue-600" />}
                    {isCancellationRequested && <Loader className="w-6 h-6 animate-spin text-orange-600" />}
                    {isPersistingStatus && <Loader className="w-6 h-6 animate-spin text-amber-600" />}
                    {isCompletedStatus && <CheckCircle className="w-6 h-6 text-green-600" />}
                    {(isFailedStatus || isCancelledStatus) && (
                        <XCircle className={`w-6 h-6 ${isFailedStatus ? 'text-red-600' : 'text-slate-600'}`} />
                    )}
                </div>
                <div className="flex-1">
                    <div className="flex items-center gap-3 mb-2">
                        <Badge variant={badgeVariant}>
                            {status?.toUpperCase()}
                        </Badge>
                        {isActiveStatus && (
                            <div className="flex items-center gap-2">
                                <div className="relative w-2 h-2">
                                    <div className="absolute inset-0 bg-blue-500 rounded-full animate-ping" />
                                    <div className="relative bg-blue-600 rounded-full w-2 h-2" />
                                </div>
                                <span className="text-sm font-bold text-blue-900">{getStepLabel(step)}</span>
                            </div>
                        )}
                        {isCancellationRequested && <span className="text-sm font-bold text-orange-900">Stopping as soon as it is safe</span>}
                        {isPersistingStatus && <span className="text-sm font-bold text-amber-900">Finishing writes</span>}
                    </div>
                    {isPendingStatus && <p className="text-sm text-gray-600 mt-1">Starting your matching run...</p>}
                    {isRunningStatus && <p className="text-sm text-gray-600 mt-1">Processing your matches...</p>}
                    {isCancellationRequested && <p className="text-sm text-gray-600 mt-1">Cancellation was requested. The worker is still winding down.</p>}
                    {isPersistingStatus && <p className="text-sm text-gray-600 mt-1">The pipeline crossed the save boundary and is finishing safely.</p>}
                    {isCompletedStatus && (
                        <div>
                            <p className="font-bold text-gray-900 mb-1">Pipeline completed!</p>
                            <div className="flex gap-4 text-sm text-gray-700">
                                <span className="font-semibold">Found: {matches_count ?? 0}</span>
                                <span className="font-semibold">Saved: {saved_count ?? 0}</span>
                                <span className="font-semibold">Time: {formatTime(execution_time)}s</span>
                            </div>
                            {stale_due_to_newer_upload && stale_message && (
                                <p className="mt-3 text-sm text-amber-800 bg-amber-50 p-3 rounded-lg border border-amber-200">
                                    {stale_message}
                                </p>
                            )}
                        </div>
                    )}
                    {isFailedStatus && (
                        <div>
                            <p className="font-bold text-red-700 mb-2">Pipeline failed</p>
                            {error && (
                                <p className="text-sm text-gray-700 bg-red-50 p-3 rounded-lg border border-red-200">{error}</p>
                            )}
                        </div>
                    )}
                    {isCancelledStatus && (
                        <div>
                            <p className="font-bold text-slate-800 mb-1">Pipeline cancelled</p>
                            <p className="text-sm text-gray-600">You can start another matching run whenever you’re ready.</p>
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
};
