import React from 'react';
import { Zap } from 'lucide-react';

export interface ActionButtonProps {
    canStop: boolean;
    isCancellationRequested: boolean;
    isPersistingStatus: boolean;
    isRunning: boolean;
    isStopping: boolean;
    onRun: () => void;
    onStop: () => void;
}

export const ActionButton: React.FC<ActionButtonProps> = ({
    canStop,
    isCancellationRequested,
    isPersistingStatus,
    isRunning,
    isStopping,
    onRun,
    onStop,
}) => {
    const isProcessing = canStop ? isStopping : isRunning;
    let buttonText = 'Run Matching';
    if (isPersistingStatus) {
        buttonText = 'Finishing...';
    } else if (isCancellationRequested) {
        buttonText = 'Stopping...';
    } else if (canStop) {
        buttonText = 'Stop';
    }

    return (
        <button
            onClick={canStop ? onStop : onRun}
            disabled={isProcessing || isCancellationRequested || isPersistingStatus}
            // Standardized to px-6 py-4, font-semibold, rounded-xl, and flex-col to match Upload button exactly
            className={`w-full lg:w-auto group relative px-6 py-4 font-semibold rounded-xl shadow-lg hover:shadow-2xl hover:scale-105 active:scale-95 transition-all duration-200 disabled:opacity-50 overflow-hidden flex flex-col items-center justify-center ${canStop || isCancellationRequested ? 'bg-red-500 text-white hover:bg-red-600' : isPersistingStatus ? 'bg-amber-500 text-white hover:bg-amber-600' : 'bg-gradient-to-r from-blue-600 to-indigo-600 text-white'
                }`}
        >
            <div className={`absolute inset-0 opacity-0 group-hover:opacity-100 transition-opacity duration-200 pointer-events-none ${canStop || isCancellationRequested ? 'bg-red-400' : isPersistingStatus ? 'bg-amber-400' : 'bg-gradient-to-r from-blue-400 to-indigo-400'
                }`} />
            <span className="relative flex items-center justify-center gap-2 text-base">
                {/* Matched sizing to FileUp and added shrink-0 */}
                {!canStop && !isCancellationRequested && !isPersistingStatus && <Zap className="w-5 h-5 sm:w-6 sm:h-6 shrink-0" />}
                <span>{buttonText}</span>
            </span>
        </button>
    );
};
