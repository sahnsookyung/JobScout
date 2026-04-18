import React from 'react';
import { Loader2, Play, Square } from 'lucide-react';

const RESUME_STEP_LABELS: Record<string, string> = {
    extracting: 'Parsing resume',
    embedding: 'Building vectors',
};

export interface ActionButtonProps {
    canStop: boolean;
    isCancellationRequested: boolean;
    isPersistingStatus: boolean;
    isRunning: boolean;
    isStopping: boolean;
    isProcessingResume?: boolean;
    processingStep?: string | null;
    onRun: () => void;
    onStop: () => void;
}

export const ActionButton: React.FC<ActionButtonProps> = ({
    canStop,
    isCancellationRequested,
    isPersistingStatus,
    isRunning,
    isStopping,
    isProcessingResume,
    processingStep,
    onRun,
    onStop,
}) => {
    const isProcessing = (canStop ? isStopping : isRunning) || (isProcessingResume ?? false);
    const preparingLabel = processingStep
        ? (RESUME_STEP_LABELS[processingStep] ?? 'Preparing')
        : 'Preparing';

    let label = 'Run matching';
    let Icon: typeof Play = Play;
    let variantClasses = 'bg-accent border-accent text-[#FFF] hover:bg-accent-hover hover:border-accent-hover';

    if (isPersistingStatus) {
        label = 'Finishing';
        Icon = Loader2;
        variantClasses = 'bg-warn-soft border-warn text-ink';
    } else if (isCancellationRequested) {
        label = 'Stopping';
        Icon = Loader2;
        variantClasses = 'bg-surface-sunk border-rule-strong text-ink-soft';
    } else if (canStop) {
        label = 'Stop';
        Icon = Square;
        variantClasses = 'bg-surface border-rule-strong text-ink hover:border-ink-soft';
    } else if (isProcessingResume) {
        label = preparingLabel;
        Icon = Loader2;
        variantClasses = 'bg-surface border-rule text-ink-soft';
    }

    const iconSpin = Icon === Loader2;

    return (
        <button
            type="button"
            onClick={canStop ? onStop : onRun}
            disabled={isProcessing || isCancellationRequested || isPersistingStatus}
            className={`inline-flex h-10 w-full items-center justify-center gap-2 rounded-md border text-[14px] font-medium transition-colors duration-200 disabled:opacity-60 disabled:cursor-not-allowed ${variantClasses}`}
        >
            <Icon className={`h-3.5 w-3.5 ${iconSpin ? 'animate-spin' : ''}`} aria-hidden="true" />
            <span>{label}</span>
        </button>
    );
};
