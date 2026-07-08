import React, { useEffect, useRef, useState } from 'react';
import { usePipeline } from '@/hooks/usePipeline';
import { usePolicy } from '@/hooks/usePolicy';
import { useStats } from '@/hooks/useStats';
import { toast } from 'sonner';
import { getResumeFilename } from '@/utils/indexedDB';
import { RESUME_MAX_SIZE, RESUME_MAX_SIZE_MB } from '@shared/constants';
import { POLICY_PRESET_VALUES } from '@/utils/constants';
import { DashboardWrapper } from './DashboardWrapper';
import { ResumeUploadSection } from './ResumeUploadSection';
import { StatsPanel } from './StatsPanel';
import { ActionButton } from './ActionButton';
import { StatusBanner } from './StatusBanner';
import { FetchSourcesPanel } from './FetchSourcesPanel';
import { JobInventoryPanel } from './JobInventoryPanel';

interface DashboardControlsProps {
    includeManagementSections?: boolean;
}

export const DashboardControls: React.FC<DashboardControlsProps> = ({
    includeManagementSections = true,
}) => {
    const {
        runPipeline,
        stopPipeline,
        isRunning,
        isStopping,
        status,
        isUploading,
        isPreparingResume,
        resumeProcessingStep,
        resumeProcessingStatus,
        uploadResume,
    } = usePipeline();
    const { policy } = usePolicy();
    const effectivePolicy = policy ?? POLICY_PRESET_VALUES.balanced;
    const { data: stats } = useStats({
        min_fit: effectivePolicy.min_fit,
        top_k: effectivePolicy.top_k,
    });
    const [resumeFilename, setResumeFilename] = useState<string | null>(null);
    const fileInputRef = useRef<HTMLInputElement>(null);

    useEffect(() => {
        const checkExistingResume = async () => {
            try {
                const filename = await getResumeFilename();
                if (filename) setResumeFilename(filename);
            } catch (error) {
                console.warn('[DashboardControls] Failed to check IndexedDB:', error);
            }
        };
        checkExistingResume();
    }, []);

    const handleResumeUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
        const file = e.target.files?.[0];
        if (!file) return;

        if (file.size > RESUME_MAX_SIZE) {
            toast.error(`That file is over ${RESUME_MAX_SIZE_MB}MB. Try a smaller one.`);
            if (fileInputRef.current) fileInputRef.current.value = '';
            return;
        }

        try {
            const { alreadyExists, message } = await uploadResume(file);
            setResumeFilename(file.name);
            if (alreadyExists) {
                toast.success('This resume is already saved.');
            } else {
                toast.success(message);
            }
        } catch (error) {
            const message = error instanceof Error ? error.message : 'Unknown error';
            toast.error(`Resume upload failed: ${message}`);
        } finally {
            if (fileInputRef.current) fileInputRef.current.value = '';
        }
    };

    const statusData = status ?? resumeProcessingStatus;
    const hasStatus = statusData !== null && statusData !== undefined;
    const isRunningStatus = hasStatus && ['pending', 'running', 'processing'].includes(statusData?.status ?? '');
    const isCancellationRequested = hasStatus && statusData?.status === 'cancellation_requested';
    const isPersistingStatus = hasStatus && statusData?.status === 'persisting';
    const canStop = status !== null && ['pending', 'running'].includes(status?.status ?? '');
    const isCompletedStatus = hasStatus && statusData?.status === 'completed';
    const isFailedStatus = hasStatus && statusData?.status === 'failed';
    const isCancelledStatus = hasStatus && statusData?.status === 'cancelled';
    const showStatusBanner = (
        isRunningStatus || isCancellationRequested || isPersistingStatus
        || isCompletedStatus || isFailedStatus || isCancelledStatus
    );

    return (
        <DashboardWrapper>
            <div className="grid grid-cols-1 gap-8 xl:grid-cols-[minmax(0,1fr)_minmax(13.75rem,15rem)] xl:items-stretch xl:gap-8">
                <StatsPanel stats={stats} />
                <div className="flex flex-col gap-3 border-t border-rule pt-6 xl:border-l xl:border-t-0 xl:justify-center xl:pl-8 xl:pt-0">
                    <p className="caption">Run</p>
                    <ResumeUploadSection
                        fileInputRef={fileInputRef}
                        onUpload={handleResumeUpload}
                        isUploading={isUploading || isPreparingResume}
                        isRunning={isRunning}
                        filename={resumeFilename}
                        processingStep={resumeProcessingStep}
                    />
                    <ActionButton
                        canStop={canStop}
                        isCancellationRequested={isCancellationRequested}
                        isPersistingStatus={isPersistingStatus}
                        isRunning={isRunning}
                        isStopping={isStopping}
                        isProcessingResume={isPreparingResume}
                        processingStep={resumeProcessingStep}
                        onRun={() => runPipeline((msg) => toast.error(msg))}
                        onStop={stopPipeline}
                    />
                </div>
            </div>
            {includeManagementSections ? (
                <>
                    <FetchSourcesPanel />
                    <JobInventoryPanel stats={stats} />
                </>
            ) : null}
            {showStatusBanner && statusData && <StatusBanner {...statusData} />}
        </DashboardWrapper>
    );
};
