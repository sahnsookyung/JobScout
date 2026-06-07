import React, { useEffect, useRef, useState } from 'react';
import { usePipeline } from '@/hooks/usePipeline';
import { useStats } from '@/hooks/useStats';
import { toast } from 'sonner';
import { getResumeFilename } from '@/utils/indexedDB';
import { RESUME_MAX_SIZE, RESUME_MAX_SIZE_MB } from '@shared/constants';
import { DashboardWrapper } from './DashboardWrapper';
import { ResumeUploadSection } from './ResumeUploadSection';
import { StatsPanel } from './StatsPanel';
import { ActionButton } from './ActionButton';
import { StatusBanner } from './StatusBanner';
import { FetchSourcesPanel } from './FetchSourcesPanel';

export const DashboardControls: React.FC = () => {
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
    const { data: stats } = useStats();
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

    const totalMatches = stats?.total_matches ?? 0;
    const activeMatches = stats?.active_matches ?? 0;
    const hiddenMatches = stats?.hidden_count ?? 0;
    const belowThreshold = stats?.below_threshold_count ?? 0;

    const activePercentage = totalMatches > 0 ? (activeMatches / totalMatches) * 100 : 0;
    const hiddenPercentage = totalMatches > 0 ? (hiddenMatches / totalMatches) * 100 : 0;
    const belowPercentage = totalMatches > 0 ? (belowThreshold / totalMatches) * 100 : 0;

    const radius = 36;
    const circumference = 2 * Math.PI * radius;
    const activeArc = (activePercentage / 100) * circumference;
    const hiddenArc = (hiddenPercentage / 100) * circumference;
    const belowArc = (belowPercentage / 100) * circumference;

    const chartProps = { activeArc, hiddenArc, belowArc, circumference, radius };

    return (
        <DashboardWrapper>
            <div className="grid grid-cols-1 gap-8 lg:grid-cols-[1fr_auto] lg:items-start lg:gap-10">
                <StatsPanel stats={stats} {...chartProps} activeMatches={activeMatches} />
                <div className="flex flex-col gap-3 lg:w-[220px] lg:border-l lg:border-rule lg:pl-8">
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
            <FetchSourcesPanel />
            {showStatusBanner && statusData && <StatusBanner {...statusData} />}
        </DashboardWrapper>
    );
};
