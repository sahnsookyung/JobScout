// CompactControls.tsx
import React, { useRef, useState, useEffect } from 'react';
import { usePipeline } from '@/hooks/usePipeline';
import { useStats } from '@/hooks/useStats';
import { toast } from 'sonner';
import { pipelineApi } from '@/services/pipelineApi';
import { saveResume, hasResume, getResumeFilename } from '@/utils/indexedDB';
import { computeFileHash } from '@/utils/fileUtils';
import { RESUME_MAX_SIZE, RESUME_MAX_SIZE_MB } from '@shared/constants';
import { DashboardWrapper } from './DashboardWrapper';
import { ResumeUploadSection } from './ResumeUploadSection';
import { StatsPanel } from './StatsPanel';
import { ActionButton } from './ActionButton';
import { StatusBanner } from './StatusBanner';

export const CompactControls: React.FC = () => {
    const { runPipeline, stopPipeline, isRunning, isStopping, status, isUploading } = usePipeline();
    const { data: stats } = useStats();
    const [resumeFilename, setResumeFilename] = useState<string | null>(null);
    const fileInputRef = useRef<HTMLInputElement>(null);
    const [isProcessingResume, setIsProcessingResume] = useState(false);

    // Check for existing resume in IndexedDB on mount
    useEffect(() => {
        const checkExistingResume = async () => {
            try {
                const filename = await getResumeFilename();
                if (filename) {
                    setResumeFilename(filename);
                }
            } catch (error) {
                console.warn('[CompactControls] Failed to check IndexedDB:', error);
            }
        };
        checkExistingResume();
    }, []);

    // Handle run matching - check if resume exists first
    const handleRunMatching = async () => {
        const exists = await hasResume();
        if (!exists) {
            toast.error("No resume found in browser storage. Please upload a resume first.");
            return;
        }
        runPipeline();
    };

    const handleResumeUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
        const file = e.target.files?.[0];
        if (!file) return;

        // Check file size
        if (file.size > RESUME_MAX_SIZE) {
            toast.error(`File size exceeds ${RESUME_MAX_SIZE_MB}MB limit. Please upload a smaller file.`);
            if (fileInputRef.current) {
                fileInputRef.current.value = '';
            }
            return;
        }

        setIsProcessingResume(true);
        try {
            // Compute hash of file bytes
            const hash = await computeFileHash(file);

            // Check if hash already exists in backend
            const checkResponse = await pipelineApi.checkResumeHash(hash);

            if (checkResponse.data.exists) {
                // Hash exists, skip upload, store in IndexedDB (best effort)
                try {
                    await saveResume(file, hash, file.name);
                } catch (indexedDbError) {
                    console.warn('Failed to save resume to IndexedDB:', indexedDbError);
                }
                setResumeFilename(file.name);
                toast.success("An identical resume has already been uploaded.");
                return;
            }

            // Hash doesn't exist, need to upload
            const uploadResponse = await pipelineApi.uploadResume(file, hash);

            // Store in IndexedDB (best effort - don't fail if this fails)
            let savedLocally = true;
            try {
                await saveResume(file, hash, file.name);
            } catch (indexedDbError) {
                console.warn('Failed to save resume to IndexedDB:', indexedDbError);
                savedLocally = false;
            }

            setResumeFilename(file.name);
            toast.success(uploadResponse.data.message || 'Resume uploaded successfully');
        } catch (error) {
            const message = error instanceof Error ? error.message : 'Unknown error';
            toast.error(`Failed to upload resume: ${message}`);
        } finally {
            setIsProcessingResume(false);
            if (fileInputRef.current) {
                fileInputRef.current.value = '';
            }
        }
    };

    const statusData = status;

    const hasStatus = statusData !== null && statusData !== undefined;
    const isRunningStatus = hasStatus && statusData?.status === 'running';
    const isCompletedStatus = hasStatus && statusData?.status === 'completed';
    const isFailedStatus = hasStatus && statusData?.status === 'failed';
    const showStatusBanner = isRunningStatus || isCompletedStatus || isFailedStatus;

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

    const chartProps = {
        activeArc, hiddenArc, belowArc, circumference, radius
    };

    return (
        <DashboardWrapper>
            <div className="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-6">
                <StatsPanel stats={stats} {...chartProps} activeMatches={activeMatches} />
                <div className={hasStatus ? 'lg:self-center' : ''}>
                    <div className="flex gap-3 lg:flex-col lg:w-[180px]">
                        <ResumeUploadSection
                            fileInputRef={fileInputRef}
                            onUpload={handleResumeUpload}
                            isUploading={isUploading || isProcessingResume}
                            isRunning={isRunning}
                            filename={resumeFilename}
                        />
                        <ActionButton
                            isRunningStatus={isRunningStatus}
                            isRunning={isRunning}
                            isStopping={isStopping}
                            onRun={handleRunMatching}
                            onStop={stopPipeline}
                        />
                    </div>
                </div>
            </div>
            {showStatusBanner && <StatusBanner {...statusData!} />}
        </DashboardWrapper>
    );
};
