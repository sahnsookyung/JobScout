// CompactControls.tsx
import React, { useRef, useState } from 'react';
import { TrendingUp, Zap, CheckCircle, XCircle, Loader, ArrowUpRight, Award, FileUp } from 'lucide-react';
import { usePipeline } from '@/hooks/usePipeline';
import { useStats } from '@/hooks/useStats';
import { Badge } from '@/components/ui/Badge';
import { toast } from 'sonner';
import { pipelineApi } from '@/services/pipelineApi';
import { saveResume } from '@/utils/indexedDB';
import { RESUME_MAX_SIZE, RESUME_MAX_SIZE_MB } from '@shared/constants';

import xxhash from 'xxhash-wasm';

let xxhPromise: ReturnType<typeof xxhash> | null = null;

async function getXxh() {
    if (!xxhPromise) {
        xxhPromise = xxhash();
    }
    return xxhPromise;
}

async function computeFileHash(file: File): Promise<string> {
    const xxh = await getXxh();
    const buffer = await file.arrayBuffer();
    return xxh.h64ToString(new Uint8Array(buffer));
}

interface ScoreBarProps {
    label: string;
    range: string;
    value: number;
    total: number;
    gradient: string;
}

const CompactScoreBar: React.FC<ScoreBarProps> = ({ label, range, value, total, gradient }) => {
    const percentage = total > 0 ? (value / total) * 100 : 0;
    return (
        <div>
            <div className="flex items-center justify-between mb-1.5">
                <div className="flex items-center gap-2">
                    <div className={`w-2 h-2 sm:w-2.5 sm:h-2.5 rounded-full bg-gradient-to-r ${gradient}`} />
                    <span className="text-xs sm:text-sm font-black text-gray-900">{label}</span>
                    <span className="text-[10px] sm:text-xs text-gray-500 font-semibold">({range})</span>
                </div>
                <span className="text-sm sm:text-base font-black text-gray-900">{value}</span>
            </div>
            <div className="h-1.5 sm:h-2 bg-gray-200 rounded-full overflow-hidden">
                <div className={`h-full bg-gradient-to-r ${gradient} transition-all duration-500 ease-out rounded-full`} style={{ width: `${percentage}%` }} />
            </div>
        </div>
    );
};

interface CircleChartProps {
    activeMatches: number;
    activeArc: number;
    hiddenArc: number;
    belowArc: number;
    circumference: number;
    radius: number;
}

const SegmentedCircle: React.FC<CircleChartProps> = ({ 
    activeMatches, activeArc, hiddenArc, belowArc, circumference, radius 
}) => (
    <div className="relative w-28 h-28 sm:w-32 sm:h-32 lg:w-36 lg:h-36 flex-shrink-0">
        <svg className="transform -rotate-90 w-full h-full" viewBox="0 0 96 96">
            <circle cx="48" cy="48" r={radius} stroke="currentColor" strokeWidth="8" fill="none" className="text-gray-200" />
            <circle cx="48" cy="48" r={radius} stroke="url(#gradient-active)" strokeWidth="8" fill="none" strokeDasharray={circumference} strokeDashoffset={0} className="transition-all duration-1000 ease-out" strokeLinecap="round" style={{ strokeDasharray: `${activeArc} ${circumference - activeArc}` }} />
            <circle cx="48" cy="48" r={radius} stroke="#9ca3af" strokeWidth="8" fill="none" strokeDasharray={circumference} strokeDashoffset={-activeArc} className="transition-all duration-1000 ease-out" strokeLinecap="round" style={{ strokeDasharray: `${hiddenArc} ${circumference - hiddenArc}` }} />
            <circle cx="48" cy="48" r={radius} stroke="#d1d5db" strokeWidth="8" fill="none" strokeDasharray={circumference} strokeDashoffset={-(activeArc + hiddenArc)} className="transition-all duration-1000 ease-out" strokeLinecap="round" style={{ strokeDasharray: `${belowArc} ${circumference - belowArc}` }} />
            <defs>
                <linearGradient id="gradient-active" x1="0%" y1="0%" x2="100%" y2="100%">
                    <stop offset="0%" stopColor="#3b82f6" />
                    <stop offset="100%" stopColor="#8b5cf6" />
                </linearGradient>
            </defs>
        </svg>
        <div className="absolute inset-0 flex items-center justify-center">
            <div className="text-center">
                <div className="text-3xl sm:text-4xl font-black text-gray-800">{activeMatches}</div>
                <div className="text-[10px] sm:text-xs font-bold text-gray-500 uppercase">Active</div>
            </div>
        </div>
    </div>
);

interface StatsPanelProps {
    stats: {
        total_matches?: number;
        active_matches?: number;
        hidden_count?: number;
        below_threshold_count?: number;
        score_distribution?: {
            excellent?: number;
            good?: number;
            average?: number;
            poor?: number;
        };
    } | null | undefined;
    activeMatches: number;
    activeArc: number;
    hiddenArc: number;
    belowArc: number;
    circumference: number;
    radius: number;
}

const StatsPanel: React.FC<StatsPanelProps> = ({ stats, ...chartProps }) => {
    const totalMatches = stats?.total_matches ?? 0;
    const activeMatches = stats?.active_matches ?? 0;
    const hiddenMatches = stats?.hidden_count ?? 0;
    const belowThreshold = stats?.below_threshold_count ?? 0;
    const scoreDist = stats?.score_distribution;

    return (
        <div className="flex flex-col sm:flex-row gap-6 lg:flex-1 sm:items-stretch">
            {/* Total Matches */}
            <div className="relative flex-1">
                <div className="absolute -inset-3 bg-gradient-to-r from-blue-500/20 to-indigo-500/20 rounded-2xl blur-xl" />
                <div className="relative bg-white/80 backdrop-blur-sm rounded-2xl p-5 sm:p-6 shadow-lg border border-white/50 h-full flex items-center justify-center">
                    <div className="flex items-center gap-3 sm:gap-4">
                        <div className="p-2.5 sm:p-3 bg-gradient-to-br from-blue-500 to-indigo-600 rounded-xl shadow-lg">
                            <TrendingUp className="w-6 h-6 sm:w-7 sm:h-7 text-white" aria-hidden="true" />
                        </div>
                        <div className="text-center">
                            <div className="text-4xl sm:text-5xl lg:text-6xl font-black bg-gradient-to-br from-gray-900 via-gray-800 to-gray-600 bg-clip-text text-transparent leading-none">{totalMatches}</div>
                            <div className="text-[11px] sm:text-xs font-bold text-gray-500 uppercase tracking-wider mt-1 sm:mt-1.5">Total Matches</div>
                        </div>
                    </div>
                </div>
            </div>

            {/* Segmented Circle */}
            <div className="relative flex-1">
                <div className="bg-white/60 backdrop-blur-sm rounded-2xl p-5 sm:p-6 border border-white/50 h-full flex items-center justify-center">
                    <div className="flex items-center justify-center gap-4 sm:gap-5">
                        <SegmentedCircle {...chartProps} activeMatches={activeMatches} />
                        <div className="space-y-2 sm:space-y-2.5 flex-1">
                            <div className="flex items-center gap-2">
                                <div className="w-3 h-3 sm:w-3.5 sm:h-3.5 rounded-full bg-gradient-to-r from-blue-500 to-purple-500" />
                                <span className="text-xs sm:text-sm font-bold text-gray-700">{activeMatches} Active</span>
                            </div>
                            <div className="flex items-center gap-2">
                                <div className="w-3 h-3 sm:w-3.5 sm:h-3.5 rounded-full bg-gray-400" />
                                <span className="text-xs sm:text-sm font-bold text-gray-700">{hiddenMatches} Hidden</span>
                            </div>
                            <div className="flex items-center gap-2">
                                <div className="w-3 h-3 sm:w-3.5 sm:h-3.5 rounded-full bg-gray-300" />
                                <span className="text-xs sm:text-sm font-bold text-gray-700">{belowThreshold} Below</span>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            {/* Score Distribution */}
            <div className="relative flex-1 lg:flex-[1.2]">
                <div className="bg-white/60 backdrop-blur-sm rounded-2xl p-5 sm:p-6 border border-white/50 h-full flex flex-col justify-center">
                    <div className="flex items-center gap-2.5 mb-3 sm:mb-4">
                        <Award className="w-5 h-5 sm:w-6 sm:h-6 text-blue-600" aria-hidden="true" />
                        <h4 className="text-xs sm:text-sm font-black text-gray-900 uppercase tracking-wider">Score Distribution</h4>
                    </div>
                    <div className="space-y-2.5 sm:space-y-3">
                        <CompactScoreBar label="Excellent" range="80+" value={scoreDist?.excellent ?? 0} total={totalMatches} gradient="from-blue-500 to-indigo-600" />
                        <CompactScoreBar label="Good" range="60-79" value={scoreDist?.good ?? 0} total={totalMatches} gradient="from-blue-400 to-blue-500" />
                        <CompactScoreBar label="Average" range="40-59" value={scoreDist?.average ?? 0} total={totalMatches} gradient="from-gray-400 to-gray-500" />
                        <CompactScoreBar label="Poor" range="<40" value={scoreDist?.poor ?? 0} total={totalMatches} gradient="from-gray-300 to-gray-400" />
                    </div>
                </div>
            </div>
        </div>
    );
};

interface ResumeUploadSectionProps {
    fileInputRef: React.RefObject<HTMLInputElement | null>;
    onUpload: (e: React.ChangeEvent<HTMLInputElement>) => void;
    isUploading: boolean;
    isRunning: boolean;
    filename: string | null;
}

const ResumeUploadSection: React.FC<ResumeUploadSectionProps> = ({ fileInputRef, onUpload, isUploading, isRunning, filename }) => (
    <>
        <button
            onClick={() => fileInputRef.current?.click()}
            disabled={isRunning || isUploading}
            className="w-full lg:w-auto px-6 py-4 border-2 border-gray-300 text-gray-700 font-semibold rounded-xl hover:border-blue-500 hover:text-blue-600 hover:bg-blue-50 transition-all duration-200 disabled:opacity-50 disabled:cursor-not-allowed flex items-center justify-center gap-2 min-w-[160px]"
        >
            {isUploading ? <Loader className="w-5 h-5 animate-spin" /> : <FileUp className="w-5 h-5" />}
            <span>{filename ? 'Update Resume' : `Upload Resume (max ${RESUME_MAX_SIZE_MB}MB)`}</span>
            {filename && <span className="ml-2 text-xs opacity-70 truncate max-w-[120px]">({filename})</span>}
        </button>
        <input
            ref={fileInputRef}
            type="file"
            accept=".json,.yaml,.yml,.txt,.docx,.pdf"
            className="hidden"
            onChange={onUpload}
            data-testid="resume-file-input"
        />
    </>
);

interface ActionButtonProps {
    isRunningStatus: boolean;
    isRunning: boolean;
    isStopping: boolean;
    onRun: () => void;
    onStop: () => void;
}

const ActionButton: React.FC<ActionButtonProps> = ({ isRunningStatus, isRunning, isStopping, onRun, onStop }) => {
    const isProcessing = isRunningStatus ? isStopping : isRunning;
    const buttonText = isRunningStatus ? 'Stop' : 'Run Matching';
    
    return (
        <button
            onClick={isRunningStatus ? onStop : onRun}
            disabled={isProcessing}
            className={`w-full lg:w-auto group relative px-8 py-5 sm:px-10 sm:py-6 font-bold rounded-2xl shadow-lg hover:shadow-2xl hover:scale-105 active:scale-95 transition-all duration-200 disabled:opacity-50 overflow-hidden ${
                isRunningStatus ? 'bg-red-500 text-white hover:bg-red-600' : 'bg-gradient-to-r from-blue-600 to-indigo-600 text-white'
            }`}
        >
            <div className={`absolute inset-0 opacity-0 group-hover:opacity-100 transition-opacity duration-200 ${
                isRunningStatus ? 'bg-red-400' : 'bg-gradient-to-r from-blue-400 to-indigo-400'
            }`} />
            <div className="relative flex items-center justify-center gap-2.5 sm:gap-3">
                {!isRunningStatus && <Zap className="w-5 h-5 sm:w-6 sm:h-6" />}
                <span className="text-base sm:text-lg">{buttonText}</span>
                {!isRunningStatus && <ArrowUpRight className="w-4 h-4 sm:w-5 sm:h-5 group-hover:translate-x-0.5 group-hover:-translate-y-0.5 transition-transform" />}
            </div>
        </button>
    );
};

interface StatusBannerProps {
    status: string;
    step?: string;
    matches_count?: number;
    saved_count?: number;
    execution_time?: number;
    error?: string;
}

const StatusBanner: React.FC<StatusBannerProps> = (statusData) => {
    const isRunningStatus = statusData.status === 'running';
    const isCompletedStatus = statusData.status === 'completed';
    const isFailedStatus = statusData.status === 'failed';

    const getStepLabel = (step?: string): string => {
        const labels: Record<string, string> = {
            loading_resume: 'Loading Resume',
            vector_matching: 'Finding Matches',
            scoring: 'Scoring Candidates',
            saving_results: 'Saving Results',
            notifying: 'Notifying',
            initializing: 'Initializing',
        };
        return step ? labels[step] || 'Processing' : 'Initializing';
    };

    const formatTime = (time?: number): string => (time ?? 0).toFixed(2);

    return (
        <div className="mt-6 bg-white/60 backdrop-blur-sm rounded-2xl p-6 border border-white/50">
            <div className="flex items-start gap-4">
                <div className={`p-3 rounded-xl ${isRunningStatus ? 'bg-blue-100' : isCompletedStatus ? 'bg-green-100' : 'bg-red-100'}`}>
                    {isRunningStatus && <Loader className="w-6 h-6 animate-spin text-blue-600" />}
                    {isCompletedStatus && <CheckCircle className="w-6 h-6 text-green-600" />}
                    {isFailedStatus && <XCircle className="w-6 h-6 text-red-600" />}
                </div>
                <div className="flex-1">
                    <div className="flex items-center gap-3 mb-2">
                        <Badge variant={isRunningStatus ? 'info' : isCompletedStatus ? 'success' : 'error'}>
                            {statusData.status?.toUpperCase()}
                        </Badge>
                        {isRunningStatus && (
                            <div className="flex items-center gap-2">
                                <div className="relative w-2 h-2">
                                    <div className="absolute inset-0 bg-blue-500 rounded-full animate-ping" />
                                    <div className="relative bg-blue-600 rounded-full w-2 h-2" />
                                </div>
                                <span className="text-sm font-bold text-blue-900">{getStepLabel(statusData.step)}</span>
                            </div>
                        )}
                    </div>
                    {isRunningStatus && <p className="text-sm text-gray-600 mt-1">Processing your matches...</p>}
                    {isCompletedStatus && (
                        <div>
                            <p className="font-bold text-gray-900 mb-1">Pipeline completed!</p>
                            <div className="flex gap-4 text-sm text-gray-700">
                                <span className="font-semibold">Found: {statusData.matches_count ?? 0}</span>
                                <span className="font-semibold">Saved: {statusData.saved_count ?? 0}</span>
                                <span className="font-semibold">Time: {formatTime(statusData.execution_time)}s</span>
                            </div>
                        </div>
                    )}
                    {isFailedStatus && (
                        <div>
                            <p className="font-bold text-red-700 mb-2">Pipeline failed</p>
                            {statusData.error && (
                                <p className="text-sm text-gray-700 bg-red-50 p-3 rounded-lg border border-red-200">{statusData.error}</p>
                            )}
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
};

const DashboardWrapper: React.FC<{ children: React.ReactNode }> = ({ children }) => (
    <div className="relative bg-gradient-to-br from-slate-50 via-blue-50 to-indigo-50 rounded-3xl overflow-hidden">
        <div className="absolute top-0 right-0 w-64 h-64 bg-blue-400/10 rounded-full blur-3xl" />
        <div className="absolute bottom-0 left-0 w-48 h-48 bg-indigo-400/10 rounded-full blur-3xl" />
        <div className="relative p-6">{children}</div>
    </div>
);

export const CompactControls: React.FC = () => {
    const { runPipeline, stopPipeline, isRunning, isStopping, status, isUploading } = usePipeline();
    const { data: stats } = useStats();
    const [resumeFilename, setResumeFilename] = useState<string | null>(null);
    const fileInputRef = useRef<HTMLInputElement>(null);
    const [isProcessingResume, setIsProcessingResume] = useState(false);

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

            if (checkResponse.exists) {
                // Hash exists, skip upload, store in IndexedDB (best effort)
                try {
                    await saveResume(file, hash);
                } catch (indexedDbError) {
                    console.warn('Failed to save resume to IndexedDB:', indexedDbError);
                }
                setResumeFilename(file.name);
                toast.success("Resume already processed!");
                return;
            }

            // Hash doesn't exist, need to upload
            const uploadResponse = await pipelineApi.uploadResume(file, hash);

            // Verify returned hash matches
            if (uploadResponse.resume_hash !== hash) {
                throw new Error("Hash verification failed");
            }

            // Store in IndexedDB (best effort - don't fail if this fails)
            let savedLocally = true;
            try {
                await saveResume(file, hash);
            } catch (indexedDbError) {
                console.warn('Failed to save resume to IndexedDB:', indexedDbError);
                savedLocally = false;
            }

            setResumeFilename(file.name);
            toast.success(
                savedLocally
                    ? 'Resume uploaded!'
                    : 'Resume uploaded (could not be saved locally)'
            );
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
                            onRun={runPipeline}
                            onStop={stopPipeline}
                        />
                    </div>
                </div>
            </div>
            {showStatusBanner && <StatusBanner {...statusData!} />}
        </DashboardWrapper>
    );
};
