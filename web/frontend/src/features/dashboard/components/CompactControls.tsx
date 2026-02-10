// CompactControls.tsx
import React, { useRef, useState } from 'react';
import { TrendingUp, Zap, CheckCircle, XCircle, Loader, ArrowUpRight, Award, FileUp } from 'lucide-react';
import { usePipeline } from '@/hooks/usePipeline';
import { useStats } from '@/hooks/useStats';
import { Button } from '@/components/ui/Button';
import { Badge } from '@/components/ui/Badge';
import { toast } from 'sonner';


export const CompactControls: React.FC = () => {
    const { runPipeline, stopPipeline, isRunning, isStopping, status, uploadResume, isUploading } = usePipeline();
    const { data: stats } = useStats();
    const [resumeFilename, setResumeFilename] = useState<string | null>(null);
    const fileInputRef = useRef<HTMLInputElement>(null);

    const handleResumeUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
        const file = e.target.files?.[0];
        if (!file) return;

        try {
            await uploadResume(file);
            setResumeFilename(file.name);
            toast.success("Resume uploaded!");
        } catch (error) {
            const message = error instanceof Error ? error.message : 'Unknown error';
            toast.error(`Failed to upload resume: ${message}`);
        } finally {
            if (fileInputRef.current) {
                fileInputRef.current.value = '';
            }
        }
    };


    const statusData = status as {
        status: string;
        step?: string;
        saved_count?: number;
        matches_count?: number;
        execution_time?: number;
        error?: string;
    } | null | undefined;


    const hasStatus = statusData !== null && statusData !== undefined;
    const isRunningStatus = hasStatus && statusData?.status === 'running';
    const isCompletedStatus = hasStatus && statusData?.status === 'completed';
    const isFailedStatus = hasStatus && statusData?.status === 'failed';


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


    const totalMatches = stats?.total_matches ?? 0;
    const activeMatches = stats?.active_matches ?? 0;
    const hiddenMatches = stats?.hidden_count ?? 0;
    const belowThreshold = stats?.below_threshold_count ?? 0;


    // Calculate percentages for the segmented circle
    const activePercentage = totalMatches > 0 ? (activeMatches / totalMatches) * 100 : 0;
    const hiddenPercentage = totalMatches > 0 ? (hiddenMatches / totalMatches) * 100 : 0;
    const belowPercentage = totalMatches > 0 ? (belowThreshold / totalMatches) * 100 : 0;


    // Calculate cumulative percentages for stroke-dashoffset
    const radius = 36;
    const circumference = 2 * Math.PI * radius;
    const activeArc = (activePercentage / 100) * circumference;
    const hiddenArc = (hiddenPercentage / 100) * circumference;
    const belowArc = (belowPercentage / 100) * circumference;


    if (!hasStatus) {
        return (
            <div className="relative bg-gradient-to-br from-slate-50 via-blue-50 to-indigo-50 rounded-3xl overflow-hidden">
                <div className="absolute top-0 right-0 w-64 h-64 bg-blue-400/10 rounded-full blur-3xl" />
                <div className="absolute bottom-0 left-0 w-48 h-48 bg-indigo-400/10 rounded-full blur-3xl" />


                <div className="relative p-6">
                    {/* Mobile: Column layout, Desktop: Row layout with button at the end */}
                    <div className="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-6">
                        {/* Stats Section */}
                        <div className="flex flex-col sm:flex-row gap-6 lg:flex-1 sm:items-stretch">
                            {/* Total Matches - CENTERED & IMPROVED SIZING */}
                            <div className="relative flex-1">
                                <div className="absolute -inset-3 bg-gradient-to-r from-blue-500/20 to-indigo-500/20 rounded-2xl blur-xl" />
                                <div className="relative bg-white/80 backdrop-blur-sm rounded-2xl p-5 sm:p-6 shadow-lg border border-white/50 h-full flex items-center justify-center">
                                    <div className="flex items-center gap-3 sm:gap-4">
                                        <div className="p-2.5 sm:p-3 bg-gradient-to-br from-blue-500 to-indigo-600 rounded-xl shadow-lg">
                                            <TrendingUp className="w-6 h-6 sm:w-7 sm:h-7 text-white" aria-hidden="true" />
                                        </div>
                                        <div className="text-center">
                                            <div className="text-4xl sm:text-5xl lg:text-6xl font-black bg-gradient-to-br from-gray-900 via-gray-800 to-gray-600 bg-clip-text text-transparent leading-none">
                                                {totalMatches}
                                            </div>
                                            <div className="text-[11px] sm:text-xs font-bold text-gray-500 uppercase tracking-wider mt-1 sm:mt-1.5">
                                                Total Matches
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            </div>


                            {/* Segmented Circle - CENTERED & IMPROVED SIZING */}
                            <div className="relative flex-1">
                                <div className="bg-white/60 backdrop-blur-sm rounded-2xl p-5 sm:p-6 border border-white/50 h-full flex items-center justify-center">
                                    <div className="flex items-center justify-center gap-4 sm:gap-5">
                                        <div className="relative w-28 h-28 sm:w-32 sm:h-32 lg:w-36 lg:h-36 flex-shrink-0">
                                            <svg className="transform -rotate-90 w-full h-full" viewBox="0 0 96 96">
                                                <circle cx="48" cy="48" r={radius} stroke="currentColor" strokeWidth="8" fill="none" className="text-gray-200" />
                                                <circle
                                                    cx="48" cy="48" r={radius} stroke="url(#gradient-active)" strokeWidth="8" fill="none"
                                                    strokeDasharray={circumference} strokeDashoffset={0}
                                                    className="transition-all duration-1000 ease-out" strokeLinecap="round"
                                                    style={{ strokeDasharray: `${activeArc} ${circumference - activeArc}` }}
                                                />
                                                <circle
                                                    cx="48" cy="48" r={radius} stroke="#9ca3af" strokeWidth="8" fill="none"
                                                    strokeDasharray={circumference} strokeDashoffset={-activeArc}
                                                    className="transition-all duration-1000 ease-out" strokeLinecap="round"
                                                    style={{ strokeDasharray: `${hiddenArc} ${circumference - hiddenArc}` }}
                                                />
                                                <circle
                                                    cx="48" cy="48" r={radius} stroke="#d1d5db" strokeWidth="8" fill="none"
                                                    strokeDasharray={circumference} strokeDashoffset={-(activeArc + hiddenArc)}
                                                    className="transition-all duration-1000 ease-out" strokeLinecap="round"
                                                    style={{ strokeDasharray: `${belowArc} ${circumference - belowArc}` }}
                                                />
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


                            {/* Score Distribution - IMPROVED SIZING */}
                            <div className="relative flex-1 lg:flex-[1.2]">
                                <div className="bg-white/60 backdrop-blur-sm rounded-2xl p-5 sm:p-6 border border-white/50 h-full flex flex-col justify-center">
                                    <div className="flex items-center gap-2.5 mb-3 sm:mb-4">
                                        <Award className="w-5 h-5 sm:w-6 sm:h-6 text-blue-600" aria-hidden="true" />
                                        <h4 className="text-xs sm:text-sm font-black text-gray-900 uppercase tracking-wider">Score Distribution</h4>
                                    </div>
                                    <div className="space-y-2.5 sm:space-y-3">
                                        <CompactScoreBar label="Excellent" range="80+" value={stats?.score_distribution?.excellent ?? 0} total={totalMatches} gradient="from-blue-500 to-indigo-600" />
                                        <CompactScoreBar label="Good" range="60-79" value={stats?.score_distribution?.good ?? 0} total={totalMatches} gradient="from-blue-400 to-blue-500" />
                                        <CompactScoreBar label="Average" range="40-59" value={stats?.score_distribution?.average ?? 0} total={totalMatches} gradient="from-gray-400 to-gray-500" />
                                        <CompactScoreBar label="Poor" range="<40" value={stats?.score_distribution?.poor ?? 0} total={totalMatches} gradient="from-gray-300 to-gray-400" />
                                    </div>
                                </div>
                            </div>
                        </div>


                        {/* Action Buttons */}
                        <div className="flex gap-3 lg:flex-col lg:w-sidebar-content">
                            {/* Resume Upload Button - Secondary Style */}
                            <button
                                onClick={() => fileInputRef.current?.click()}
                                disabled={isRunning || isUploading}
                                className="w-full lg:w-auto px-6 py-4 border-2 border-gray-300 text-gray-700 font-semibold rounded-xl hover:border-blue-500 hover:text-blue-600 hover:bg-blue-50 transition-all duration-200 disabled:opacity-50 disabled:cursor-not-allowed flex items-center justify-center gap-2 min-w-[160px]"
                            >
                                {isUploading ? (
                                    <Loader className="w-5 h-5 animate-spin" />
                                ) : (
                                    <FileUp className="w-5 h-5" />
                                )}
                                <span>{resumeFilename ? 'Update Resume' : 'Upload Resume'}</span>
                                {resumeFilename && (
                                    <span className="ml-2 text-xs opacity-70 truncate max-w-[120px]">
                                        ({resumeFilename})
                                    </span>
                                )}
                            </button>
                            <input
                                ref={fileInputRef}
                                type="file"
                                accept=".json"
                                className="hidden"
                                onChange={handleResumeUpload}
                                data-testid="resume-file-input"
                            />

                            {/* Run Matching Button - Primary Style */}
                            <button
                                onClick={() => runPipeline()}
                                disabled={isRunning}
                                className="w-full lg:w-auto group relative px-8 py-5 sm:px-10 sm:py-6 bg-gradient-to-r from-blue-600 to-indigo-600 text-white font-bold rounded-2xl shadow-lg hover:shadow-2xl hover:scale-105 active:scale-95 transition-all duration-200 disabled:opacity-50 disabled:cursor-not-allowed overflow-hidden"
                            >
                                <div className="absolute inset-0 bg-gradient-to-r from-blue-400 to-indigo-400 opacity-0 group-hover:opacity-100 transition-opacity duration-200" />
                                <div className="relative flex items-center justify-center gap-2.5 sm:gap-3">
                                    <Zap className="w-5 h-5 sm:w-6 sm:h-6" />
                                    <span className="text-base sm:text-lg">Run Matching</span>
                                    <ArrowUpRight className="w-4 h-4 sm:w-5 sm:h-5 group-hover:translate-x-0.5 group-hover:-translate-y-0.5 transition-transform" />
                                </div>
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        );
    }


    return (
        <div className="relative bg-gradient-to-br from-slate-50 via-blue-50 to-indigo-50 rounded-3xl overflow-hidden">
            <div className="absolute top-0 right-0 w-64 h-64 bg-blue-400/10 rounded-full blur-3xl" />
            <div className="absolute bottom-0 left-0 w-48 h-48 bg-indigo-400/10 rounded-full blur-3xl" />


            <div className="relative p-6">
                <div className="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-6">
                    <div className="flex flex-col sm:flex-row gap-6 lg:flex-1 sm:items-stretch">
                        {/* Total Matches - CENTERED & IMPROVED SIZING */}
                        <div className="relative flex-1">
                            <div className="absolute -inset-3 bg-gradient-to-r from-blue-500/20 to-indigo-500/20 rounded-2xl blur-xl" />
                            <div className="relative bg-white/80 backdrop-blur-sm rounded-2xl p-5 sm:p-6 shadow-lg border border-white/50 h-full flex items-center justify-center">
                                <div className="flex items-center gap-3 sm:gap-4">
                                    <div className="p-2.5 sm:p-3 bg-gradient-to-br from-blue-500 to-indigo-600 rounded-xl shadow-lg">
                                        <TrendingUp className="w-6 h-6 sm:w-7 sm:h-7 text-white" />
                                    </div>
                                    <div className="text-center">
                                        <div className="text-4xl sm:text-5xl lg:text-6xl font-black bg-gradient-to-br from-gray-900 via-gray-800 to-gray-600 bg-clip-text text-transparent leading-none">
                                            {totalMatches}
                                        </div>
                                        <div className="text-[11px] sm:text-xs font-bold text-gray-500 uppercase tracking-wider mt-1 sm:mt-1.5">
                                            Total Matches
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>


                        {/* Segmented Circle - CENTERED & IMPROVED SIZING */}
                        <div className="relative flex-1">
                            <div className="bg-white/60 backdrop-blur-sm rounded-2xl p-5 sm:p-6 border border-white/50 h-full flex items-center justify-center">
                                <div className="flex items-center justify-center gap-4 sm:gap-5">
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


                        {/* Score Distribution - IMPROVED SIZING */}
                        <div className="relative flex-1 lg:flex-[1.2]">
                            <div className="bg-white/60 backdrop-blur-sm rounded-2xl p-5 sm:p-6 border border-white/50 h-full flex flex-col justify-center">
                                <div className="flex items-center gap-2.5 mb-3 sm:mb-4">
                                    <Award className="w-5 h-5 sm:w-6 sm:h-6 text-blue-600" aria-hidden="true" />
                                    <h4 className="text-xs sm:text-sm font-black text-gray-900 uppercase tracking-wider">Score Distribution</h4>
                                </div>
                                <div className="space-y-2.5 sm:space-y-3">
                                    <CompactScoreBar label="Excellent" range="80+" value={stats?.score_distribution?.excellent ?? 0} total={totalMatches} gradient="from-blue-500 to-indigo-600" />
                                    <CompactScoreBar label="Good" range="60-79" value={stats?.score_distribution?.good ?? 0} total={totalMatches} gradient="from-blue-400 to-blue-500" />
                                    <CompactScoreBar label="Average" range="40-59" value={stats?.score_distribution?.average ?? 0} total={totalMatches} gradient="from-gray-400 to-gray-500" />
                                    <CompactScoreBar label="Poor" range="<40" value={stats?.score_distribution?.poor ?? 0} total={totalMatches} gradient="from-gray-300 to-gray-400" />
                                </div>
                            </div>
                        </div>
                    </div>


                    {/* Action Section */}
                    <div className="lg:self-center">
                        <div className="flex gap-3 lg:flex-col lg:w-sidebar-content">
                            {/* Resume Upload Button - Secondary Style */}
                            <button
                                onClick={() => fileInputRef.current?.click()}
                                disabled={isRunning || isUploading}
                                className="w-full lg:w-auto px-6 py-4 border-2 border-gray-300 text-gray-700 font-semibold rounded-xl hover:border-blue-500 hover:text-blue-600 hover:bg-blue-50 transition-all duration-200 disabled:opacity-50 disabled:cursor-not-allowed flex items-center justify-center gap-2 min-w-[160px]"
                            >
                                {isUploading ? (
                                    <Loader className="w-5 h-5 animate-spin" />
                                ) : (
                                    <FileUp className="w-5 h-5" />
                                )}
                                <span>{resumeFilename ? 'Update Resume' : 'Upload Resume'}</span>
                                {resumeFilename && (
                                    <span className="ml-2 text-xs opacity-70 truncate max-w-[120px]">
                                        ({resumeFilename})
                                    </span>
                                )}
                            </button>
                            <input
                                ref={fileInputRef}
                                type="file"
                                accept=".json"
                                className="hidden"
                                onChange={handleResumeUpload}
                                data-testid="resume-file-input"
                            />

                            {/* Run/Stop Matching Button */}
                            <button
                                onClick={isRunningStatus ? () => stopPipeline() : () => runPipeline()}
                                disabled={isRunningStatus ? isStopping : isRunning}
                                className={`w-full lg:w-auto group relative px-8 py-5 sm:px-10 sm:py-6 font-bold rounded-2xl shadow-lg hover:shadow-2xl hover:scale-105 active:scale-95 transition-all duration-200 disabled:opacity-50 overflow-hidden ${isRunningStatus
                                    ? 'bg-red-500 text-white hover:bg-red-600'
                                    : 'bg-gradient-to-r from-blue-600 to-indigo-600 text-white'
                                    }`}
                            >
                                <div className={`absolute inset-0 opacity-0 group-hover:opacity-100 transition-opacity duration-200 ${isRunningStatus ? 'bg-red-400' : 'bg-gradient-to-r from-blue-400 to-indigo-400'
                                    }`} />
                                <div className="relative flex items-center justify-center gap-2.5 sm:gap-3">
                                    {!isRunningStatus && <Zap className="w-5 h-5 sm:w-6 sm:h-6" />}
                                    <span className="text-base sm:text-lg">{isRunningStatus ? 'Stop' : 'Run Matching'}</span>
                                    {!isRunningStatus && (
                                        <ArrowUpRight className="w-4 h-4 sm:w-5 sm:h-5 group-hover:translate-x-0.5 group-hover:-translate-y-0.5 transition-transform" />
                                    )}
                                </div>
                            </button>
                        </div>
                    </div>
                </div>


                {/* Status Banner - WITH RUNNING INDICATOR */}
                {(isRunningStatus || isCompletedStatus || isFailedStatus) && (
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
                                        {statusData?.status?.toUpperCase()}
                                    </Badge>
                                    {/* Running Indicator - Pulsing Dot + Step Label */}
                                    {isRunningStatus && (
                                        <div className="flex items-center gap-2">
                                            <div className="relative w-2 h-2">
                                                <div className="absolute inset-0 bg-blue-500 rounded-full animate-ping" />
                                                <div className="relative bg-blue-600 rounded-full w-2 h-2" />
                                            </div>
                                            <span className="text-sm font-bold text-blue-900">
                                                {getStepLabel(statusData?.step)}
                                            </span>
                                        </div>
                                    )}
                                </div>
                                {isRunningStatus && (
                                    <div>
                                        <p className="text-sm text-gray-600 mt-1">Processing your matches...</p>
                                    </div>
                                )}
                                {isCompletedStatus && (
                                    <div>
                                        <p className="font-bold text-gray-900 mb-1">Pipeline completed!</p>
                                        <div className="flex gap-4 text-sm text-gray-700">
                                            <span className="font-semibold">Found: {statusData?.matches_count ?? 0}</span>
                                            <span className="font-semibold">Saved: {statusData?.saved_count ?? 0}</span>
                                            <span className="font-semibold">Time: {formatTime(statusData?.execution_time)}s</span>
                                        </div>
                                    </div>
                                )}
                                {isFailedStatus && (
                                    <div>
                                        <p className="font-bold text-red-700 mb-2">Pipeline failed</p>
                                        {statusData?.error && (
                                            <p className="text-sm text-gray-700 bg-red-50 p-3 rounded-lg border border-red-200">{statusData.error}</p>
                                        )}
                                    </div>
                                )}
                            </div>
                        </div>
                    </div>
                )}
            </div>
        </div>
    );
};


// Compact Score Bar Component - IMPROVED SIZING
interface CompactScoreBarProps {
    label: string;
    range: string;
    value: number;
    total: number;
    gradient: string;
}


const CompactScoreBar: React.FC<CompactScoreBarProps> = ({ label, range, value, total, gradient }) => {
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
                <div
                    className={`h-full bg-gradient-to-r ${gradient} transition-all duration-500 ease-out rounded-full`}
                    style={{ width: `${percentage}%` }}
                />
            </div>
        </div>
    );
};
