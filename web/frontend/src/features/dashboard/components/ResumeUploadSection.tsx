import React from 'react';
import { FileText, Loader2, Upload } from 'lucide-react';
import { RESUME_MAX_SIZE_MB } from '@shared/constants';

const RESUME_STEP_LABELS: Record<string, string> = {
    extracting: 'Parsing resume',
    embedding: 'Building vectors',
};

export interface ResumeUploadSectionProps {
    fileInputRef: React.RefObject<HTMLInputElement | null>;
    onUpload: (e: React.ChangeEvent<HTMLInputElement>) => void;
    isUploading: boolean;
    isRunning: boolean;
    filename: string | null;
    processingStep?: string | null;
}

export const ResumeUploadSection: React.FC<ResumeUploadSectionProps> = ({
    fileInputRef,
    onUpload,
    isUploading,
    isRunning,
    filename,
    processingStep,
}) => {
    const uploadingLabel = processingStep
        ? (RESUME_STEP_LABELS[processingStep] ?? 'Processing')
        : 'Uploading';

    let label = 'Upload resume';
    if (isUploading) label = uploadingLabel;
    else if (filename) label = 'Replace resume';

    const Icon = isUploading ? Loader2 : filename ? FileText : Upload;

    return (
        <>
            <button
                type="button"
                onClick={() => fileInputRef.current?.click()}
                disabled={isRunning || isUploading}
                className="group inline-flex h-10 w-full items-center justify-center gap-2 rounded-md border border-rule bg-surface text-[14px] font-medium text-ink transition-colors duration-200 hover:border-rule-strong disabled:opacity-60 disabled:cursor-not-allowed"
                title={filename ?? `Upload resume (max ${RESUME_MAX_SIZE_MB}MB)`}
            >
                <Icon className={`h-3.5 w-3.5 text-ink-muted ${isUploading ? 'animate-spin' : ''}`} aria-hidden="true" />
                <span>{label}</span>
            </button>
            {filename && !isUploading && (
                <p className="truncate text-[12px] text-ink-muted" title={filename}>
                    {filename}
                </p>
            )}
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
};
