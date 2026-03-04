import React from 'react';
import { FileUp } from 'lucide-react';
import { RESUME_MAX_SIZE_MB } from '@shared/constants';

export interface ResumeUploadSectionProps {
    fileInputRef: React.RefObject<HTMLInputElement | null>;
    onUpload: (e: React.ChangeEvent<HTMLInputElement>) => void;
    isUploading: boolean;
    isRunning: boolean;
    filename: string | null;
}

export const ResumeUploadSection: React.FC<ResumeUploadSectionProps> = ({ fileInputRef, onUpload, isUploading, isRunning, filename }) => (
    <>
        <button
            onClick={() => fileInputRef.current?.click()}
            disabled={isRunning || isUploading}
            className="w-full lg:w-auto px-6 py-4 border-2 border-gray-300 text-gray-700 font-semibold rounded-xl hover:border-blue-500 hover:text-blue-600 hover:bg-blue-50 transition-all duration-200 flex flex-col items-center justify-center gap-1 min-w-[160px] relative group shadow-lg hover:shadow-2xl hover:scale-105 active:scale-95 disabled:opacity-50 disabled:cursor-not-allowed disabled:hover:scale-100 disabled:active:scale-100 disabled:hover:shadow-lg"
        >
            <span className="flex items-center gap-2 text-base">
                {!filename && <FileUp className="w-5 h-5 sm:w-6 sm:h-6 shrink-0" />}
                <span>{filename ? 'Update Resume' : 'Upload Resume'}</span>
            </span>
            {filename && <span className="text-xs opacity-70 truncate max-w-[200px]">{filename}</span>}
            <span className="absolute -top-8 left-1/2 -translate-x-1/2 bg-gray-800 text-white text-xs px-2 py-1 rounded opacity-0 group-hover:opacity-100 transition-opacity whitespace-nowrap pointer-events-none">
                {filename || `Upload Resume (max ${RESUME_MAX_SIZE_MB}MB)`}
            </span>
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
