import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { CompactControls } from '../CompactControls';
import { usePipeline } from '@/hooks/usePipeline';
import { toast } from 'sonner';
import { vi } from 'vitest';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

// Mock modules - vi.mock is hoisted, so factories run before this code
vi.mock('@/hooks/usePipeline');
vi.mock('sonner');
vi.mock('@shared/constants', () => ({
    RESUME_MAX_SIZE_MB: 2,
    RESUME_MAX_SIZE: 2 * 1024 * 1024,
    RESUME_INDEXEDDB_NAME: 'jobscout-resume',
    RESUME_MAX_AGE_DAYS: 30,
}));

vi.mock('@/utils/indexedDB', () => ({
    saveResume: vi.fn().mockResolvedValue(undefined),
    hasResume: vi.fn().mockResolvedValue(false),
    getResumeFilename: vi.fn().mockResolvedValue(null),
}));

vi.mock('@/services/pipelineApi', () => ({
    pipelineApi: {
        checkResumeHash: vi.fn().mockRejectedValue(new Error('Network error')),
        uploadResume: vi.fn().mockRejectedValue(new Error('Network error')),
    },
}));

const mockUsePipeline = usePipeline as ReturnType<typeof vi.fn>;

const createWrapper = () => {
    const queryClient = new QueryClient({
        defaultOptions: { queries: { retry: false } },
    });
    return ({ children }: { children: React.ReactNode }) => (
        <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );
};

describe('CompactControls', () => {
    const mockRunPipeline = vi.fn();
    const mockStopPipeline = vi.fn();
    const mockUploadResumeFromHook = vi.fn();

    beforeEach(async () => {
        // Reset all mocks to initial state
        vi.clearAllMocks();
        
        // Get fresh references to mocked functions and set up defaults
        const { pipelineApi } = await import('@/services/pipelineApi');
        const { hasResume, getResumeFilename, saveResume } = await import('@/utils/indexedDB');

        mockUsePipeline.mockReturnValue({
            runPipeline: mockRunPipeline,
            stopPipeline: mockStopPipeline,
            isRunning: false,
            isStopping: false,
            status: null,
            uploadResume: mockUploadResumeFromHook,
            isUploading: false,
        });

        (pipelineApi.checkResumeHash as any).mockRejectedValue(new Error('Network error'));
        (pipelineApi.uploadResume as any).mockRejectedValue(new Error('Network error'));
        (hasResume as any).mockResolvedValue(false);
        (getResumeFilename as any).mockResolvedValue(null);
        (saveResume as any).mockResolvedValue(undefined);
    });

    describe('Resume Upload Button', () => {
        it('displays "Upload Resume" when no resume is uploaded', () => {
            render(<CompactControls />, { wrapper: createWrapper() });

            expect(screen.getByText('Upload Resume')).toBeInTheDocument();
        });

        it('displays "Update Resume" with filename after successful upload', async () => {
            const { pipelineApi } = await import('@/services/pipelineApi');
            (pipelineApi.checkResumeHash as any).mockResolvedValue({ data: { exists: false } });
            (pipelineApi.uploadResume as any).mockResolvedValue({ data: { message: 'Resume uploaded successfully' } });

            render(<CompactControls />, { wrapper: createWrapper() });

            const fileInput = screen.getByTestId('resume-file-input');
            const file = new File([JSON.stringify({ name: 'Test' })], 'my-resume.json', { type: 'application/json' });

            await userEvent.upload(fileInput, file);

            await waitFor(() => {
                expect(screen.getByText('Update Resume')).toBeInTheDocument();
            }, { timeout: 1000 });
            
            // Check for filename in any element
            const filenameElements = screen.getAllByText(/my-resume\.json/i);
            expect(filenameElements.length).toBeGreaterThan(0);
            expect(toast.success).toHaveBeenCalled();
        });

        it('shows error toast on upload failure', async () => {
            const { pipelineApi } = await import('@/services/pipelineApi');
            (pipelineApi.checkResumeHash as any).mockResolvedValue({ data: { exists: false } });
            (pipelineApi.uploadResume as any).mockRejectedValue(new Error('Network error'));

            render(<CompactControls />, { wrapper: createWrapper() });

            const fileInput = screen.getByTestId('resume-file-input');
            const file = new File([JSON.stringify({ name: 'Test' })], 'resume.json', { type: 'application/json' });

            await userEvent.upload(fileInput, file);

            await waitFor(() => {
                expect(toast.error).toHaveBeenCalledWith(expect.stringContaining('Failed to upload resume'));
            }, { timeout: 1000 });
        });

        it('disables button while uploading', () => {
            mockUsePipeline.mockReturnValue({
                runPipeline: mockRunPipeline,
                stopPipeline: mockStopPipeline,
                isRunning: false,
                isStopping: false,
                status: null,
                uploadResume: mockUploadResumeFromHook,
                isUploading: true,
            });

            render(<CompactControls />, { wrapper: createWrapper() });

            const uploadButton = screen.getByText('Upload Resume');
            expect(uploadButton.closest('button')).toBeDisabled();
        });

        it('disables button while pipeline is running', () => {
            mockUsePipeline.mockReturnValue({
                runPipeline: mockRunPipeline,
                stopPipeline: mockStopPipeline,
                isRunning: true,
                isStopping: false,
                status: { status: 'running', step: 'loading_resume' },
                uploadResume: mockUploadResumeFromHook,
                isUploading: false,
            });

            render(<CompactControls />, { wrapper: createWrapper() });

            const uploadButton = screen.getByText('Upload Resume');
            expect(uploadButton.closest('button')).toBeDisabled();
        });

        it('accepts multiple file formats', () => {
            render(<CompactControls />, { wrapper: createWrapper() });

            const fileInput = screen.getByTestId('resume-file-input');
            expect(fileInput).toHaveAttribute('accept', '.json,.yaml,.yml,.txt,.docx,.pdf');
        });

        it('calls pipelineApi.uploadResume with selected file', async () => {
            const { pipelineApi } = await import('@/services/pipelineApi');
            (pipelineApi.checkResumeHash as any).mockResolvedValue({ data: { exists: false } });
            (pipelineApi.uploadResume as any).mockResolvedValue({ data: { message: 'Success' } });

            render(<CompactControls />, { wrapper: createWrapper() });

            const file = new File([JSON.stringify({ name: 'Test' })], 'resume.json', { type: 'application/json' });
            const fileInput = screen.getByTestId('resume-file-input');

            await userEvent.upload(fileInput, file);

            await waitFor(() => {
                expect(pipelineApi.uploadResume).toHaveBeenCalled();
            }, { timeout: 1000 });
        });

        it('displays filename on upload', async () => {
            const { pipelineApi } = await import('@/services/pipelineApi');
            (pipelineApi.checkResumeHash as any).mockResolvedValue({ data: { exists: false } });
            (pipelineApi.uploadResume as any).mockResolvedValue({ data: { message: 'Success' } });

            render(<CompactControls />, { wrapper: createWrapper() });

            const file1 = new File([JSON.stringify({ name: 'First' })], 'first-resume.json', { type: 'application/json' });
            const fileInput = screen.getByTestId('resume-file-input');

            await userEvent.upload(fileInput, file1);

            await waitFor(() => {
                expect(screen.getByText('Update Resume')).toBeInTheDocument();
            }, { timeout: 1000 });
            
            // Check filename is displayed
            const filenameElements = screen.getAllByText(/first-resume\.json/i);
            expect(filenameElements.length).toBeGreaterThan(0);
        });
    });
});
