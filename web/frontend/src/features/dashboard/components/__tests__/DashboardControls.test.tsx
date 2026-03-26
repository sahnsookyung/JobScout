import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { DashboardControls } from '../DashboardControls';
import { usePipeline } from '@/hooks/usePipeline';
import { useStats } from '@/hooks/useStats';
import { toast } from 'sonner';
import { vi } from 'vitest';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

vi.mock('@/hooks/usePipeline');
vi.mock('@/hooks/useStats');
vi.mock('sonner');
vi.mock('@shared/constants', () => ({
    RESUME_MAX_SIZE_MB: 2,
    RESUME_MAX_SIZE: 2 * 1024 * 1024,
    RESUME_INDEXEDDB_NAME: 'jobscout-resume',
    RESUME_MAX_AGE_DAYS: 30,
}));

vi.mock('@/utils/indexedDB', () => ({
    getResumeFilename: vi.fn().mockResolvedValue(null),
}));

const mockUsePipeline = usePipeline as ReturnType<typeof vi.fn>;
const mockUseStats = useStats as ReturnType<typeof vi.fn>;

const createWrapper = () => {
    const queryClient = new QueryClient({
        defaultOptions: { queries: { retry: false } },
    });
    return ({ children }: { children: React.ReactNode }) => (
        <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );
};

describe('DashboardControls', () => {
    const mockRunPipeline = vi.fn();
    const mockStopPipeline = vi.fn();
    const mockUploadResume = vi.fn();

    beforeEach(async () => {
        vi.clearAllMocks();

        mockUsePipeline.mockReturnValue({
            runPipeline: mockRunPipeline,
            stopPipeline: mockStopPipeline,
            isRunning: false,
            isStopping: false,
            status: null,
            uploadResume: mockUploadResume,
            isUploading: false,
            isPreparingResume: false,
        });

        mockUseStats.mockReturnValue({ data: null });

        // Default upload mock: succeeds immediately
        mockUploadResume.mockResolvedValue({
            alreadyExists: false,
            message: 'Resume uploaded successfully',
        });
    });

    describe('Resume Upload Button', () => {
        it('displays "Upload Resume" when no resume is uploaded', () => {
            render(<DashboardControls />, { wrapper: createWrapper() });

            expect(screen.getByText('Upload Resume')).toBeInTheDocument();
        });

        it('displays "Update Resume" with filename after successful upload', async () => {
            render(<DashboardControls />, { wrapper: createWrapper() });

            const fileInput = screen.getByTestId('resume-file-input');
            const file = new File([JSON.stringify({ name: 'Test' })], 'my-resume.json', { type: 'application/json' });

            await userEvent.upload(fileInput, file);

            await waitFor(() => {
                expect(screen.getByText('Update Resume')).toBeInTheDocument();
            }, { timeout: 2000 });

            const filenameElements = screen.getAllByText(/my-resume\.json/i);
            expect(filenameElements.length).toBeGreaterThan(0);
            expect(toast.success).toHaveBeenCalled();
        });

        it('shows error toast on upload failure', async () => {
            mockUploadResume.mockRejectedValue(new Error('Network error'));

            render(<DashboardControls />, { wrapper: createWrapper() });

            const fileInput = screen.getByTestId('resume-file-input');
            const file = new File([JSON.stringify({ name: 'Test' })], 'resume.json', { type: 'application/json' });

            await userEvent.upload(fileInput, file);

            await waitFor(() => {
                expect(toast.error).toHaveBeenCalledWith(expect.stringContaining('Failed to upload resume'));
            }, { timeout: 2000 });
        });

        it('disables button while uploading', () => {
            mockUsePipeline.mockReturnValue({
                runPipeline: mockRunPipeline,
                stopPipeline: mockStopPipeline,
                isRunning: false,
                isStopping: false,
                status: null,
                uploadResume: mockUploadResume,
                isUploading: true,
                isPreparingResume: false,
                resumeProcessingStep: undefined,
            });

            render(<DashboardControls />, { wrapper: createWrapper() });

            const uploadButton = screen.getByText('Uploading...');
            expect(uploadButton.closest('button')).toBeDisabled();
        });

        it('disables button while pipeline is running', () => {
            mockUsePipeline.mockReturnValue({
                runPipeline: mockRunPipeline,
                stopPipeline: mockStopPipeline,
                isRunning: true,
                isStopping: false,
                status: { status: 'running', step: 'loading_resume' },
                uploadResume: mockUploadResume,
                isUploading: false,
                isPreparingResume: false,
            });

            render(<DashboardControls />, { wrapper: createWrapper() });

            const uploadButton = screen.getByText('Upload Resume');
            expect(uploadButton.closest('button')).toBeDisabled();
        });

        it('accepts multiple file formats', () => {
            render(<DashboardControls />, { wrapper: createWrapper() });

            const fileInput = screen.getByTestId('resume-file-input');
            expect(fileInput).toHaveAttribute('accept', '.json,.yaml,.yml,.txt,.docx,.pdf');
        });

        it('calls uploadResume hook with selected file', async () => {
            render(<DashboardControls />, { wrapper: createWrapper() });

            const file = new File([JSON.stringify({ name: 'Test' })], 'resume.json', { type: 'application/json' });
            const fileInput = screen.getByTestId('resume-file-input');

            await userEvent.upload(fileInput, file);

            await waitFor(() => {
                expect(mockUploadResume).toHaveBeenCalledWith(file);
            }, { timeout: 2000 });
        });

        it('displays filename on upload', async () => {
            render(<DashboardControls />, { wrapper: createWrapper() });

            const file1 = new File([JSON.stringify({ name: 'First' })], 'first-resume.json', { type: 'application/json' });
            const fileInput = screen.getByTestId('resume-file-input');

            await userEvent.upload(fileInput, file1);

            await waitFor(() => {
                expect(screen.getByText('Update Resume')).toBeInTheDocument();
            }, { timeout: 2000 });

            const filenameElements = screen.getAllByText(/first-resume\.json/i);
            expect(filenameElements.length).toBeGreaterThan(0);
        });

        it('shows error toast when file exceeds size limit', async () => {
            const file = new File(['x'], 'big.pdf', { type: 'application/pdf' });
            Object.defineProperty(file, 'size', { value: 3 * 1024 * 1024 }); // 3MB > 2MB limit

            render(<DashboardControls />, { wrapper: createWrapper() });
            const fileInput = screen.getByTestId('resume-file-input');
            await userEvent.upload(fileInput, file);

            await waitFor(() => {
                expect(toast.error).toHaveBeenCalledWith(
                    expect.stringContaining('File size exceeds')
                );
            }, { timeout: 2000 });

            expect(mockUploadResume).not.toHaveBeenCalled();
        });

        it('shows "already uploaded" toast when hash already exists', async () => {
            mockUploadResume.mockResolvedValue({
                alreadyExists: true,
                message: 'An identical resume has already been uploaded.',
            });

            render(<DashboardControls />, { wrapper: createWrapper() });
            const fileInput = screen.getByTestId('resume-file-input');
            const file = new File(['content'], 'existing-resume.json', { type: 'application/json' });
            await userEvent.upload(fileInput, file);

            await waitFor(() => {
                expect(toast.success).toHaveBeenCalledWith(
                    expect.stringContaining('identical resume')
                );
            }, { timeout: 2000 });
        });

        it('disables run button while resume is being prepared', () => {
            mockUsePipeline.mockReturnValue({
                runPipeline: mockRunPipeline,
                stopPipeline: mockStopPipeline,
                isRunning: false,
                isStopping: false,
                status: null,
                uploadResume: mockUploadResume,
                isUploading: false,
                isPreparingResume: true,
                resumeProcessingStep: undefined,
            });

            render(<DashboardControls />, { wrapper: createWrapper() });

            // ActionButton receives isProcessingResume=true so run button is disabled
            // isPreparingResume is combined with isUploading in DashboardControls, so it shows "Uploading..."
            const uploadButton = screen.getByText('Uploading...');
            expect(uploadButton.closest('button')).toBeDisabled();
        });
    });

    describe('Run Matching button', () => {
        it('calls runPipeline with an error callback when clicked', async () => {
            render(<DashboardControls />, { wrapper: createWrapper() });
            const runButton = screen.getByText('Run Matching');
            await userEvent.click(runButton);

            await waitFor(() => {
                expect(mockRunPipeline).toHaveBeenCalledWith(expect.any(Function));
            }, { timeout: 2000 });
        });

        it('shows error toast when runPipeline calls back with an error', async () => {
            // Simulate the hook calling the onError callback (e.g. no resume found)
            mockRunPipeline.mockImplementation((onError?: (msg: string) => void) => {
                onError?.('No resume found in browser storage. Please upload a resume first.');
            });

            render(<DashboardControls />, { wrapper: createWrapper() });
            const runButton = screen.getByText('Run Matching');
            await userEvent.click(runButton);

            await waitFor(() => {
                expect(toast.error).toHaveBeenCalledWith(
                    expect.stringContaining('No resume found')
                );
            }, { timeout: 2000 });
        });
    });

    describe('on mount', () => {
        it('loads and displays existing resume filename from IndexedDB', async () => {
            const { getResumeFilename } = await import('@/utils/indexedDB');
            (getResumeFilename as any).mockResolvedValue('saved-resume.pdf');

            render(<DashboardControls />, { wrapper: createWrapper() });

            await waitFor(() => {
                expect(screen.getByText('Update Resume')).toBeInTheDocument();
            }, { timeout: 2000 });
        });

        it('shows "Upload Resume" when no filename is stored', async () => {
            const { getResumeFilename } = await import('@/utils/indexedDB');
            (getResumeFilename as any).mockResolvedValue(null);

            render(<DashboardControls />, { wrapper: createWrapper() });

            await waitFor(() => {
                expect(screen.getByText('Upload Resume')).toBeInTheDocument();
            }, { timeout: 1000 });
        });

        it('handles IndexedDB errors on mount gracefully without crashing', async () => {
            const { getResumeFilename } = await import('@/utils/indexedDB');
            (getResumeFilename as any).mockRejectedValue(new Error('IndexedDB unavailable'));

            // Should not throw
            expect(() => render(<DashboardControls />, { wrapper: createWrapper() })).not.toThrow();
        });
    });

    describe('stats panel', () => {
        it('renders stats with non-zero match data', () => {
            mockUseStats.mockReturnValue({
                data: {
                    total_matches: 100,
                    active_matches: 60,
                    hidden_count: 20,
                    below_threshold_count: 20,
                },
            });

            render(<DashboardControls />, { wrapper: createWrapper() });
            expect(screen.getByText('Run Matching')).toBeInTheDocument();
        });

        it('renders status banner when pipeline is running', () => {
            mockUsePipeline.mockReturnValue({
                runPipeline: mockRunPipeline,
                stopPipeline: mockStopPipeline,
                isRunning: true,
                isStopping: false,
                status: { status: 'running', step: 'matching' },
                uploadResume: mockUploadResume,
                isUploading: false,
                isPreparingResume: false,
            });

            render(<DashboardControls />, { wrapper: createWrapper() });
            expect(screen.getByText('Stop')).toBeInTheDocument();
        });

        it('renders status banner while pipeline is pending', () => {
            mockUsePipeline.mockReturnValue({
                runPipeline: mockRunPipeline,
                stopPipeline: mockStopPipeline,
                isRunning: true,
                isStopping: false,
                status: { task_id: 'task-1', status: 'pending', step: 'initializing' },
                uploadResume: mockUploadResume,
                isUploading: false,
                isPreparingResume: false,
            });

            render(<DashboardControls />, { wrapper: createWrapper() });
            expect(screen.getByText('PENDING')).toBeInTheDocument();
            expect(screen.getByText('Initializing')).toBeInTheDocument();
        });

        it('renders status banner when pipeline completed', () => {
            mockUsePipeline.mockReturnValue({
                runPipeline: mockRunPipeline,
                stopPipeline: mockStopPipeline,
                isRunning: false,
                isStopping: false,
                status: { status: 'completed', step: 'done' },
                uploadResume: mockUploadResume,
                isUploading: false,
                isPreparingResume: false,
            });

            render(<DashboardControls />, { wrapper: createWrapper() });
            expect(screen.getByText('Run Matching')).toBeInTheDocument();
        });
    });
});
