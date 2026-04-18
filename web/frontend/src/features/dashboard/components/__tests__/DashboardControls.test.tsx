import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { vi } from 'vitest';
import { toast } from 'sonner';

import { DashboardControls } from '../DashboardControls';
import { usePipeline } from '@/hooks/usePipeline';
import { useStats } from '@/hooks/useStats';

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

    beforeEach(() => {
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
            resumeProcessingStep: undefined,
        });

        mockUseStats.mockReturnValue({ data: null });

        mockUploadResume.mockResolvedValue({
            alreadyExists: false,
            message: 'Resume uploaded successfully',
        });
    });

    describe('resume upload', () => {
        it('shows "Upload resume" when no file is present', () => {
            render(<DashboardControls />, { wrapper: createWrapper() });
            expect(screen.getByText('Upload resume')).toBeInTheDocument();
        });

        it('shows "Replace resume" and the filename after a successful upload', async () => {
            render(<DashboardControls />, { wrapper: createWrapper() });

            const fileInput = screen.getByTestId('resume-file-input');
            const file = new File([JSON.stringify({ name: 'Test' })], 'my-resume.json', {
                type: 'application/json',
            });

            await userEvent.upload(fileInput, file);

            await waitFor(() => {
                expect(screen.getByText('Replace resume')).toBeInTheDocument();
            });

            expect(screen.getByText('my-resume.json')).toBeInTheDocument();
            expect(toast.success).toHaveBeenCalledWith('Resume uploaded successfully');
        });

        it('shows upload failures with the new toast copy', async () => {
            mockUploadResume.mockRejectedValue(new Error('Network error'));

            render(<DashboardControls />, { wrapper: createWrapper() });

            const fileInput = screen.getByTestId('resume-file-input');
            const file = new File([JSON.stringify({ name: 'Test' })], 'resume.json', {
                type: 'application/json',
            });

            await userEvent.upload(fileInput, file);

            await waitFor(() => {
                expect(toast.error).toHaveBeenCalledWith('Resume upload failed: Network error');
            });
        });

        it('disables the upload affordance while uploading or running', () => {
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

            const { rerender } = render(<DashboardControls />, { wrapper: createWrapper() });
            expect(screen.getByText('Uploading').closest('button')).toBeDisabled();

            mockUsePipeline.mockReturnValue({
                runPipeline: mockRunPipeline,
                stopPipeline: mockStopPipeline,
                isRunning: true,
                isStopping: false,
                status: { status: 'running', step: 'loading_resume' },
                uploadResume: mockUploadResume,
                isUploading: false,
                isPreparingResume: false,
                resumeProcessingStep: undefined,
            });

            rerender(<DashboardControls />);
            expect(screen.getByText('Upload resume').closest('button')).toBeDisabled();
        });

        it('accepts supported file formats and forwards the uploaded file', async () => {
            render(<DashboardControls />, { wrapper: createWrapper() });

            const file = new File([JSON.stringify({ name: 'Test' })], 'resume.json', {
                type: 'application/json',
            });
            const fileInput = screen.getByTestId('resume-file-input');

            expect(fileInput).toHaveAttribute('accept', '.json,.yaml,.yml,.txt,.docx,.pdf');
            await userEvent.upload(fileInput, file);

            await waitFor(() => {
                expect(mockUploadResume).toHaveBeenCalledWith(file);
            });
        });

        it('blocks oversized files with the updated size warning', async () => {
            const file = new File(['x'], 'big.pdf', { type: 'application/pdf' });
            Object.defineProperty(file, 'size', { value: 3 * 1024 * 1024 });

            render(<DashboardControls />, { wrapper: createWrapper() });
            await userEvent.upload(screen.getByTestId('resume-file-input'), file);

            await waitFor(() => {
                expect(toast.error).toHaveBeenCalledWith('That file is over 2MB. Try a smaller one.');
            });

            expect(mockUploadResume).not.toHaveBeenCalled();
        });

        it('shows the saved-resume toast when the upload already exists', async () => {
            mockUploadResume.mockResolvedValue({
                alreadyExists: true,
                message: 'An identical resume has already been uploaded.',
            });

            render(<DashboardControls />, { wrapper: createWrapper() });
            const file = new File(['content'], 'existing-resume.json', { type: 'application/json' });
            await userEvent.upload(screen.getByTestId('resume-file-input'), file);

            await waitFor(() => {
                expect(toast.success).toHaveBeenCalledWith('This resume is already saved.');
            });
        });

        it('disables the run button while the resume is being prepared', () => {
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
            expect(screen.getByText('Preparing').closest('button')).toBeDisabled();
        });
    });

    describe('run matching', () => {
        it('calls runPipeline with an error callback when clicked', async () => {
            render(<DashboardControls />, { wrapper: createWrapper() });
            await userEvent.click(screen.getByText('Run matching'));

            await waitFor(() => {
                expect(mockRunPipeline).toHaveBeenCalledWith(expect.any(Function));
            });
        });

        it('surfaces runPipeline errors via toast', async () => {
            mockRunPipeline.mockImplementation((onError?: (msg: string) => void) => {
                onError?.('No resume found in browser storage. Please upload a resume first.');
            });

            render(<DashboardControls />, { wrapper: createWrapper() });
            await userEvent.click(screen.getByText('Run matching'));

            await waitFor(() => {
                expect(toast.error).toHaveBeenCalledWith(
                    'No resume found in browser storage. Please upload a resume first.'
                );
            });
        });
    });

    describe('mount behavior', () => {
        it('loads and displays an existing resume filename from IndexedDB', async () => {
            const { getResumeFilename } = await import('@/utils/indexedDB');
            (getResumeFilename as any).mockResolvedValue('saved-resume.pdf');

            render(<DashboardControls />, { wrapper: createWrapper() });

            await waitFor(() => {
                expect(screen.getByText('Replace resume')).toBeInTheDocument();
            });
        });

        it('stays on "Upload resume" when no filename is stored', async () => {
            const { getResumeFilename } = await import('@/utils/indexedDB');
            (getResumeFilename as any).mockResolvedValue(null);

            render(<DashboardControls />, { wrapper: createWrapper() });

            await waitFor(() => {
                expect(screen.getByText('Upload resume')).toBeInTheDocument();
            });
        });

        it('handles IndexedDB errors without crashing', async () => {
            const { getResumeFilename } = await import('@/utils/indexedDB');
            (getResumeFilename as any).mockRejectedValue(new Error('IndexedDB unavailable'));

            expect(() => render(<DashboardControls />, { wrapper: createWrapper() })).not.toThrow();
        });
    });

    describe('stats and status', () => {
        it('renders non-zero stats and the idle action label', () => {
            mockUseStats.mockReturnValue({
                data: {
                    total_matches: 100,
                    active_matches: 60,
                    hidden_count: 20,
                    below_threshold_count: 20,
                },
            });

            render(<DashboardControls />, { wrapper: createWrapper() });
            expect(screen.getByText('100')).toBeInTheDocument();
            expect(screen.getByText('Run matching')).toBeInTheDocument();
        });

        it('switches the action button to stop while the pipeline is running', () => {
            mockUsePipeline.mockReturnValue({
                runPipeline: mockRunPipeline,
                stopPipeline: mockStopPipeline,
                isRunning: true,
                isStopping: false,
                status: { status: 'running', step: 'matching' },
                uploadResume: mockUploadResume,
                isUploading: false,
                isPreparingResume: false,
                resumeProcessingStep: undefined,
            });

            render(<DashboardControls />, { wrapper: createWrapper() });
            expect(screen.getByText('Stop')).toBeInTheDocument();
        });

        it('renders the updated pending status copy', () => {
            mockUsePipeline.mockReturnValue({
                runPipeline: mockRunPipeline,
                stopPipeline: mockStopPipeline,
                isRunning: true,
                isStopping: false,
                status: { task_id: 'task-1', status: 'pending', step: 'initializing' },
                uploadResume: mockUploadResume,
                isUploading: false,
                isPreparingResume: false,
                resumeProcessingStep: undefined,
            });

            render(<DashboardControls />, { wrapper: createWrapper() });
            expect(screen.getByText('Active')).toBeInTheDocument();
            expect(screen.getByText('Starting up')).toBeInTheDocument();
        });

        it('renders the completed state alongside the idle action label', () => {
            mockUsePipeline.mockReturnValue({
                runPipeline: mockRunPipeline,
                stopPipeline: mockStopPipeline,
                isRunning: false,
                isStopping: false,
                status: { status: 'completed', step: 'done' },
                uploadResume: mockUploadResume,
                isUploading: false,
                isPreparingResume: false,
                resumeProcessingStep: undefined,
            });

            render(<DashboardControls />, { wrapper: createWrapper() });
            expect(screen.getByText('Complete')).toBeInTheDocument();
            expect(screen.getByText('Run matching')).toBeInTheDocument();
        });
    });
});
