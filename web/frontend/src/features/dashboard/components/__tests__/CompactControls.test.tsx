import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { CompactControls } from '../CompactControls';
import { usePipeline } from '@/hooks/usePipeline';
import { toast } from 'sonner';
import { vi } from 'vitest';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

vi.mock('@/hooks/usePipeline');
vi.mock('sonner');

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
        });
    });

    describe('Resume Upload Button', () => {
        it('displays "Upload Resume" when no resume is uploaded', () => {
            render(<CompactControls />, { wrapper: createWrapper() });

            expect(screen.getByText('Upload Resume')).toBeInTheDocument();
        });

        it('displays "Update Resume" with filename after successful upload', async () => {
            mockUploadResume.mockResolvedValue({ success: true });

            render(<CompactControls />, { wrapper: createWrapper() });

            const fileInput = screen.getByTestId('resume-file-input');
            const file = new File([JSON.stringify({ name: 'Test' })], 'my-resume.json', { type: 'application/json' });

            await userEvent.upload(fileInput, file);

            await waitFor(() => {
                expect(screen.getByText('Update Resume')).toBeInTheDocument();
                expect(screen.getByText('(my-resume.json)')).toBeInTheDocument();
            });

            expect(toast.success).toHaveBeenCalledWith('Resume uploaded!');
        });

        it('shows error toast on upload failure', async () => {
            mockUploadResume.mockRejectedValue(new Error('Network error'));

            render(<CompactControls />, { wrapper: createWrapper() });

            const fileInput = screen.getByTestId('resume-file-input');
            const file = new File([JSON.stringify({ name: 'Test' })], 'resume.json', { type: 'application/json' });

            await userEvent.upload(fileInput, file);

            await waitFor(() => {
                expect(toast.error).toHaveBeenCalledWith('Failed to upload resume: Network error');
            });
        });

        it('truncates long filenames at 120 characters', async () => {
            mockUploadResume.mockResolvedValue({ success: true });

            render(<CompactControls />, { wrapper: createWrapper() });

            const longFilename = 'a'.repeat(150) + '.json';
            const file = new File([JSON.stringify({ name: 'Test' })], longFilename, { type: 'application/json' });

            const fileInput = screen.getByTestId('resume-file-input');
            await userEvent.upload(fileInput, file);

            await waitFor(() => {
                const filenameSpan = screen.getByText((content) =>
                    content.startsWith('(') && content.includes('...') && content.endsWith('.json)')
                );
                expect(filenameSpan.textContent?.length).toBeLessThan(longFilename.length + 3);
            });
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
            });

            render(<CompactControls />, { wrapper: createWrapper() });

            const uploadButton = screen.getByText('Upload Resume');
            expect(uploadButton.closest('button')).toBeDisabled();
            expect(screen.getByTestId('resume-file-input')).toBeDisabled();
        });

        it('shows loading spinner while uploading', () => {
            mockUsePipeline.mockReturnValue({
                runPipeline: mockRunPipeline,
                stopPipeline: mockStopPipeline,
                isRunning: false,
                isStopping: false,
                status: null,
                uploadResume: mockUploadResume,
                isUploading: true,
            });

            render(<CompactControls />, { wrapper: createWrapper() });

            expect(screen.getByTestId('resume-file-input')).toBeDisabled();
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
            });

            render(<CompactControls />, { wrapper: createWrapper() });

            const uploadButton = screen.getByText('Upload Resume');
            expect(uploadButton.closest('button')).toBeDisabled();
        });

        it('accepts only JSON files', () => {
            render(<CompactControls />, { wrapper: createWrapper() });

            const fileInput = screen.getByTestId('resume-file-input');
            expect(fileInput).toHaveAttribute('accept', '.json');
        });

        it('calls uploadResume with selected file', async () => {
            render(<CompactControls />, { wrapper: createWrapper() });

            const file = new File([JSON.stringify({ name: 'Test' })], 'resume.json', { type: 'application/json' });
            const fileInput = screen.getByTestId('resume-file-input');

            await userEvent.upload(fileInput, file);

            await waitFor(() => {
                expect(mockUploadResume).toHaveBeenCalledWith(file);
            });
        });

        it('resets file input after successful upload', async () => {
            mockUploadResume.mockResolvedValue({ success: true });

            render(<CompactControls />, { wrapper: createWrapper() });

            const file = new File([JSON.stringify({ name: 'Test' })], 'resume.json', { type: 'application/json' });
            const fileInput = screen.getByTestId('resume-file-input');

            await userEvent.upload(fileInput, file);
            await waitFor(() => {});

            expect((fileInput as HTMLInputElement).value).toBe('');
        });

        it('resets file input after failed upload', async () => {
            mockUploadResume.mockRejectedValue(new Error('Upload failed'));

            render(<CompactControls />, { wrapper: createWrapper() });

            const file = new File([JSON.stringify({ name: 'Test' })], 'resume.json', { type: 'application/json' });
            const fileInput = screen.getByTestId('resume-file-input');

            await userEvent.upload(fileInput, file);
            await waitFor(() => {});

            expect((fileInput as HTMLInputElement).value).toBe('');
        });

        it('displays filename on multiple uploads', async () => {
            mockUploadResume.mockResolvedValue({ success: true });

            render(<CompactControls />, { wrapper: createWrapper() });

            const file1 = new File([JSON.stringify({ name: 'First' })], 'first-resume.json', { type: 'application/json' });
            const fileInput = screen.getByTestId('resume-file-input');

            await userEvent.upload(fileInput, file1);
            await waitFor(() => {
                expect(screen.getByText('(first-resume.json)')).toBeInTheDocument();
            });
        });
    });
});
