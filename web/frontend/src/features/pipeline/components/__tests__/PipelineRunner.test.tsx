/**
 * Tests for PipelineRunner component
 * Covers: src/features/pipeline/components/PipelineRunner.tsx
 */

import { render, screen, fireEvent } from '@testing-library/react';
import { PipelineRunner } from '../PipelineRunner';

vi.mock('@/hooks/usePipeline');

import { usePipeline } from '@/hooks/usePipeline';

const mockUsePipeline = vi.mocked(usePipeline);

const defaultHook = {
    runPipeline: vi.fn(),
    stopPipeline: vi.fn(),
    isRunning: false,
    isStopping: false,
    status: null,
    clearTask: vi.fn(),
    connectionState: 'idle',
    sseError: null,
    retrySSE: vi.fn(),
    activePipeline: null,
    isLoading: false,
    runPipelineError: null,
    stopPipelineError: null,
    uploadResume: vi.fn(),
    uploadResumeError: null,
    isUploading: false,
};

describe('PipelineRunner', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        mockUsePipeline.mockReturnValue(defaultHook as never);
    });

    describe('idle / ready state (no status)', () => {
        it('renders the Matching Pipeline heading', () => {
            render(<PipelineRunner />);
            expect(screen.getByText('Matching Pipeline')).toBeTruthy();
        });

        it('shows "Ready to match" subtitle when not running', () => {
            render(<PipelineRunner />);
            expect(screen.getByText('Ready to match')).toBeTruthy();
        });

        it('renders Run Matching button', () => {
            render(<PipelineRunner />);
            expect(screen.getByRole('button', { name: /run matching/i })).toBeTruthy();
        });

        it('renders ready prompt when status is null', () => {
            render(<PipelineRunner />);
            expect(screen.getByText(/ready to start matching/i)).toBeTruthy();
        });

        it('calls runPipeline when Run Matching is clicked', () => {
            render(<PipelineRunner />);
            fireEvent.click(screen.getByRole('button', { name: /run matching/i }));
            expect(defaultHook.runPipeline).toHaveBeenCalledTimes(1);
        });

        it('disables Run Matching button when isRunning is true', () => {
            mockUsePipeline.mockReturnValue({ ...defaultHook, isRunning: true } as never);
            render(<PipelineRunner />);
            const btn = screen.getByRole('button', { name: /run matching/i });
            expect((btn as HTMLButtonElement).disabled).toBe(true);
        });
    });

    describe('running state', () => {
        const runningHook = {
            ...defaultHook,
            isRunning: true,
            status: { status: 'running', step: 'scoring' },
            connectionState: 'connected',
        };

        beforeEach(() => {
            mockUsePipeline.mockReturnValue(runningHook as never);
        });

        it('shows "Processing..." subtitle', () => {
            render(<PipelineRunner />);
            expect(screen.getByText('Processing...')).toBeTruthy();
        });

        it('renders Stop Matching button instead of Run Matching', () => {
            render(<PipelineRunner />);
            expect(screen.getByRole('button', { name: /stop matching/i })).toBeTruthy();
            expect(screen.queryByRole('button', { name: /run matching/i })).toBeNull();
        });

        it('calls stopPipeline when Stop Matching is clicked', () => {
            render(<PipelineRunner />);
            fireEvent.click(screen.getByRole('button', { name: /stop matching/i }));
            expect(defaultHook.stopPipeline).toHaveBeenCalledTimes(1);
        });

        it('disables Stop Matching button when isStopping is true', () => {
            mockUsePipeline.mockReturnValue({ ...runningHook, isStopping: true } as never);
            render(<PipelineRunner />);
            const btn = screen.getByRole('button', { name: /stop matching/i });
            expect((btn as HTMLButtonElement).disabled).toBe(true);
        });

        it('shows RUNNING badge', () => {
            render(<PipelineRunner />);
            expect(screen.getByText('RUNNING')).toBeTruthy();
        });

        it('shows Scoring Candidates step label', () => {
            render(<PipelineRunner />);
            expect(screen.getByText('Scoring Candidates')).toBeTruthy();
        });

        it.each([
            ['connected',    'Live'],
            ['connecting',   'Connecting...'],
            ['reconnecting', 'Reconnecting...'],
            ['failed',       'Disconnected'],
        ])('shows "%s" connection indicator', (state, label) => {
            mockUsePipeline.mockReturnValue({ ...runningHook, connectionState: state } as never);
            render(<PipelineRunner />);
            expect(screen.getByText(label)).toBeTruthy();
        });

        it('shows pending status badge', () => {
            mockUsePipeline.mockReturnValue({
                ...runningHook,
                status: { status: 'pending', step: 'initializing' },
            } as never);
            render(<PipelineRunner />);
            expect(screen.getByText('PENDING')).toBeTruthy();
        });

        it('maps known step keys to readable labels', () => {
            const steps: [string, string][] = [
                ['loading_resume', 'Loading Resume'],
                ['vector_matching', 'Finding Potential Matches'],
                ['saving_results', 'Saving Results'],
                ['notifying', 'Sending Notifications'],
                ['initializing', 'Initializing Pipeline'],
            ];
            for (const [step, label] of steps) {
                mockUsePipeline.mockReturnValue({
                    ...runningHook,
                    status: { status: 'running', step },
                } as never);
                const { unmount } = render(<PipelineRunner />);
                expect(screen.getByText(label)).toBeTruthy();
                unmount();
            }
        });

        it('falls back to "Processing" for unknown step', () => {
            mockUsePipeline.mockReturnValue({
                ...runningHook,
                status: { status: 'running', step: 'unknown_step' },
            } as never);
            render(<PipelineRunner />);
            expect(screen.getByText('Processing')).toBeTruthy();
        });

        it('shows "Pipeline Running" when step is undefined', () => {
            mockUsePipeline.mockReturnValue({
                ...runningHook,
                status: { status: 'running' },
            } as never);
            render(<PipelineRunner />);
            expect(screen.getByText('Pipeline Running')).toBeTruthy();
        });
    });

    describe('completed state', () => {
        const completedHook = {
            ...defaultHook,
            status: {
                status: 'completed',
                matches_count: 42,
                saved_count: 38,
                execution_time: 12.5,
            },
        };

        beforeEach(() => {
            mockUsePipeline.mockReturnValue(completedHook as never);
        });

        it('shows COMPLETED badge', () => {
            render(<PipelineRunner />);
            expect(screen.getByText('COMPLETED')).toBeTruthy();
        });

        it('shows success message', () => {
            render(<PipelineRunner />);
            expect(screen.getByText(/pipeline completed successfully/i)).toBeTruthy();
        });

        it('shows matches count', () => {
            render(<PipelineRunner />);
            expect(screen.getByText('42')).toBeTruthy();
        });

        it('shows saved count', () => {
            render(<PipelineRunner />);
            expect(screen.getByText('38')).toBeTruthy();
        });

        it('shows execution time', () => {
            render(<PipelineRunner />);
            expect(screen.getByText('12.5s')).toBeTruthy();
        });

        it('shows Clear Status button and calls clearTask on click', () => {
            render(<PipelineRunner />);
            const btn = screen.getByRole('button', { name: /clear status/i });
            expect(btn).toBeTruthy();
            fireEvent.click(btn);
            expect(defaultHook.clearTask).toHaveBeenCalledTimes(1);
        });

        it('shows 0 for missing matches_count', () => {
            mockUsePipeline.mockReturnValue({
                ...completedHook,
                status: { status: 'completed', execution_time: 5.0 },
            } as never);
            render(<PipelineRunner />);
            // Two 0s for matches and saved
            const zeros = screen.getAllByText('0');
            expect(zeros.length).toBeGreaterThanOrEqual(2);
        });
    });

    describe('failed state', () => {
        const failedHook = {
            ...defaultHook,
            status: {
                status: 'failed',
                error: 'No resume found in database',
            },
        };

        beforeEach(() => {
            mockUsePipeline.mockReturnValue(failedHook as never);
        });

        it('shows FAILED badge', () => {
            render(<PipelineRunner />);
            expect(screen.getByText('FAILED')).toBeTruthy();
        });

        it('shows failure message', () => {
            render(<PipelineRunner />);
            expect(screen.getByText(/pipeline execution failed/i)).toBeTruthy();
        });

        it('shows error details', () => {
            render(<PipelineRunner />);
            expect(screen.getByText('No resume found in database')).toBeTruthy();
        });

        it('shows Error Details label', () => {
            render(<PipelineRunner />);
            expect(screen.getByText(/error details/i)).toBeTruthy();
        });

        it('shows Clear Status button and calls clearTask on click', () => {
            render(<PipelineRunner />);
            const btn = screen.getByRole('button', { name: /clear status/i });
            expect(btn).toBeTruthy();
            fireEvent.click(btn);
            expect(defaultHook.clearTask).toHaveBeenCalledTimes(1);
        });

        it('does not show error section when error is undefined', () => {
            mockUsePipeline.mockReturnValue({
                ...failedHook,
                status: { status: 'failed' },
            } as never);
            render(<PipelineRunner />);
            expect(screen.queryByText(/error details/i)).toBeNull();
        });
    });

    describe('SSE error state', () => {
        it('shows Connection Issue banner when sseError is set and not in a terminal status', () => {
            mockUsePipeline.mockReturnValue({
                ...defaultHook,
                status: { status: 'idle' },
                sseError: 'Connection timed out',
            } as never);
            render(<PipelineRunner />);
            expect(screen.getByText('Connection Issue')).toBeTruthy();
            expect(screen.getByText('Connection timed out')).toBeTruthy();
        });

        it('shows Retry Connection button', () => {
            mockUsePipeline.mockReturnValue({
                ...defaultHook,
                status: { status: 'idle' },
                sseError: 'Dropped',
            } as never);
            render(<PipelineRunner />);
            expect(screen.getByRole('button', { name: /retry connection/i })).toBeTruthy();
        });

        it('calls retrySSE when Retry Connection is clicked', () => {
            mockUsePipeline.mockReturnValue({
                ...defaultHook,
                status: { status: 'idle' },
                sseError: 'Dropped',
            } as never);
            render(<PipelineRunner />);
            fireEvent.click(screen.getByRole('button', { name: /retry connection/i }));
            expect(defaultHook.retrySSE).toHaveBeenCalledTimes(1);
        });

        it('hides Connection Issue banner when status is running', () => {
            mockUsePipeline.mockReturnValue({
                ...defaultHook,
                isRunning: true,
                status: { status: 'running' },
                sseError: 'Some error',
            } as never);
            render(<PipelineRunner />);
            expect(screen.queryByText('Connection Issue')).toBeNull();
        });

        it('hides Connection Issue banner when status is completed', () => {
            mockUsePipeline.mockReturnValue({
                ...defaultHook,
                status: { status: 'completed', execution_time: 1.0 },
                sseError: 'Some error',
            } as never);
            render(<PipelineRunner />);
            expect(screen.queryByText('Connection Issue')).toBeNull();
        });

        it('hides Connection Issue banner when status is failed', () => {
            mockUsePipeline.mockReturnValue({
                ...defaultHook,
                status: { status: 'failed' },
                sseError: 'Some error',
            } as never);
            render(<PipelineRunner />);
            expect(screen.queryByText('Connection Issue')).toBeNull();
        });
    });
});
