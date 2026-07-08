import { render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { ActionButton, type ActionButtonProps } from '../ActionButton';
import { CompactScoreBar, type ScoreBarProps } from '../CompactScoreBar';
import { DashboardWrapper } from '../DashboardWrapper';
import { ResumeUploadSection, type ResumeUploadSectionProps } from '../ResumeUploadSection';
import { SegmentedCircle, type CircleChartProps } from '../SegmentedCircle';
import { StatsPanel, type StatsPanelProps } from '../StatsPanel';
import { StatusBanner, type StatusBannerProps } from '../StatusBanner';

vi.mock('lucide-react', () => ({
    FileText: ({ className, ...props }: any) => <div data-testid="file-text-icon" className={className} {...props} />,
    FileCheck2: ({ className, ...props }: any) => <div data-testid="file-check2-icon" className={className} {...props} />,
    Sparkles: ({ className, ...props }: any) => <div data-testid="sparkles-icon" className={className} {...props} />,
    Database: ({ className, ...props }: any) => <div data-testid="database-icon" className={className} {...props} />,
    SearchCheck: ({ className, ...props }: any) => <div data-testid="search-check-icon" className={className} {...props} />,
    Bell: ({ className, ...props }: any) => <div data-testid="bell-icon" className={className} {...props} />,
    Loader2: ({ className, ...props }: any) => <div data-testid="loader2-icon" className={className} {...props} />,
    Upload: ({ className, ...props }: any) => <div data-testid="upload-icon" className={className} {...props} />,
    CheckCircle2: ({ className, ...props }: any) => <div data-testid="check-circle2-icon" className={className} {...props} />,
    CircleSlash: ({ className, ...props }: any) => <div data-testid="circle-slash-icon" className={className} {...props} />,
    TriangleAlert: ({ className, ...props }: any) => <div data-testid="triangle-alert-icon" className={className} {...props} />,
    Play: ({ className, ...props }: any) => <div data-testid="play-icon" className={className} {...props} />,
    Square: ({ className, ...props }: any) => <div data-testid="square-icon" className={className} {...props} />,
}));

vi.mock('@shared/constants', () => ({
    RESUME_MAX_SIZE_MB: 5,
}));

describe('DashboardWrapper', () => {
    it('renders children content', () => {
        render(
            <DashboardWrapper>
                <div data-testid="child-content">Test Content</div>
            </DashboardWrapper>
        );

        expect(screen.getByTestId('child-content')).toBeInTheDocument();
        expect(screen.getByText('Test Content')).toBeInTheDocument();
    });

    it('applies the redesigned shell classes', () => {
        const { container } = render(
            <DashboardWrapper>
                <div>Content</div>
            </DashboardWrapper>
        );

        const wrapper = container.firstChild as HTMLElement;
        expect(wrapper).toHaveClass('border', 'border-rule', 'bg-surface');
        expect(container.querySelector(String.raw`.p-5.sm\:p-7`)).toBeInTheDocument();
    });
});

describe('ResumeUploadSection', () => {
    const defaultProps: ResumeUploadSectionProps = {
        fileInputRef: { current: null },
        onUpload: vi.fn(),
        isUploading: false,
        isRunning: false,
        filename: null,
    };

    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('displays "Upload resume" when no filename', () => {
        render(<ResumeUploadSection {...defaultProps} />);
        expect(screen.getByText('Upload resume')).toBeInTheDocument();
    });

    it('displays "Replace resume" with filename', () => {
        render(<ResumeUploadSection {...defaultProps} filename="resume.pdf" />);
        expect(screen.getByText('Replace resume')).toBeInTheDocument();
        expect(screen.getByText('resume.pdf')).toBeInTheDocument();
    });

    it('shows the file size limit in the button title', () => {
        render(<ResumeUploadSection {...defaultProps} />);
        expect(screen.getByRole('button')).toHaveAttribute('title', 'Upload resume (max 5MB)');
    });

    it('has hidden file input with correct accept attribute', () => {
        render(<ResumeUploadSection {...defaultProps} />);
        const fileInput = screen.getByTestId('resume-file-input');
        expect(fileInput).toHaveClass('hidden');
        expect(fileInput).toHaveAttribute('accept', '.json,.yaml,.yml,.txt,.docx,.pdf');
    });

    it('opens the file picker from the upload button', () => {
        const fileInputRef = { current: null as HTMLInputElement | null };

        render(<ResumeUploadSection {...defaultProps} fileInputRef={fileInputRef} />);

        const click = vi.spyOn(screen.getByTestId('resume-file-input'), 'click');
        screen.getByRole('button').click();

        expect(click).toHaveBeenCalledTimes(1);
    });

    it('calls onUpload when file is selected', () => {
        const mockOnUpload = vi.fn();
        render(<ResumeUploadSection {...defaultProps} onUpload={mockOnUpload} />);
        screen.getByTestId('resume-file-input').dispatchEvent(new Event('change', { bubbles: true }));
        expect(mockOnUpload).toHaveBeenCalledTimes(1);
    });

    it('disables the button when uploading or running', () => {
        const { rerender } = render(<ResumeUploadSection {...defaultProps} isUploading />);
        expect(screen.getByRole('button')).toBeDisabled();
        expect(screen.getByText('Uploading')).toBeInTheDocument();

        rerender(<ResumeUploadSection {...defaultProps} isRunning />);
        expect(screen.getByRole('button')).toBeDisabled();
    });

    it('shows the active processing-step label while uploading', () => {
        render(
            <ResumeUploadSection
                {...defaultProps}
                isUploading
                processingStep="embedding"
            />
        );

        expect(screen.getByText('Building vectors')).toBeInTheDocument();
    });
});

describe('StatusBanner', () => {
    const defaultProps: StatusBannerProps = {
        status: 'running',
        step: 'loading_resume',
    };

    it('renders pending status with initializing copy', () => {
        render(<StatusBanner status="pending" step="initializing" />);
        expect(screen.getByText('Active')).toBeInTheDocument();
        expect(screen.getByText('Starting up')).toBeInTheDocument();
        expect(screen.getByText('Starting your match run.')).toBeInTheDocument();
    });

    it('renders running status copy', () => {
        render(<StatusBanner {...defaultProps} />);
        expect(screen.getByText('Active')).toBeInTheDocument();
        expect(screen.getByText('Loading resume')).toBeInTheDocument();
        expect(screen.getByText('Working through your feed.')).toBeInTheDocument();
    });

    it('renders completed status summary', () => {
        render(<StatusBanner status="completed" matches_count={10} saved_count={8} execution_time={45.5} />);
        expect(screen.getAllByTestId('check-circle2-icon').length).toBeGreaterThanOrEqual(1);
        expect(screen.getByText('Complete')).toBeInTheDocument();
        expect(screen.getByText((_, node) => node?.textContent === 'Found 10')).toBeInTheDocument();
        expect(screen.getByText((_, node) => node?.textContent === 'Saved 8')).toBeInTheDocument();
        expect(screen.getByText('45.5s')).toBeInTheDocument();
    });

    it('renders stale-result warning when a newer upload exists', () => {
        render(
            <StatusBanner
                status="completed"
                matches_count={10}
                saved_count={8}
                stale_due_to_newer_upload
                stale_message="These results were generated from an older resume upload."
            />
        );

        expect(
            screen.getByText('These results were generated from an older resume upload.')
        ).toBeInTheDocument();
    });

    it('renders failed and cancelled states', () => {
        const { rerender } = render(<StatusBanner status="failed" error="Database connection failed" />);
        expect(screen.getByTestId('triangle-alert-icon')).toBeInTheDocument();
        expect(screen.getByText('Failed')).toBeInTheDocument();
        expect(screen.getByText("This run didn't finish.")).toBeInTheDocument();
        expect(screen.getByText('Database connection failed')).toBeInTheDocument();

        rerender(<StatusBanner status="cancelled" step="scoring" />);
        expect(screen.getByTestId('circle-slash-icon')).toBeInTheDocument();
        expect(screen.getByText('Stopped')).toBeInTheDocument();
    });

    it('renders cancellation requested and persisting warnings', () => {
        const { rerender, container } = render(
            <StatusBanner status="cancellation_requested" step="scoring" />
        );
        expect(screen.getByText('Stopping')).toBeInTheDocument();
        expect(screen.getByText('Stopping as soon as it is safe to.')).toBeInTheDocument();
        expect(container.querySelector('.ember')).toBeInTheDocument();

        rerender(<StatusBanner status="persisting" step="saving_results" />);
        expect(screen.getByText('Finishing')).toBeInTheDocument();
        expect(screen.getByText('Past the save boundary — finishing safely.')).toBeInTheDocument();
    });

    it('shows the current step for running states', () => {
        const { rerender } = render(<StatusBanner status="running" step="loading_resume" />);
        expect(screen.getByText('Loading resume')).toBeInTheDocument();

        rerender(<StatusBanner status="running" step="vector_matching" />);
        expect(screen.getByText('Finding candidates')).toBeInTheDocument();

        rerender(<StatusBanner status="running" step="scoring" />);
        expect(screen.getByText('Scoring')).toBeInTheDocument();

        rerender(<StatusBanner status="running" step="saving_results" />);
        expect(screen.getByText('Saving')).toBeInTheDocument();

        rerender(<StatusBanner status="running" step="notifying" />);
        expect(screen.getByText('Notifying')).toBeInTheDocument();
    });

    it('handles missing optional props gracefully', () => {
        render(<StatusBanner status="running" />);
        expect(screen.getByText('Working through your feed.')).toBeInTheDocument();
    });

    it('formats execution time with one decimal place', () => {
        render(<StatusBanner status="completed" execution_time={12.345} />);
        expect(screen.getByText('12.3s')).toBeInTheDocument();
    });
});

describe('SegmentedCircle', () => {
    const defaultProps: CircleChartProps = {
        activeMatches: 5,
        activeArc: 100,
        cappedArc: 20,
        hiddenArc: 50,
        belowArc: 30,
        circumference: 226.19,
        radius: 36,
    };

    it('renders SVG circle chart with five circles', () => {
        const { container } = render(<SegmentedCircle {...defaultProps} />);
        expect(container.querySelector('svg')).toBeInTheDocument();
        expect(container.querySelectorAll('circle')).toHaveLength(5);
    });

    it('displays active matches count in the center', () => {
        render(<SegmentedCircle {...defaultProps} />);
        expect(screen.getByText('5')).toBeInTheDocument();
        expect(screen.getByText('fit')).toBeInTheDocument();
    });

    it('uses the accent stroke for the active segment', () => {
        const { container } = render(<SegmentedCircle {...defaultProps} />);
        const segments = container.querySelectorAll('circle');
        expect(segments[1]).toHaveAttribute('stroke', 'var(--accent)');
    });

    it('stays responsive across breakpoints', () => {
        const { container } = render(<SegmentedCircle {...defaultProps} />);
        const svgContainer = container.firstChild as HTMLElement;
        expect(svgContainer).toHaveClass('h-24', 'w-24', 'sm:h-28', 'sm:w-28');
    });
});

describe('StatsPanel', () => {
    const defaultStats = {
        total_matches: 100,
        active_matches: 45,
        hidden_count: 30,
        below_threshold_count: 25,
        beyond_top_k_count: 0,
        job_post_total: 150,
        active_job_posts: 90,
        inactive_job_posts: 50,
        expired_job_posts: 10,
        ready_to_score_job_posts: 80,
        active_ready_to_score_job_posts: 70,
        pending_extraction_job_posts: 20,
        retryable_extraction_job_posts: 2,
        active_pending_extraction_job_posts: 3,
        active_retryable_extraction_job_posts: 1,
        inactive_pending_extraction_job_posts: 18,
        pending_embedding_job_posts: 5,
        retryable_embedding_job_posts: 1,
        active_pending_embedding_job_posts: 2,
        active_retryable_embedding_job_posts: 0,
        inactive_pending_embedding_job_posts: 4,
        missing_description_job_posts: 12,
        active_missing_description_job_posts: 2,
        inactive_missing_description_job_posts: 10,
        score_distribution: {
            excellent: 20,
            good: 25,
            average: 30,
            poor: 25,
        },
    };

    const defaultProps: StatsPanelProps = {
        stats: defaultStats,
    };

    it('renders the total card, match breakdown, and score distribution', () => {
        render(<StatsPanel {...defaultProps} />);
        expect(screen.getByText('Total')).toBeInTheDocument();
        expect(screen.getAllByText('100').length).toBeGreaterThan(0);
        expect(screen.getByText('matched candidates')).toBeInTheDocument();
        expect(screen.getByText('Imported')).toBeInTheDocument();
        expect(screen.getByText('Ready active')).toBeInTheDocument();
        expect(screen.getByText('Not active')).toBeInTheDocument();
        expect(screen.getByText('Ready extract')).toBeInTheDocument();
        expect(screen.getByText('Missing desc')).toBeInTheDocument();
        expect(screen.getByText('Active backlog')).toBeInTheDocument();
        expect(screen.getByText('Inactive backlog')).toBeInTheDocument();
        expect(screen.getByText('Fit')).toBeInTheDocument();
        expect(screen.getByText('Below threshold')).toBeInTheDocument();
        expect(screen.getByText('Hidden')).toBeInTheDocument();
        expect(screen.getByText('Score distribution')).toBeInTheDocument();
        expect(screen.getByText('Strong')).toBeInTheDocument();
        expect(screen.getByText('Good')).toBeInTheDocument();
        expect(screen.getByText('Fair')).toBeInTheDocument();
        expect(screen.getByText('Low')).toBeInTheDocument();
    });

    it('handles null and undefined stats gracefully', () => {
        const { rerender } = render(<StatsPanel {...defaultProps} stats={null} />);
        expect(screen.getAllByText('0').length).toBeGreaterThan(0);

        rerender(<StatsPanel {...defaultProps} stats={undefined} />);
        expect(screen.getAllByText('0').length).toBeGreaterThan(0);
    });

    it('renders partial distributions and expected widths', () => {
        const { container } = render(
            <StatsPanel
                {...defaultProps}
                stats={{
                    ...defaultStats,
                    score_distribution: { excellent: 10, good: 0, average: 0, poor: 0 },
                }}
            />
        );

        expect(screen.getByText('Strong')).toBeInTheDocument();
        expect(container.querySelector('div[style*="width: 10%"]')).toBeInTheDocument();
    });

    it('shows jobs above the current max result cap as a separate segment', () => {
        render(
            <StatsPanel
                stats={{
                    ...defaultStats,
                    active_matches: 50,
                    beyond_top_k_count: 12,
                }}
            />
        );

        expect(screen.getByText('Above max')).toBeInTheDocument();
        expect(screen.getAllByText('12').length).toBeGreaterThan(0);
    });
});

describe('CompactScoreBar', () => {
    const defaultProps: ScoreBarProps = {
        label: 'Test Label',
        range: '0-100',
        value: 75,
        total: 100,
        tone: 'accent',
    };

    it('renders label, range, and value', () => {
        render(<CompactScoreBar {...defaultProps} />);
        expect(screen.getByText('Test Label')).toBeInTheDocument();
        expect(screen.getByText('0-100')).toBeInTheDocument();
        expect(screen.getByText('75')).toBeInTheDocument();
    });

    it('calculates widths for normal, overflow, and zero-total cases', () => {
        const { container, rerender } = render(<CompactScoreBar {...defaultProps} />);
        expect(container.querySelector('div[style*="width: 75%"]')).toBeInTheDocument();

        rerender(<CompactScoreBar {...defaultProps} value={150} total={100} />);
        expect(container.querySelector('div[style*="width: 150%"]')).toBeInTheDocument();

        rerender(<CompactScoreBar {...defaultProps} value={50} total={0} />);
        expect(container.querySelector('div[style*="width: 0%"]')).toBeInTheDocument();
    });

    it('applies the selected tone class', () => {
        const { container } = render(<CompactScoreBar {...defaultProps} />);
        expect(container.querySelector('.bg-accent')).toBeInTheDocument();
    });
});

describe('ActionButton', () => {
    const defaultProps: ActionButtonProps = {
        canStop: false,
        isCancellationRequested: false,
        isPersistingStatus: false,
        isRunning: false,
        isStopping: false,
        onRun: vi.fn(),
        onStop: vi.fn(),
    };

    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('renders run and stop variants', () => {
        const { rerender, container } = render(<ActionButton {...defaultProps} />);
        expect(screen.getByText('Run matching')).toBeInTheDocument();
        expect(screen.getByTestId('play-icon')).toBeInTheDocument();
        expect(container.querySelector('button')).toHaveClass('bg-accent', 'border-accent');

        rerender(<ActionButton {...defaultProps} canStop />);
        expect(screen.getByText('Stop')).toBeInTheDocument();
        expect(screen.getByTestId('square-icon')).toBeInTheDocument();
        expect(container.querySelector('button')).toHaveClass('bg-surface', 'border-rule-strong');
    });

    it('calls onRun or onStop on click', () => {
        const onRun = vi.fn();
        const onStop = vi.fn();
        const { rerender } = render(<ActionButton {...defaultProps} onRun={onRun} onStop={onStop} />);

        screen.getByRole('button').click();
        expect(onRun).toHaveBeenCalledTimes(1);

        rerender(<ActionButton {...defaultProps} canStop onRun={onRun} onStop={onStop} />);
        screen.getByRole('button').click();
        expect(onStop).toHaveBeenCalledTimes(1);
    });

    it('disables processing states', () => {
        const { rerender } = render(<ActionButton {...defaultProps} isRunning />);
        expect(screen.getByRole('button')).toBeDisabled();

        rerender(<ActionButton {...defaultProps} canStop isStopping />);
        expect(screen.getByRole('button')).toBeDisabled();
    });

    it('renders persisting and cancellation labels', () => {
        const { rerender, container } = render(<ActionButton {...defaultProps} isPersistingStatus />);
        expect(screen.getByText('Finishing')).toBeInTheDocument();
        expect(container.querySelector('button')).toHaveClass('bg-warn-soft', 'border-warn');

        rerender(<ActionButton {...defaultProps} isCancellationRequested />);
        expect(screen.getByText('Stopping')).toBeInTheDocument();
    });

    it('renders resume processing labels', () => {
        const { rerender } = render(
            <ActionButton {...defaultProps} isProcessingResume processingStep="extracting" />
        );
        expect(screen.getByText('Parsing resume')).toBeInTheDocument();
        expect(screen.getByTestId('loader2-icon')).toBeInTheDocument();

        rerender(<ActionButton {...defaultProps} isProcessingResume processingStep="unknown-step" />);
        expect(screen.getByText('Preparing')).toBeInTheDocument();
    });
});
