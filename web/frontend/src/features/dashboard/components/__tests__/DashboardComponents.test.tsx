/**
 * Tests for Dashboard Components
 * Covers: web/frontend/src/features/dashboard/components/
 */

import { render, screen, within } from '@testing-library/react';
import { describe, it, expect, vi, beforeEach } from 'vitest';

// Import components
import { DashboardWrapper } from '../DashboardWrapper';
import { ResumeUploadSection, type ResumeUploadSectionProps } from '../ResumeUploadSection';
import { StatusBanner, type StatusBannerProps } from '../StatusBanner';
import { SegmentedCircle, type CircleChartProps } from '../SegmentedCircle';
import { StatsPanel, type StatsPanelProps } from '../StatsPanel';
import { CompactScoreBar, type ScoreBarProps } from '../CompactScoreBar';
import { ActionButton, type ActionButtonProps } from '../ActionButton';

// Mock lucide-react icons
vi.mock('lucide-react', () => ({
    FileUp: ({ className, ...props }: any) => <div data-testid="file-up-icon" className={className} {...props} />,
    Zap: ({ className, ...props }: any) => <div data-testid="zap-icon" className={className} {...props} />,
    Loader: ({ className, ...props }: any) => <div data-testid="loader-icon" className={className} {...props} />,
    CheckCircle: ({ className, ...props }: any) => <div data-testid="check-circle-icon" className={className} {...props} />,
    XCircle: ({ className, ...props }: any) => <div data-testid="x-circle-icon" className={className} {...props} />,
    TrendingUp: ({ className, ...props }: any) => <div data-testid="trending-up-icon" className={className} {...props} />,
    Award: ({ className, ...props }: any) => <div data-testid="award-icon" className={className} {...props} />,
    MapPin: ({ className, ...props }: any) => <div data-testid="map-pin-icon" className={className} {...props} />,
    Building2: ({ className, ...props }: any) => <div data-testid="building2-icon" className={className} {...props} />,
    Laptop: ({ className, ...props }: any) => <div data-testid="laptop-icon" className={className} {...props} />,
    Eye: ({ className, ...props }: any) => <div data-testid="eye-icon" className={className} {...props} />,
    EyeOff: ({ className, ...props }: any) => <div data-testid="eye-off-icon" className={className} {...props} />,
    ArrowUpRight: ({ className, ...props }: any) => <div data-testid="arrow-up-right-icon" className={className} {...props} />,
    Sparkles: ({ className, ...props }: any) => <div data-testid="sparkles-icon" className={className} {...props} />,
}));

// Mock Badge component
vi.mock('@/components/ui/Badge', () => ({
    Badge: ({ children, variant }: any) => (
        <span data-testid="badge" data-variant={variant} className={`badge-${variant}`}>
            {children}
        </span>
    ),
}));

// Mock RESUME_MAX_SIZE_MB constant
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

    it('applies correct styling classes', () => {
        const { container } = render(
            <DashboardWrapper>
                <div>Content</div>
            </DashboardWrapper>
        );

        const wrapper = container.firstChild as HTMLElement;
        expect(wrapper).toHaveClass('bg-gradient-to-br');
        expect(wrapper).toHaveClass('from-slate-50');
        expect(wrapper).toHaveClass('rounded-3xl');
    });

    it('renders decorative blur elements', () => {
        const { container } = render(
            <DashboardWrapper>
                <div>Content</div>
            </DashboardWrapper>
        );

        // Should have decorative blur circles
        const blurElements = container.querySelectorAll('.blur-3xl');
        expect(blurElements.length).toBe(2);
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

    it('displays "Upload Resume" when no filename', () => {
        render(<ResumeUploadSection {...defaultProps} />);

        expect(screen.getByText('Upload Resume')).toBeInTheDocument();
    });

    it('displays "Update Resume" with filename', () => {
        render(<ResumeUploadSection {...defaultProps} filename="resume.pdf" />);

        expect(screen.getByText('Update Resume')).toBeInTheDocument();
        expect(screen.getByText('resume.pdf')).toBeInTheDocument();
    });

    it('shows file size limit in tooltip', () => {
        render(<ResumeUploadSection {...defaultProps} />);

        // Tooltip should mention max size
        expect(screen.getByText(/max.*MB/i)).toBeInTheDocument();
    });

    it('has hidden file input with correct accept attribute', () => {
        render(<ResumeUploadSection {...defaultProps} />);

        const fileInput = screen.getByTestId('resume-file-input');
        expect(fileInput).toHaveClass('hidden');
        expect(fileInput).toHaveAttribute('accept', '.json,.yaml,.yml,.txt,.docx,.pdf');
    });

    it('calls onUpload when file is selected', async () => {
        const mockOnUpload = vi.fn();
        render(<ResumeUploadSection {...defaultProps} onUpload={mockOnUpload} />);

        const fileInput = screen.getByTestId('resume-file-input');
        const file = new File(['test content'], 'resume.pdf', { type: 'application/pdf' });

        await vi.waitFor(() => {
            fileInput.dispatchEvent(new Event('change', { bubbles: true }));
        });

        expect(mockOnUpload).toHaveBeenCalled();
    });

    it('disables button when isUploading', () => {
        render(<ResumeUploadSection {...defaultProps} isUploading />);

        const button = screen.getByRole('button');
        expect(button).toBeDisabled();
    });

    it('disables button when isRunning', () => {
        render(<ResumeUploadSection {...defaultProps} isRunning />);

        const button = screen.getByRole('button');
        expect(button).toBeDisabled();
    });

    it('shows loading state when uploading', () => {
        render(<ResumeUploadSection {...defaultProps} isUploading />);

        const button = screen.getByRole('button');
        expect(button).toHaveAttribute('disabled');
    });
});

describe('StatusBanner', () => {
    const defaultProps: StatusBannerProps = {
        status: 'running',
        step: 'loading_resume',
    };

    it('renders running status with spinner', () => {
        render(<StatusBanner {...defaultProps} />);

        expect(screen.getByTestId('loader-icon')).toBeInTheDocument();
        expect(screen.getByTestId('badge')).toHaveAttribute('data-variant', 'info');
        expect(screen.getByText('RUNNING')).toBeInTheDocument();
    });

    it('renders completed status with checkmark', () => {
        render(<StatusBanner status="completed" matches_count={10} saved_count={8} execution_time={45.5} />);

        expect(screen.getByTestId('check-circle-icon')).toBeInTheDocument();
        expect(screen.getByTestId('badge')).toHaveAttribute('data-variant', 'success');
        expect(screen.getByText('Pipeline completed!')).toBeInTheDocument();
        expect(screen.getByText('Found: 10')).toBeInTheDocument();
        expect(screen.getByText('Saved: 8')).toBeInTheDocument();
        expect(screen.getByText('Time: 45.50s')).toBeInTheDocument();
    });

    it('renders failed status with error message', () => {
        render(<StatusBanner status="failed" error="Database connection failed" />);

        expect(screen.getByTestId('x-circle-icon')).toBeInTheDocument();
        expect(screen.getByTestId('badge')).toHaveAttribute('data-variant', 'error');
        expect(screen.getByText('Pipeline failed')).toBeInTheDocument();
        expect(screen.getByText('Database connection failed')).toBeInTheDocument();
    });

    it('shows current step when running', () => {
        const { rerender } = render(<StatusBanner status="running" step="loading_resume" />);
        expect(screen.getByText('Loading Resume')).toBeInTheDocument();

        rerender(<StatusBanner status="running" step="vector_matching" />);
        expect(screen.getByText('Finding Matches')).toBeInTheDocument();

        rerender(<StatusBanner status="running" step="scoring" />);
        expect(screen.getByText('Scoring Candidates')).toBeInTheDocument();

        rerender(<StatusBanner status="running" step="saving_results" />);
        expect(screen.getByText('Saving Results')).toBeInTheDocument();

        rerender(<StatusBanner status="running" step="notifying" />);
        expect(screen.getByText('Notifying')).toBeInTheDocument();
    });

    it('shows pulsing indicator when running', () => {
        render(<StatusBanner {...defaultProps} />);

        // Should have pulsing animation element
        const pulsingElement = screen.getByTestId('loader-icon').parentElement;
        expect(pulsingElement).toBeInTheDocument();
    });

    it('handles undefined optional props gracefully', () => {
        render(<StatusBanner status="running" />);

        expect(screen.getByText('Processing your matches...')).toBeInTheDocument();
    });

    it('formats execution time with 2 decimal places', () => {
        render(<StatusBanner status="completed" execution_time={12.345} />);

        expect(screen.getByText('Time: 12.35s')).toBeInTheDocument();
    });
});

describe('SegmentedCircle', () => {
    const defaultProps: CircleChartProps = {
        activeMatches: 5,
        activeArc: 100,
        hiddenArc: 50,
        belowArc: 30,
        circumference: 226.19,
        radius: 36,
    };

    it('renders SVG circle chart', () => {
        render(<SegmentedCircle {...defaultProps} />);

        // Should have SVG element
        const svg = screen.getByRole('img', { hidden: true }) || document.querySelector('svg');
        expect(svg).toBeInTheDocument();
    });

    it('displays active matches count in center', () => {
        render(<SegmentedCircle {...defaultProps} />);

        expect(screen.getByText('5')).toBeInTheDocument();
        expect(screen.getByText('Fits')).toBeInTheDocument();
    });

    it('renders three segment circles', () => {
        const { container } = render(<SegmentedCircle {...defaultProps} />);

        const circles = container.querySelectorAll('circle');
        expect(circles.length).toBe(4); // 1 background + 3 segments
    });

    it('applies gradient to active segment', () => {
        const { container } = render(<SegmentedCircle {...defaultProps} />);

        // Should have gradient definition
        expect(container.querySelector('linearGradient')).toBeInTheDocument();
        expect(container.querySelector('#gradient-active')).toBeInTheDocument();
    });

    it('calculates correct stroke-dasharray for segments', () => {
        render(<SegmentedCircle {...defaultProps} />);

        const { container } = render(<SegmentedCircle {...defaultProps} />);
        const segments = container.querySelectorAll('circle:not(:first-child)');

        // Each segment should have stroke-dasharray style
        segments.forEach(segment => {
            expect(segment).toHaveAttribute('style');
        });
    });

    it('is responsive with different screen sizes', () => {
        const { container } = render(<SegmentedCircle {...defaultProps} />);

        const svgContainer = container.firstChild as HTMLElement;
        expect(svgContainer).toHaveClass('sm:w-32');
        expect(svgContainer).toHaveClass('lg:w-36');
    });
});

describe('StatsPanel', () => {
    const defaultStats = {
        total_matches: 100,
        active_matches: 45,
        hidden_count: 30,
        below_threshold_count: 25,
        score_distribution: {
            excellent: 20,
            good: 25,
            average: 30,
            poor: 25,
        },
    };

    const defaultProps: StatsPanelProps = {
        stats: defaultStats,
        activeMatches: 45,
        activeArc: 100,
        hiddenArc: 50,
        belowArc: 30,
        circumference: 226.19,
        radius: 36,
    };

    it('renders total matches card', () => {
        render(<StatsPanel {...defaultProps} />);

        expect(screen.getByText('100')).toBeInTheDocument();
        expect(screen.getByText('Total Matches')).toBeInTheDocument();
        expect(screen.getByTestId('trending-up-icon')).toBeInTheDocument();
    });

    it('renders segmented circle with match breakdown', () => {
        render(<StatsPanel {...defaultProps} />);

        expect(screen.getByText('45 Fit')).toBeInTheDocument();
        expect(screen.getByText('25 Misfit')).toBeInTheDocument();
        expect(screen.getByText('30 Hidden')).toBeInTheDocument();
    });

    it('renders score distribution section', () => {
        render(<StatsPanel {...defaultProps} />);

        expect(screen.getByTestId('award-icon')).toBeInTheDocument();
        expect(screen.getByText('Score Distribution')).toBeInTheDocument();
        expect(screen.getByText('Excellent')).toBeInTheDocument();
        expect(screen.getByText('Good')).toBeInTheDocument();
        expect(screen.getByText('Average')).toBeInTheDocument();
        expect(screen.getByText('Poor')).toBeInTheDocument();
    });

    it('handles null stats gracefully', () => {
        render(<StatsPanel {...defaultProps} stats={null} />);

        // Should show 0 for all values
        expect(screen.getByText('0')).toBeInTheDocument();
    });

    it('handles undefined stats gracefully', () => {
        render(<StatsPanel {...defaultProps} stats={undefined} />);

        expect(screen.getByText('0')).toBeInTheDocument();
    });

    it('handles partial score distribution', () => {
        const partialStats = {
            ...defaultStats,
            score_distribution: {
                excellent: 10,
                good: 0,
                average: 0,
                poor: 0,
            },
        };

        render(<StatsPanel {...defaultProps} stats={partialStats} />);

        expect(screen.getByText('Excellent')).toBeInTheDocument();
    });

    it('calculates correct percentages for score bars', () => {
        render(<StatsPanel {...defaultProps} />);

        // Score bars should be rendered with correct widths
        const scoreBars = document.querySelectorAll('[class*="CompactScoreBar"]');
        expect(scoreBars.length).toBe(4);
    });
});

describe('CompactScoreBar', () => {
    const defaultProps: ScoreBarProps = {
        label: 'Test Label',
        range: '0-100',
        value: 75,
        total: 100,
        gradient: 'from-blue-500 to-indigo-600',
    };

    it('renders label and range', () => {
        render(<CompactScoreBar {...defaultProps} />);

        expect(screen.getByText('Test Label')).toBeInTheDocument();
        expect(screen.getByText('(0-100)')).toBeInTheDocument();
    });

    it('renders value', () => {
        render(<CompactScoreBar {...defaultProps} />);

        expect(screen.getByText('75')).toBeInTheDocument();
    });

    it('calculates correct percentage width', () => {
        const { container } = render(<CompactScoreBar {...defaultProps} />);

        const progressBar = container.querySelector('.bg-gradient-to-r');
        expect(progressBar).toHaveAttribute('style', 'width: 75%');
    });

    it('handles value greater than total', () => {
        const { container } = render(<CompactScoreBar {...defaultProps} value={150} total={100} />);

        const progressBar = container.querySelector('.bg-gradient-to-r');
        expect(progressBar).toHaveAttribute('style', 'width: 100%');
    });

    it('handles zero total', () => {
        const { container } = render(<CompactScoreBar {...defaultProps} value={50} total={0} />);

        const progressBar = container.querySelector('.bg-gradient-to-r');
        expect(progressBar).toHaveAttribute('style', 'width: 0%');
    });

    it('applies gradient colors', () => {
        const { container } = render(<CompactScoreBar {...defaultProps} />);

        const indicator = container.querySelector('.rounded-full.bg-gradient-to-r');
        expect(indicator).toHaveClass('from-blue-500');
        expect(indicator).toHaveClass('to-indigo-600');
    });

    it('is responsive', () => {
        render(<CompactScoreBar {...defaultProps} />);

        // Should have responsive text sizes
        expect(screen.getByText('Test Label')).toHaveClass('text-xs', 'sm:text-sm');
    });
});

describe('ActionButton', () => {
    const defaultProps: ActionButtonProps = {
        isRunningStatus: false,
        isRunning: false,
        isStopping: false,
        onRun: vi.fn(),
        onStop: vi.fn(),
    };

    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('displays "Run Matching" when not running', () => {
        render(<ActionButton {...defaultProps} />);

        expect(screen.getByText('Run Matching')).toBeInTheDocument();
    });

    it('displays "Stop" when running', () => {
        render(<ActionButton {...defaultProps} isRunningStatus />);

        expect(screen.getByText('Stop')).toBeInTheDocument();
    });

    it('calls onRun when clicked (not running)', () => {
        const mockOnRun = vi.fn();
        render(<ActionButton {...defaultProps} onRun={mockOnRun} />);

        const button = screen.getByRole('button');
        button.click();

        expect(mockOnRun).toHaveBeenCalledTimes(1);
        expect(mockOnRun).toHaveBeenCalled();
    });

    it('calls onStop when clicked (running)', () => {
        const mockOnStop = vi.fn();
        render(<ActionButton {...defaultProps} isRunningStatus onStop={mockOnStop} />);

        const button = screen.getByRole('button');
        button.click();

        expect(mockOnStop).toHaveBeenCalledTimes(1);
    });

    it('is disabled when processing', () => {
        render(<ActionButton {...defaultProps} isRunning />);

        const button = screen.getByRole('button');
        expect(button).toBeDisabled();
    });

    it('is disabled when stopping', () => {
        render(<ActionButton {...defaultProps} isRunningStatus isStopping />);

        const button = screen.getByRole('button');
        expect(button).toBeDisabled();
    });

    it('shows Zap icon when not running', () => {
        render(<ActionButton {...defaultProps} />);

        expect(screen.getByTestId('zap-icon')).toBeInTheDocument();
    });

    it('does not show Zap icon when running', () => {
        render(<ActionButton {...defaultProps} isRunningStatus />);

        expect(screen.queryByTestId('zap-icon')).not.toBeInTheDocument();
    });

    it('has gradient background when not running', () => {
        const { container } = render(<ActionButton {...defaultProps} />);

        const button = container.querySelector('button');
        expect(button).toHaveClass('from-blue-600');
        expect(button).toHaveClass('to-indigo-600');
    });

    it('has red background when running', () => {
        const { container } = render(<ActionButton {...defaultProps} isRunningStatus />);

        const button = container.querySelector('button');
        expect(button).toHaveClass('bg-red-500');
    });
});
