import { fireEvent, render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { JobInventoryPanel } from '../JobInventoryPanel';

const mockUseJobs = vi.hoisted(() => vi.fn());

vi.mock('@/hooks/useJobs', () => ({
    useJobs: (params: any, enabled: boolean) => mockUseJobs(params, enabled),
}));

vi.mock('lucide-react', () => ({
    AlertTriangle: ({ className, ...props }: any) => <div data-testid="alert-triangle-icon" className={className} {...props} />,
    ChevronLeft: ({ className, ...props }: any) => <div data-testid="chevron-left-icon" className={className} {...props} />,
    ChevronRight: ({ className, ...props }: any) => <div data-testid="chevron-right-icon" className={className} {...props} />,
    ExternalLink: ({ className, ...props }: any) => <div data-testid="external-link-icon" className={className} {...props} />,
    ListChecks: ({ className, ...props }: any) => <div data-testid="list-checks-icon" className={className} {...props} />,
    RefreshCw: ({ className, ...props }: any) => <div data-testid="refresh-cw-icon" className={className} {...props} />,
    Search: ({ className, ...props }: any) => <div data-testid="search-icon" className={className} {...props} />,
}));

const readyJob = {
    job_id: 'job-1',
    title: 'Software Engineer',
    company: 'Ramp',
    location: 'New York, NY',
    is_remote: true,
    status: 'active',
    is_extracted: true,
    is_embedded: true,
    extraction_status: 'succeeded',
    embedding_status: 'succeeded',
    description_completeness: 'full',
    description_source: 'ats.greenhouse',
    source_site: 'greenhouse',
    source_url: 'https://boards.greenhouse.io/ramp/jobs/1',
    first_seen_at: '2026-06-01T00:00:00Z',
    last_seen_at: '2026-06-18T00:00:00Z',
    extraction_attempts: 1,
    extraction_last_error: null,
    extraction_next_retry_at: null,
    embedding_attempts: 1,
    embedding_last_error: null,
    embedding_next_retry_at: null,
};

const stats = {
    job_post_total: 1460,
    ready_to_score_job_posts: 353,
    pending_extraction_job_posts: 1103,
    retryable_extraction_job_posts: 4,
    pending_embedding_job_posts: 330,
};

describe('JobInventoryPanel', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        mockUseJobs.mockReturnValue({
            data: { success: true, count: 1, total: 1, limit: 50, offset: 0, jobs: [readyJob] },
            isLoading: false,
            error: null,
            refetch: vi.fn(),
        });
    });

    it('stays collapsed by default while showing inventory totals', () => {
        render(<JobInventoryPanel stats={stats} />);

        expect(screen.getByText('Imported jobs')).toBeInTheDocument();
        expect(screen.getByText('1460')).toBeInTheDocument();
        expect(screen.getByText('1107')).toBeInTheDocument();
        expect(screen.getByRole('button', { name: /browse jobs/i })).toHaveAttribute('aria-expanded', 'false');
        expect(screen.queryByText('Software Engineer')).not.toBeInTheDocument();
        expect(mockUseJobs).toHaveBeenCalledWith(
            expect.objectContaining({ processing_status: 'all', limit: 50, offset: 0 }),
            false,
        );
    });

    it('renders imported jobs when opened', () => {
        render(<JobInventoryPanel stats={stats} />);

        fireEvent.click(screen.getByRole('button', { name: /browse jobs/i }));

        expect(screen.getByRole('button', { name: /hide jobs/i })).toHaveAttribute('aria-expanded', 'true');
        expect(screen.getByText('Software Engineer')).toBeInTheDocument();
        expect(screen.getByText(/Ramp/)).toBeInTheDocument();
        expect(screen.getAllByText('Ready').length).toBeGreaterThan(0);
        expect(mockUseJobs).toHaveBeenLastCalledWith(
            expect.objectContaining({ processing_status: 'all', job_status: 'all' }),
            true,
        );
    });

    it('updates processing filters and resets to the first page', () => {
        render(<JobInventoryPanel stats={stats} />);
        fireEvent.click(screen.getByRole('button', { name: /browse jobs/i }));

        fireEvent.click(screen.getByRole('button', { name: /^Pending extract$/i }));

        expect(mockUseJobs).toHaveBeenLastCalledWith(
            expect.objectContaining({ processing_status: 'pending_extraction', offset: 0 }),
            true,
        );
    });
});
