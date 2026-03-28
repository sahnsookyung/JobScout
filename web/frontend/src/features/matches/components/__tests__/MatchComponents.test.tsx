/**
 * Tests for Match components
 * Covers: MatchCard, MatchFilters, MatchList, MatchDetailsModal
 */

import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MatchFilters } from '../MatchFilters';
import { MatchCard } from '../MatchCard';
import { MatchList } from '../MatchList';
import { MatchDetailsModal } from '../MatchDetailsModal';

// ── Mocks ──────────────────────────────────────────────────────────────────

vi.mock('lucide-react', () => ({
    MapPin: ({ className }: any) => <svg data-testid="map-pin" className={className} />,
    Building2: ({ className }: any) => <svg data-testid="building2" className={className} />,
    Laptop: ({ className }: any) => <svg data-testid="laptop" className={className} />,
    Eye: ({ className }: any) => <svg data-testid="eye" className={className} />,
    EyeOff: ({ className }: any) => <svg data-testid="eye-off" className={className} />,
    ArrowUpRight: ({ className }: any) => <svg data-testid="arrow-up-right" className={className} />,
    Award: ({ className }: any) => <svg data-testid="award" className={className} />,
    Sparkles: ({ className }: any) => <svg data-testid="sparkles" className={className} />,
    Filter: ({ className }: any) => <svg data-testid="filter" className={className} />,
    SortDesc: ({ className }: any) => <svg data-testid="sort-desc" className={className} />,
    Star: ({ className }: any) => <svg data-testid="star" className={className} />,
    X: ({ className }: any) => <svg data-testid="x-icon" className={className} />,
    TrendingUp: ({ className }: any) => <svg data-testid="trending-up" className={className} />,
    CheckCircle: ({ className }: any) => <svg data-testid="check-circle" className={className} />,
    XCircle: ({ className }: any) => <svg data-testid="x-circle" className={className} />,
}));

vi.mock('@/utils/formatters', () => ({
    formatScore: (v: number) => `${Math.round(v)}%`,
    formatSalary: (v: any) => v || 'N/A',
}));

vi.mock('@/utils/constants', () => ({
    MATCH_STATUSES: [
        { value: 'active', label: 'Active' },
        { value: 'stale', label: 'Stale' },
        { value: 'all', label: 'All' },
    ],
    SORT_OPTIONS: [
        { value: 'overall', label: 'Overall Score' },
        { value: 'fit', label: 'Fit Score' },
        { value: 'want', label: 'Want Score' },
    ],
}));

vi.mock('@/components/ui/Toast', () => ({
    toast: { success: vi.fn(), error: vi.fn() },
}));

vi.mock('@/components/ui/Badge', () => ({
    Badge: ({ children, variant }: any) => (
        <span data-testid="badge" data-variant={variant}>{children}</span>
    ),
}));

vi.mock('@/services/matchesApi', () => ({
    matchesApi: { toggleHidden: vi.fn() },
}));

const mockUseMatchDetails = vi.fn();
vi.mock('@/hooks/useMatchDetails', () => ({
    useMatchDetails: (id: any) => mockUseMatchDetails(id),
}));

const mockUseMatches = vi.fn();
vi.mock('@/hooks/useMatches', () => ({
    useMatches: (params: any) => mockUseMatches(params),
}));

// ── Helpers ────────────────────────────────────────────────────────────────

function makeQueryWrapper() {
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    return ({ children }: any) => (
        <QueryClientProvider client={qc}>{children}</QueryClientProvider>
    );
}

function makeMatch(overrides: Record<string, any> = {}) {
    return {
        match_id: 'match-1',
        title: 'Senior Engineer',
        company: 'TechCorp',
        location: 'San Francisco, CA',
        is_remote: false,
        is_hidden: false,
        overall_score: 85,
        fit_score: 80,
        want_score: 75,
        required_coverage: 0.9,
        match_type: 'vector_match',
        ...overrides,
    };
}

// ── MatchFilters ───────────────────────────────────────────────────────────

describe('MatchFilters', () => {
    const defaultProps = {
        status: 'active' as const,
        onStatusChange: vi.fn(),
        remoteOnly: false,
        onRemoteOnlyChange: vi.fn(),
        showWantScore: false,
        onShowWantScoreChange: vi.fn(),
        sortBy: 'overall' as const,
        onSortByChange: vi.fn(),
        showHidden: false,
        onShowHiddenChange: vi.fn(),
    };

    beforeEach(() => vi.clearAllMocks());

    it('renders status select with options', () => {
        render(<MatchFilters {...defaultProps} />);
        expect(screen.getByText('Active')).toBeInTheDocument();
        expect(screen.getByText('Stale')).toBeInTheDocument();
        expect(screen.getByText('All')).toBeInTheDocument();
    });

    it('renders sort select with options', () => {
        render(<MatchFilters {...defaultProps} />);
        expect(screen.getByText('Overall Score')).toBeInTheDocument();
        expect(screen.getByText('Fit Score')).toBeInTheDocument();
        expect(screen.getByText('Want Score')).toBeInTheDocument();
    });

    it('calls onStatusChange when status select changes', () => {
        render(<MatchFilters {...defaultProps} />);
        const selects = screen.getAllByRole('combobox');
        fireEvent.change(selects[0], { target: { value: 'stale' } });
        expect(defaultProps.onStatusChange).toHaveBeenCalledWith('stale');
    });

    it('calls onSortByChange when sort select changes', () => {
        render(<MatchFilters {...defaultProps} />);
        const selects = screen.getAllByRole('combobox');
        fireEvent.change(selects[1], { target: { value: 'fit' } });
        expect(defaultProps.onSortByChange).toHaveBeenCalledWith('fit');
    });

    it('renders Remote Only toggle', () => {
        render(<MatchFilters {...defaultProps} />);
        expect(screen.getByText('Remote Only')).toBeInTheDocument();
    });

    it('renders Show Want Score toggle', () => {
        render(<MatchFilters {...defaultProps} />);
        expect(screen.getByText('Show Want Score')).toBeInTheDocument();
    });

    it('renders Show Hidden toggle', () => {
        render(<MatchFilters {...defaultProps} />);
        expect(screen.getByText('Show Hidden')).toBeInTheDocument();
    });

    it('calls onRemoteOnlyChange when Remote Only toggled', () => {
        const onRemoteOnlyChange = vi.fn();
        render(<MatchFilters {...defaultProps} onRemoteOnlyChange={onRemoteOnlyChange} />);
        const checkboxes = screen.getAllByRole('checkbox');
        fireEvent.click(checkboxes[0]);
        expect(onRemoteOnlyChange).toHaveBeenCalledWith(true);
    });

    it('calls onShowWantScoreChange when Show Want Score toggled', () => {
        const onShowWantScoreChange = vi.fn();
        render(<MatchFilters {...defaultProps} onShowWantScoreChange={onShowWantScoreChange} />);
        const checkboxes = screen.getAllByRole('checkbox');
        fireEvent.click(checkboxes[1]);
        expect(onShowWantScoreChange).toHaveBeenCalledWith(true);
    });

    it('calls onShowHiddenChange when Show Hidden toggled', () => {
        const onShowHiddenChange = vi.fn();
        render(<MatchFilters {...defaultProps} onShowHiddenChange={onShowHiddenChange} />);
        const checkboxes = screen.getAllByRole('checkbox');
        fireEvent.click(checkboxes[2]);
        expect(onShowHiddenChange).toHaveBeenCalledWith(true);
    });

    it('shows checked state for remoteOnly=true', () => {
        render(<MatchFilters {...defaultProps} remoteOnly={true} />);
        const checkboxes = screen.getAllByRole('checkbox');
        expect(checkboxes[0]).toBeChecked();
    });
});

// ── MatchCard ──────────────────────────────────────────────────────────────

describe('MatchCard', () => {
    beforeEach(() => vi.clearAllMocks());

    it('renders job title and company', () => {
        render(<MatchCard match={makeMatch()} onSelect={vi.fn()} />, { wrapper: makeQueryWrapper() });
        expect(screen.getByText('Senior Engineer')).toBeInTheDocument();
        expect(screen.getByText('TechCorp')).toBeInTheDocument();
    });

    it('renders location when provided', () => {
        render(<MatchCard match={makeMatch()} onSelect={vi.fn()} />, { wrapper: makeQueryWrapper() });
        expect(screen.getByText('San Francisco, CA')).toBeInTheDocument();
    });

    it('does not show map-pin icon when location is null', () => {
        render(<MatchCard match={makeMatch({ location: null })} onSelect={vi.fn()} />, { wrapper: makeQueryWrapper() });
        expect(screen.queryByTestId('map-pin')).not.toBeInTheDocument();
    });

    it('shows Remote badge when is_remote=true', () => {
        render(<MatchCard match={makeMatch({ is_remote: true })} onSelect={vi.fn()} />, { wrapper: makeQueryWrapper() });
        expect(screen.getByText('Remote')).toBeInTheDocument();
    });

    it('does not show Remote badge when is_remote=false', () => {
        render(<MatchCard match={makeMatch({ is_remote: false })} onSelect={vi.fn()} />, { wrapper: makeQueryWrapper() });
        expect(screen.queryByText('Remote')).not.toBeInTheDocument();
    });

    it('shows Top Match badge for high scores (>=80)', () => {
        render(<MatchCard match={makeMatch({ overall_score: 85 })} onSelect={vi.fn()} />, { wrapper: makeQueryWrapper() });
        expect(screen.getByText('Top Match')).toBeInTheDocument();
    });

    it('does not show Top Match badge for scores below 80', () => {
        render(<MatchCard match={makeMatch({ overall_score: 70 })} onSelect={vi.fn()} />, { wrapper: makeQueryWrapper() });
        expect(screen.queryByText('Top Match')).not.toBeInTheDocument();
    });

    it('shows Hidden badge when is_hidden=true', () => {
        render(<MatchCard match={makeMatch({ is_hidden: true })} onSelect={vi.fn()} />, { wrapper: makeQueryWrapper() });
        expect(screen.getByText('Hidden')).toBeInTheDocument();
    });

    it('calls onSelect with match_id when card is clicked', () => {
        const onSelect = vi.fn();
        render(<MatchCard match={makeMatch()} onSelect={onSelect} />, { wrapper: makeQueryWrapper() });
        fireEvent.click(screen.getByRole('button', { name: /view details for senior engineer at techcorp/i }));
        expect(onSelect).toHaveBeenCalledWith('match-1');
    });

    it('shows EyeOff icon when match is hidden', () => {
        render(<MatchCard match={makeMatch({ is_hidden: true })} onSelect={vi.fn()} />, { wrapper: makeQueryWrapper() });
        expect(screen.getByTestId('eye-off')).toBeInTheDocument();
    });

    it('shows Eye icon when match is visible', () => {
        render(<MatchCard match={makeMatch({ is_hidden: false })} onSelect={vi.fn()} />, { wrapper: makeQueryWrapper() });
        expect(screen.getByTestId('eye')).toBeInTheDocument();
    });

    it('shows want score bar when showWantScore=true and want_score present', () => {
        render(
            <MatchCard match={makeMatch({ want_score: 75 })} onSelect={vi.fn()} showWantScore />,
            { wrapper: makeQueryWrapper() }
        );
        expect(screen.getByText('Want Match')).toBeInTheDocument();
    });

    it('hides want score bar when showWantScore=false', () => {
        render(
            <MatchCard match={makeMatch({ want_score: 75 })} onSelect={vi.fn()} showWantScore={false} />,
            { wrapper: makeQueryWrapper() }
        );
        expect(screen.queryByText('Want Match')).not.toBeInTheDocument();
    });

    it('renders formatted score value', () => {
        render(<MatchCard match={makeMatch({ overall_score: 85 })} onSelect={vi.fn()} />, { wrapper: makeQueryWrapper() });
        expect(screen.getByText('85%')).toBeInTheDocument();
    });

    it('renders match_type with underscores replaced by spaces', () => {
        render(<MatchCard match={makeMatch({ match_type: 'vector_match' })} onSelect={vi.fn()} />, { wrapper: makeQueryWrapper() });
        expect(screen.getByText('vector match')).toBeInTheDocument();
    });

    it('hide button click does not trigger onSelect', () => {
        const onSelect = vi.fn();
        render(<MatchCard match={makeMatch()} onSelect={onSelect} />, { wrapper: makeQueryWrapper() });
        const hideBtn = screen.getByRole('button', { name: /hide this job/i });
        fireEvent.click(hideBtn);
        expect(onSelect).not.toHaveBeenCalled();
    });

    it('hide button is aria-pressed=false for visible jobs', () => {
        render(<MatchCard match={makeMatch({ is_hidden: false })} onSelect={vi.fn()} />, { wrapper: makeQueryWrapper() });
        const btn = screen.getByRole('button', { name: /hide this job/i });
        expect(btn).toHaveAttribute('aria-pressed', 'false');
    });

    it('hide button is aria-pressed=true for hidden jobs', () => {
        render(<MatchCard match={makeMatch({ is_hidden: true })} onSelect={vi.fn()} />, { wrapper: makeQueryWrapper() });
        const btn = screen.getByRole('button', { name: /unhide this job/i });
        expect(btn).toHaveAttribute('aria-pressed', 'true');
    });
});

// ── MatchList ──────────────────────────────────────────────────────────────

const storageMock = (() => {
    let store: Record<string, string> = {};
    return {
        getItem: (key: string) => store[key] ?? null,
        setItem: (key: string, value: string) => { store[key] = String(value); },
        removeItem: (key: string) => { delete store[key]; },
        clear: () => { store = {}; },
    };
})();

describe('MatchList', () => {
    beforeAll(() => {
        vi.stubGlobal('localStorage', storageMock);
    });
    afterAll(() => {
        vi.unstubAllGlobals();
    });
    beforeEach(() => {
        vi.clearAllMocks();
        storageMock.clear();
    });

    it('shows loading spinner while loading', () => {
        mockUseMatches.mockReturnValue({ data: null, isLoading: true, error: null, refetch: vi.fn() });
        const { container } = render(<MatchList onMatchSelect={vi.fn()} />, { wrapper: makeQueryWrapper() });
        expect(container.querySelector('.animate-spin')).toBeInTheDocument();
    });

    it('shows error message on error', () => {
        mockUseMatches.mockReturnValue({ data: null, isLoading: false, error: new Error('fail'), refetch: vi.fn() });
        render(<MatchList onMatchSelect={vi.fn()} />, { wrapper: makeQueryWrapper() });
        expect(screen.getByText(/Error loading matches/i)).toBeInTheDocument();
    });

    it('retry button calls refetch', () => {
        const refetch = vi.fn();
        mockUseMatches.mockReturnValue({ data: null, isLoading: false, error: new Error('fail'), refetch });
        render(<MatchList onMatchSelect={vi.fn()} />, { wrapper: makeQueryWrapper() });
        fireEvent.click(screen.getByText('Retry'));
        expect(refetch).toHaveBeenCalledTimes(1);
    });

    it('shows match count plural when multiple matches', () => {
        const matches = [makeMatch(), makeMatch({ match_id: 'match-2', title: 'Dev' })];
        mockUseMatches.mockReturnValue({ data: { matches }, isLoading: false, error: null, refetch: vi.fn() });
        render(<MatchList onMatchSelect={vi.fn()} />, { wrapper: makeQueryWrapper() });
        expect(screen.getByText('Showing 2 matches')).toBeInTheDocument();
    });

    it('shows singular "match" for single result', () => {
        mockUseMatches.mockReturnValue({ data: { matches: [makeMatch()] }, isLoading: false, error: null, refetch: vi.fn() });
        render(<MatchList onMatchSelect={vi.fn()} />, { wrapper: makeQueryWrapper() });
        expect(screen.getByText('Showing 1 match')).toBeInTheDocument();
    });

    it('shows empty state when no matches', () => {
        mockUseMatches.mockReturnValue({ data: { matches: [] }, isLoading: false, error: null, refetch: vi.fn() });
        render(<MatchList onMatchSelect={vi.fn()} />, { wrapper: makeQueryWrapper() });
        expect(screen.getByText(/No matches found/i)).toBeInTheDocument();
    });

    it('persists showHidden preference in localStorage', async () => {
        mockUseMatches.mockReturnValue({ data: { matches: [] }, isLoading: false, error: null, refetch: vi.fn() });
        render(<MatchList onMatchSelect={vi.fn()} />, { wrapper: makeQueryWrapper() });
        const checkboxes = screen.getAllByRole('checkbox');
        fireEvent.click(checkboxes[2]); // Show Hidden toggle
        await waitFor(() => {
            expect(localStorage.getItem('jobscout_show_hidden')).toBe('true');
        });
    });

    it('reads showHidden initial value from localStorage', () => {
        storageMock.setItem('jobscout_show_hidden', 'true');
        mockUseMatches.mockReturnValue({ data: { matches: [] }, isLoading: false, error: null, refetch: vi.fn() });
        render(<MatchList onMatchSelect={vi.fn()} />, { wrapper: makeQueryWrapper() });
        const checkboxes = screen.getAllByRole('checkbox');
        expect(checkboxes[2]).toBeChecked();
    });
});

// ── MatchDetailsModal ──────────────────────────────────────────────────────

function makeModalData(overrides: Record<string, any> = {}) {
    return {
        job: {
            title: 'Staff Engineer',
            company: 'Acme',
            location: 'NYC',
            is_remote: true,
            salary_min: null,
            salary_max: null,
            currency: null,
            min_years_experience: null,
            job_level: null,
            requires_degree: null,
            description: null,
            ...overrides.job,
        },
        match: {
            overall_score: 90,
            fit_score: 88,
            want_score: 82,
            required_coverage: 0.95,
            preferred_coverage: 0.85,
            matched_requirements_count: 9,
            total_requirements: 10,
            penalties: 0.5,
            ...overrides.match,
        },
        requirements: overrides.requirements ?? [],
    };
}

describe('MatchDetailsModal', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        // Provide a default so null-matchId case doesn't throw
        mockUseMatchDetails.mockReturnValue({ data: undefined, isLoading: false });
    });

    it('renders nothing when matchId is null', () => {
        const { container } = render(<MatchDetailsModal matchId={null} onClose={vi.fn()} />);
        expect(container.firstChild).toBeNull();
    });

    it('shows loading spinner when data is loading', () => {
        mockUseMatchDetails.mockReturnValue({ data: undefined, isLoading: true });
        const { container } = render(<MatchDetailsModal matchId="match-1" onClose={vi.fn()} />);
        expect(container.querySelector('.animate-spin')).toBeInTheDocument();
    });

    it('shows error state when data is undefined and not loading', () => {
        mockUseMatchDetails.mockReturnValue({ data: undefined, isLoading: false });
        render(<MatchDetailsModal matchId="match-1" onClose={vi.fn()} />);
        expect(screen.getByText(/Failed to load match details/i)).toBeInTheDocument();
    });

    it('calls onClose when close button is clicked', () => {
        mockUseMatchDetails.mockReturnValue({ data: undefined, isLoading: true });
        const onClose = vi.fn();
        render(<MatchDetailsModal matchId="match-1" onClose={onClose} />);
        fireEvent.click(screen.getByLabelText('Close'));
        expect(onClose).toHaveBeenCalledTimes(1);
    });

    it('calls onClose on Escape key', () => {
        mockUseMatchDetails.mockReturnValue({ data: undefined, isLoading: true });
        const onClose = vi.fn();
        render(<MatchDetailsModal matchId="match-1" onClose={onClose} />);
        fireEvent.keyDown(globalThis as Window, { key: 'Escape' });
        expect(onClose).toHaveBeenCalledTimes(1);
    });

    it('does not listen for Escape when matchId is null', () => {
        const onClose = vi.fn();
        render(<MatchDetailsModal matchId={null} onClose={onClose} />);
        fireEvent.keyDown(globalThis as Window, { key: 'Escape' });
        expect(onClose).not.toHaveBeenCalled();
    });

    it('renders job title and company when data is available', () => {
        mockUseMatchDetails.mockReturnValue({ data: makeModalData(), isLoading: false });
        render(<MatchDetailsModal matchId="match-1" onClose={vi.fn()} />);
        expect(screen.getByText('Staff Engineer')).toBeInTheDocument();
        expect(screen.getByText('Acme')).toBeInTheDocument();
    });

    it('shows Match Details heading', () => {
        mockUseMatchDetails.mockReturnValue({ data: makeModalData(), isLoading: false });
        render(<MatchDetailsModal matchId="match-1" onClose={vi.fn()} />);
        expect(screen.getByText('Match Details')).toBeInTheDocument();
    });

    it('shows exceptional match badge for high scores', () => {
        mockUseMatchDetails.mockReturnValue({
            data: makeModalData({ match: { overall_score: 90 } }),
            isLoading: false,
        });
        render(<MatchDetailsModal matchId="match-1" onClose={vi.fn()} />);
        expect(screen.getByText('Exceptional Match!')).toBeInTheDocument();
    });

    it('shows Remote badge when job is remote', () => {
        mockUseMatchDetails.mockReturnValue({ data: makeModalData(), isLoading: false });
        render(<MatchDetailsModal matchId="match-1" onClose={vi.fn()} />);
        expect(screen.getByText('Remote')).toBeInTheDocument();
    });

    it('shows job description when present', () => {
        mockUseMatchDetails.mockReturnValue({
            data: makeModalData({ job: { description: 'Great job opportunity' } }),
            isLoading: false,
        });
        render(<MatchDetailsModal matchId="match-1" onClose={vi.fn()} />);
        expect(screen.getByText('Great job opportunity')).toBeInTheDocument();
    });

    it('shows salary when salary_min is set', () => {
        mockUseMatchDetails.mockReturnValue({
            data: makeModalData({ job: { salary_min: 100000, salary_max: null, currency: 'USD' } }),
            isLoading: false,
        });
        render(<MatchDetailsModal matchId="match-1" onClose={vi.fn()} />);
        expect(screen.getByText('Salary')).toBeInTheDocument();
    });

    it('shows experience requirement when min_years_experience is set', () => {
        mockUseMatchDetails.mockReturnValue({
            data: makeModalData({ job: { min_years_experience: 5 } }),
            isLoading: false,
        });
        render(<MatchDetailsModal matchId="match-1" onClose={vi.fn()} />);
        expect(screen.getByText('5+ years')).toBeInTheDocument();
    });

    it('shows job level when job_level is set', () => {
        mockUseMatchDetails.mockReturnValue({
            data: makeModalData({ job: { job_level: 'Senior' } }),
            isLoading: false,
        });
        render(<MatchDetailsModal matchId="match-1" onClose={vi.fn()} />);
        expect(screen.getByText('Senior')).toBeInTheDocument();
    });

    it('shows degree required when requires_degree is true', () => {
        mockUseMatchDetails.mockReturnValue({
            data: makeModalData({ job: { requires_degree: true } }),
            isLoading: false,
        });
        render(<MatchDetailsModal matchId="match-1" onClose={vi.fn()} />);
        expect(screen.getByText('Required')).toBeInTheDocument();
    });

    it('shows degree not required when requires_degree is false', () => {
        mockUseMatchDetails.mockReturnValue({
            data: makeModalData({ job: { requires_degree: false } }),
            isLoading: false,
        });
        render(<MatchDetailsModal matchId="match-1" onClose={vi.fn()} />);
        expect(screen.getByText('Not Required')).toBeInTheDocument();
    });

    it('does not show exceptional badge for low overall score', () => {
        mockUseMatchDetails.mockReturnValue({
            data: makeModalData({ match: { overall_score: 70 } }),
            isLoading: false,
        });
        render(<MatchDetailsModal matchId="match-1" onClose={vi.fn()} />);
        expect(screen.queryByText('Exceptional Match!')).not.toBeInTheDocument();
    });

    it('does not show Want score when want_score is null', () => {
        mockUseMatchDetails.mockReturnValue({
            data: makeModalData({ match: { overall_score: 85, fit_score: 80, want_score: null } }),
            isLoading: false,
        });
        render(<MatchDetailsModal matchId="match-1" onClose={vi.fn()} />);
        expect(screen.queryByText('Want')).not.toBeInTheDocument();
    });

    it('renders required requirements section', () => {
        const requirements = [
            {
                requirement_id: 'req-1',
                req_type: 'required',
                is_covered: true,
                similarity_score: 0.92,
                requirement_text: 'React experience',
                evidence_text: 'Built React apps',
                evidence_section: 'Work Experience',
            },
        ];
        mockUseMatchDetails.mockReturnValue({
            data: makeModalData({ requirements }),
            isLoading: false,
        });
        render(<MatchDetailsModal matchId="match-1" onClose={vi.fn()} />);
        expect(screen.getByText('React experience')).toBeInTheDocument();
        expect(screen.getByText('Built React apps')).toBeInTheDocument();
        expect(screen.getByText('Source: Work Experience')).toBeInTheDocument();
    });

    it('renders preferred requirements section', () => {
        const requirements = [
            {
                requirement_id: 'req-2',
                req_type: 'preferred',
                is_covered: false,
                similarity_score: 0.45,
                requirement_text: 'GraphQL knowledge',
                evidence_text: null,
                evidence_section: null,
            },
        ];
        mockUseMatchDetails.mockReturnValue({
            data: makeModalData({ requirements }),
            isLoading: false,
        });
        render(<MatchDetailsModal matchId="match-1" onClose={vi.fn()} />);
        expect(screen.getByText('GraphQL knowledge')).toBeInTheDocument();
    });

    it('renders requirement without evidence text', () => {
        const requirements = [
            {
                requirement_id: 'req-3',
                req_type: 'required',
                is_covered: false,
                similarity_score: 0.3,
                requirement_text: 'Kubernetes expertise',
                evidence_text: null,
                evidence_section: null,
            },
        ];
        mockUseMatchDetails.mockReturnValue({
            data: makeModalData({ requirements }),
            isLoading: false,
        });
        render(<MatchDetailsModal matchId="match-1" onClose={vi.fn()} />);
        expect(screen.getByText('Kubernetes expertise')).toBeInTheDocument();
        expect(screen.queryByText('Evidence Found')).not.toBeInTheDocument();
    });

    it('shows covered/missing badges on requirement cards', () => {
        const requirements = [
            {
                requirement_id: 'req-4',
                req_type: 'required',
                is_covered: true,
                similarity_score: 0.9,
                requirement_text: 'TypeScript',
                evidence_text: null,
                evidence_section: null,
            },
            {
                requirement_id: 'req-5',
                req_type: 'required',
                is_covered: false,
                similarity_score: 0.2,
                requirement_text: 'Rust',
                evidence_text: null,
                evidence_section: null,
            },
        ];
        mockUseMatchDetails.mockReturnValue({
            data: makeModalData({ requirements }),
            isLoading: false,
        });
        render(<MatchDetailsModal matchId="match-1" onClose={vi.fn()} />);
        expect(screen.getByText('✓ Covered')).toBeInTheDocument();
        expect(screen.getByText('✗ Missing')).toBeInTheDocument();
    });

    it('shows both required and preferred requirement sections', () => {
        const requirements = [
            {
                requirement_id: 'req-6',
                req_type: 'required',
                is_covered: true,
                similarity_score: 0.9,
                requirement_text: 'Node.js',
                evidence_text: null,
                evidence_section: null,
            },
            {
                requirement_id: 'req-7',
                req_type: 'preferred',
                is_covered: false,
                similarity_score: 0.4,
                requirement_text: 'Docker',
                evidence_text: null,
                evidence_section: null,
            },
        ];
        mockUseMatchDetails.mockReturnValue({
            data: makeModalData({ requirements }),
            isLoading: false,
        });
        render(<MatchDetailsModal matchId="match-1" onClose={vi.fn()} />);
        expect(screen.getByText('Required (1)')).toBeInTheDocument();
        expect(screen.getByText('Preferred (1)')).toBeInTheDocument();
    });

    it('shows backdrop overlay when modal is open', () => {
        mockUseMatchDetails.mockReturnValue({ data: makeModalData(), isLoading: false });
        const onClose = vi.fn();
        const { container } = render(<MatchDetailsModal matchId="match-1" onClose={onClose} />);
        const backdrop = container.querySelector(String.raw`.bg-black\/60`);
        expect(backdrop).toBeInTheDocument();
        fireEvent.click(backdrop!);
        expect(onClose).toHaveBeenCalledTimes(1);
    });
});
