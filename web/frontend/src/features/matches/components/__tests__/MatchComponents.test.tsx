import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { MatchCard } from '../MatchCard';
import { MatchDetailsModal } from '../MatchDetailsModal';
import { MatchFilters } from '../MatchFilters';
import { MatchList } from '../MatchList';
import { matchesApi } from '@/services/matchesApi';
import { toast } from '@/components/ui/Toast';

vi.mock('lucide-react', () => ({
    MapPin: ({ className }: any) => <svg data-testid="map-pin" className={className} />,
    Building2: ({ className }: any) => <svg data-testid="building2" className={className} />,
    Laptop: ({ className }: any) => <svg data-testid="laptop" className={className} />,
    Wifi: ({ className }: any) => <svg data-testid="wifi" className={className} />,
    Eye: ({ className }: any) => <svg data-testid="eye" className={className} />,
    EyeOff: ({ className }: any) => <svg data-testid="eye-off" className={className} />,
    Sparkles: ({ className }: any) => <svg data-testid="sparkles-icon" className={className} />,
    X: ({ className }: any) => <svg data-testid="x-icon" className={className} />,
    Download: ({ className }: any) => <svg data-testid="download-icon" className={className} />,
    RefreshCw: ({ className }: any) => <svg data-testid="refresh-icon" className={className} />,
    RotateCcw: ({ className }: any) => <svg data-testid="rotate-icon" className={className} />,
    Trash2: ({ className }: any) => <svg data-testid="trash-icon" className={className} />,
    Wand2: ({ className }: any) => <svg data-testid="wand-icon" className={className} />,
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
    RANKING_MODE_OPTIONS: [
        { value: 'balanced', label: 'Balanced' },
        { value: 'preference_first', label: 'Preference First' },
        { value: 'fit_first', label: 'Fit First' },
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

vi.mock('@/components/ui/Button', () => ({
    Button: ({ children, onClick, disabled, isLoading: _isLoading, ...props }: any) => (
        <button type="button" onClick={onClick} disabled={disabled} {...props}>
            {children}
        </button>
    ),
}));

vi.mock('@/services/matchesApi', () => ({
    matchesApi: {
        toggleHidden: vi.fn(),
        getLlmEvaluations: vi.fn(),
        generateLlmEvaluation: vi.fn(),
        deleteLlmEvaluation: vi.fn(),
        retryLlmEvaluation: vi.fn(),
    },
}));

const mockUseMatchDetails = vi.fn();
vi.mock('@/hooks/useMatchDetails', () => ({
    useMatchDetails: (id: any) => mockUseMatchDetails(id),
}));

const mockUseMatches = vi.fn();
vi.mock('@/hooks/useMatches', () => ({
    useMatches: (params: any) => mockUseMatches(params),
}));

const mockUseStats = vi.fn();
vi.mock('@/hooks/useStats', () => ({
    useStats: (params: any) => mockUseStats(params),
}));

const mockUsePolicy = vi.fn();
vi.mock('@/hooks/usePolicy', () => ({
    usePolicy: () => mockUsePolicy(),
}));

function makeQueryWrapper() {
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    return ({ children }: any) => (
        <QueryClientProvider client={qc}>{children}</QueryClientProvider>
    );
}

function makeMatch(overrides: Record<string, any> = {}) {
    return {
        match_id: 'match-1',
        job_id: null,
        title: 'Senior Engineer',
        company: 'TechCorp',
        location: 'San Francisco, CA',
        is_remote: false,
        is_hidden: false,
        fit_score: 85,
        preference_score: null,
        penalties: 0,
        required_coverage: 0.9,
        preferred_requirement_coverage: 0,
        match_type: 'vector_match',
        created_at: null,
        calculated_at: null,
        ranking_mode_used: null,
        dominant_reason_code: null,
        explanation_label: null,
        balanced_primary_score: null,
        missing_scores: [],
        selection_tier: 'primary' as const,
        ...overrides,
    };
}

describe('MatchFilters', () => {
    const defaultProps = {
        status: 'active' as const,
        onStatusChange: vi.fn(),
        remoteOnly: false,
        onRemoteOnlyChange: vi.fn(),
        rankingMode: 'balanced' as const,
        onRankingModeChange: vi.fn(),
        showHidden: false,
        onShowHiddenChange: vi.fn(),
    };

    beforeEach(() => vi.clearAllMocks());

    it('renders filter controls and toggles', () => {
        render(<MatchFilters {...defaultProps} />);
        expect(screen.getByText('Active')).toBeInTheDocument();
        expect(screen.getByText('Stale')).toBeInTheDocument();
        expect(screen.getByText('All')).toBeInTheDocument();
        expect(screen.getByText('Balanced')).toBeInTheDocument();
        expect(screen.getByText('Preference First')).toBeInTheDocument();
        expect(screen.getByText('Fit First')).toBeInTheDocument();
        expect(screen.getByText('Remote only')).toBeInTheDocument();
        expect(screen.getByText('Hidden')).toBeInTheDocument();
    });

    it('calls change handlers for selects and toggles', () => {
        const onStatusChange = vi.fn();
        const onRankingModeChange = vi.fn();
        const onRemoteOnlyChange = vi.fn();
        const onShowHiddenChange = vi.fn();

        render(
            <MatchFilters
                {...defaultProps}
                onStatusChange={onStatusChange}
                onRankingModeChange={onRankingModeChange}
                onRemoteOnlyChange={onRemoteOnlyChange}
                onShowHiddenChange={onShowHiddenChange}
            />
        );

        const selects = screen.getAllByRole('combobox');
        fireEvent.change(selects[0], { target: { value: 'stale' } });
        fireEvent.change(selects[1], { target: { value: 'fit_first' } });
        const checkboxes = screen.getAllByRole('checkbox');
        fireEvent.click(checkboxes[0]);
        fireEvent.click(checkboxes[1]);

        expect(onStatusChange).toHaveBeenCalledWith('stale');
        expect(onRankingModeChange).toHaveBeenCalledWith('fit_first');
        expect(onRemoteOnlyChange).toHaveBeenCalledWith(true);
        expect(onShowHiddenChange).toHaveBeenCalledWith(true);
    });
});

describe('MatchCard', () => {
    beforeEach(() => vi.clearAllMocks());

    it('renders title, company, location, and formatted score', () => {
        render(<MatchCard match={makeMatch()} onSelect={vi.fn()} />, { wrapper: makeQueryWrapper() });
        expect(screen.getByText('Senior Engineer')).toBeInTheDocument();
        expect(screen.getByText('TechCorp')).toBeInTheDocument();
        expect(screen.getByText('San Francisco, CA')).toBeInTheDocument();
        expect(screen.getAllByText('85%').length).toBeGreaterThan(0);
        expect(screen.getByText('vector match')).toBeInTheDocument();
    });

    it('shows remote and hidden affordances when present', () => {
        const { rerender } = render(
            <MatchCard match={makeMatch({ is_remote: true })} onSelect={vi.fn()} />,
            { wrapper: makeQueryWrapper() }
        );
        expect(screen.getByText('Remote')).toBeInTheDocument();
        expect(screen.getByTestId('wifi')).toBeInTheDocument();

        rerender(<MatchCard match={makeMatch({ is_hidden: true })} onSelect={vi.fn()} />);
        expect(screen.getByText('hidden')).toBeInTheDocument();
        expect(screen.getByTestId('eye-off')).toBeInTheDocument();
    });

    it('renders the featured top-match footer only for featured cards', () => {
        const { rerender } = render(
            <MatchCard match={makeMatch({ fit_score: 85 })} onSelect={vi.fn()} featured />,
            { wrapper: makeQueryWrapper() }
        );
        expect(screen.getByText('Top match')).toBeInTheDocument();

        rerender(<MatchCard match={makeMatch({ fit_score: 70 })} onSelect={vi.fn()} />);
        expect(screen.queryByText('Top match')).not.toBeInTheDocument();
    });

    it('calls onSelect from the card button and keeps hide separate', () => {
        const onSelect = vi.fn();
        render(<MatchCard match={makeMatch()} onSelect={onSelect} />, { wrapper: makeQueryWrapper() });

        fireEvent.click(screen.getByRole('button', { name: /view details for senior engineer at techcorp/i }));
        expect(onSelect).toHaveBeenCalledWith('match-1');

        fireEvent.click(screen.getByRole('button', { name: /^hide$/i }));
        expect(onSelect).toHaveBeenCalledTimes(1);
    });

    it('updates the hide button aria-pressed state', () => {
        const { rerender } = render(
            <MatchCard match={makeMatch({ is_hidden: false })} onSelect={vi.fn()} />,
            { wrapper: makeQueryWrapper() }
        );
        expect(screen.getByRole('button', { name: /^hide$/i })).toHaveAttribute('aria-pressed', 'false');

        rerender(<MatchCard match={makeMatch({ is_hidden: true })} onSelect={vi.fn()} />);
        expect(screen.getByRole('button', { name: /^unhide$/i })).toHaveAttribute('aria-pressed', 'true');
    });

    it('shows a visible focus outline on the card trigger for keyboard users', () => {
        render(<MatchCard match={makeMatch()} onSelect={vi.fn()} />, { wrapper: makeQueryWrapper() });

        expect(
            screen.getByRole('button', { name: /view details for senior engineer at techcorp/i }),
        ).toHaveClass('focus-visible:outline-accent');
    });

    it('shows excluded matches without a hide toggle', () => {
        render(
            <MatchCard
                match={makeMatch({
                    selection_tier: 'excluded',
                    excluded_reason: 'below_threshold',
                })}
                onSelect={vi.fn()}
            />,
            { wrapper: makeQueryWrapper() }
        );

        expect(screen.getByText('below threshold')).toBeInTheDocument();
        expect(screen.queryByRole('button', { name: /^hide$/i })).not.toBeInTheDocument();
    });

    it('shows compact LLM evaluation markers', () => {
        render(
            <MatchCard
                match={makeMatch({
                    llm_evaluation_status: 'succeeded',
                    llm_score: 91,
                })}
                onSelect={vi.fn()}
            />,
            { wrapper: makeQueryWrapper() }
        );

        expect(screen.getByText('LLM judged')).toBeInTheDocument();
        expect(screen.getByTestId('sparkles-icon')).toBeInTheDocument();
    });

    it('persists hide changes and reports failures', async () => {
        vi.mocked(matchesApi.toggleHidden).mockResolvedValueOnce({
            data: { is_hidden: true },
        } as never);

        render(<MatchCard match={makeMatch()} onSelect={vi.fn()} />, { wrapper: makeQueryWrapper() });

        fireEvent.click(screen.getByRole('button', { name: /^hide$/i }));

        await waitFor(() => {
            expect(matchesApi.toggleHidden).toHaveBeenCalledWith('match-1');
        });
        expect(toast.success).toHaveBeenCalledWith(
            'Hidden from your list',
            expect.objectContaining({ duration: 5000 }),
        );

        vi.mocked(matchesApi.toggleHidden).mockRejectedValueOnce(new Error('boom'));
        fireEvent.click(screen.getByRole('button', { name: /^hide$/i }));

        await waitFor(() => {
            expect(toast.error).toHaveBeenCalledWith('Could not update job visibility.');
        });
    });
});

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
        mockUseStats.mockReturnValue({ data: { total_scored: 0, excluded_count: 0 } });
        mockUsePolicy.mockReturnValue({
            policy: { min_fit: 55, top_k: 50, min_jd_required_coverage: null },
            isLoading: false,
            updatePolicy: vi.fn(),
            applyPreset: vi.fn(),
        });
    });

    it('shows the loading state', () => {
        mockUseMatches.mockReturnValue({ data: null, isLoading: true, error: null, refetch: vi.fn() });
        render(<MatchList onMatchSelect={vi.fn()} />, { wrapper: makeQueryWrapper() });
        expect(screen.getByLabelText('Loading matches')).toBeInTheDocument();
        expect(screen.getByText('Fetching matches')).toBeInTheDocument();
    });

    it('shows the error state and retries', () => {
        const refetch = vi.fn();
        mockUseMatches.mockReturnValue({ data: null, isLoading: false, error: new Error('fail'), refetch });
        render(<MatchList onMatchSelect={vi.fn()} />, { wrapper: makeQueryWrapper() });
        expect(screen.getByText(/Something went wrong loading your matches/i)).toBeInTheDocument();
        fireEvent.click(screen.getByText('Try again'));
        expect(refetch).toHaveBeenCalledTimes(1);
    });

    it('shows match totals and singular/plural copy', () => {
        const matches = [makeMatch(), makeMatch({ match_id: 'match-2', title: 'Dev' })];
        mockUseMatches.mockReturnValue({ data: { matches }, isLoading: false, error: null, refetch: vi.fn() });
        const { container, rerender } = render(<MatchList onMatchSelect={vi.fn()} />, { wrapper: makeQueryWrapper() });
        expect(container.querySelector('header')).toHaveTextContent('2');
        expect(container.querySelector('header')).toHaveTextContent('matches');

        mockUseMatches.mockReturnValue({ data: { matches: [makeMatch()] }, isLoading: false, error: null, refetch: vi.fn() });
        rerender(<MatchList onMatchSelect={vi.fn()} />);
        expect(container.querySelector('header')).toHaveTextContent('1');
        expect(container.querySelector('header')).toHaveTextContent('match');
    });

    it('shows the empty state and persists hidden preference', async () => {
        mockUseMatches.mockReturnValue({ data: { matches: [] }, isLoading: false, error: null, refetch: vi.fn() });
        render(<MatchList onMatchSelect={vi.fn()} />, { wrapper: makeQueryWrapper() });
        expect(screen.getByText(/Nothing to show yet/i)).toBeInTheDocument();

        const checkboxes = screen.getAllByRole('checkbox');
        fireEvent.click(checkboxes[1]);
        await waitFor(() => {
            expect(localStorage.getItem('jobscout_show_hidden')).toBe('true');
        });
    });

    it('reads the hidden preference from localStorage', () => {
        storageMock.setItem('jobscout_show_hidden', 'true');
        mockUseMatches.mockReturnValue({ data: { matches: [] }, isLoading: false, error: null, refetch: vi.fn() });
        render(<MatchList onMatchSelect={vi.fn()} />, { wrapper: makeQueryWrapper() });
        expect(screen.getAllByRole('checkbox')[1]).toBeChecked();
    });

    it('shows degraded copy and can reveal all processed results', () => {
        mockUseStats.mockReturnValue({ data: { total_scored: 2, excluded_count: 2 } });
        mockUseMatches.mockImplementation((params) => ({
            data: {
                matches: params.min_fit === undefined
                    ? [makeMatch({ match_id: 'match-2', fit_score: 62 })]
                    : [makeMatch({ scoring_degraded_reason: 'remote_unavailable' })],
            },
            isLoading: false,
            error: null,
            refetch: vi.fn(),
        }));

        render(<MatchList onMatchSelect={vi.fn()} />, { wrapper: makeQueryWrapper() });

        expect(mockUseMatches).toHaveBeenLastCalledWith(
            expect.objectContaining({
                tier: 'primary',
                min_fit: 55,
                top_k: 50,
                limit: 50,
                cursor: null,
                page_mode: 'cursor',
                view: 'compact',
                include: 'llm',
            }),
        );
        expect(screen.getByText(/remote cross-encoder is unreachable/i)).toBeInTheDocument();

        mockUseMatches.mockImplementation((params) => ({
            data: { matches: params.tier === 'all' ? [makeMatch({ match_id: 'match-3' })] : [] },
            isLoading: false,
            error: null,
            refetch: vi.fn(),
        }));
        fireEvent.click(screen.getByRole('checkbox', { name: /all matched candidates \(2\)/i }));

        expect(mockUseMatches).toHaveBeenLastCalledWith(
            expect.objectContaining({
                tier: 'all',
                min_fit: undefined,
                top_k: undefined,
                limit: 100,
                cursor: null,
                page_mode: 'cursor',
            }),
        );
    });

    it('loads all matched candidates in pages after the user opts in', async () => {
        mockUseStats.mockReturnValue({ data: { total_scored: 3, excluded_count: 2 } });
        mockUseMatches.mockImplementation((params) => ({
            data: params.tier === 'all'
                ? {
                    matches: [
                        makeMatch({
                            match_id: params.cursor ? 'match-1' : 'match-0',
                            title: params.cursor ? 'Candidate 1' : 'Candidate 0',
                        }),
                    ],
                    total: 3,
                    has_more: !params.cursor,
                    next_cursor: params.cursor ? null : 'cursor-1',
                }
                : {
                    matches: [makeMatch({ match_id: 'base-1', title: 'Base Candidate' })],
                    total: 1,
                    has_more: false,
                },
            isLoading: false,
            isFetching: false,
            error: null,
            refetch: vi.fn(),
        }));

        render(<MatchList onMatchSelect={vi.fn()} />, { wrapper: makeQueryWrapper() });

        expect(mockUseMatches).toHaveBeenLastCalledWith(
            expect.objectContaining({
                tier: 'primary',
                top_k: 50,
                limit: 50,
                cursor: null,
                page_mode: 'cursor',
            }),
        );

        fireEvent.click(screen.getByRole('checkbox', { name: /all matched candidates \(3\)/i }));

        await waitFor(() => {
            expect(screen.getByText('Candidate 0')).toBeInTheDocument();
        });
        expect(screen.getByText('1 of 3 matched candidates')).toBeInTheDocument();
        expect(mockUseMatches).toHaveBeenLastCalledWith(
            expect.objectContaining({ top_k: undefined, limit: 100, cursor: null }),
        );

        fireEvent.click(screen.getByRole('button', { name: /load more candidates/i }));

        await waitFor(() => {
            expect(mockUseMatches).toHaveBeenLastCalledWith(
                expect.objectContaining({ top_k: undefined, limit: 100, cursor: 'cursor-1' }),
            );
        });
    });

    it('renders the top-match treatment for the strongest visible result', () => {
        mockUseMatches.mockReturnValue({
            data: {
                matches: [
                    makeMatch({ fit_score: 86 }),
                    makeMatch({ match_id: 'match-2', title: 'Platform Engineer', fit_score: 70 }),
                ],
            },
            isLoading: false,
            error: null,
            refetch: vi.fn(),
        });

        render(<MatchList onMatchSelect={vi.fn()} />, { wrapper: makeQueryWrapper() });

        expect(screen.getByText('Top match')).toBeInTheDocument();
        expect(screen.getByText('1 strong')).toBeInTheDocument();
    });

    it('uses relaxed policy filters without requesting all candidates by default', () => {
        mockUsePolicy.mockReturnValue({
            policy: { min_fit: 0, top_k: 100, min_jd_required_coverage: null },
            isLoading: false,
            updatePolicy: vi.fn(),
            applyPreset: vi.fn(),
        });
        mockUseStats.mockReturnValue({ data: { total_scored: 2, excluded_count: 2 } });
        mockUseMatches.mockReturnValue({
            data: {
                matches: [
                    makeMatch({
                        match_id: 'excluded-1',
                        title: 'Backend Engineer',
                        fit_score: 35,
                        selection_tier: 'excluded',
                        excluded_reason: 'below_min_fit',
                    }),
                ],
            },
            isLoading: false,
            error: null,
            refetch: vi.fn(),
        });

        render(<MatchList onMatchSelect={vi.fn()} />, { wrapper: makeQueryWrapper() });

        expect(mockUseMatches).toHaveBeenLastCalledWith(
            expect.objectContaining({
                tier: 'primary',
                min_fit: 0,
                top_k: 100,
                limit: 100,
                cursor: null,
                page_mode: 'cursor',
            }),
        );
        expect(screen.getByText('Backend Engineer')).toBeInTheDocument();
        expect(screen.queryByText(/Nothing above your threshold/i)).not.toBeInTheDocument();
    });
});

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
            fit_score: 86,
            preference_score: 0.74,
            required_coverage: 0.8,
            preferred_requirement_coverage: 0.5,
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
        mockUseMatchDetails.mockReturnValue({ data: undefined, isLoading: false });
        mockUsePolicy.mockReturnValue({
            policy: {
                min_fit: 55,
                top_k: 50,
                min_jd_required_coverage: null,
                llm_judge_available: true,
                llm_judge_unavailable_reason: 'available',
            },
            isLoading: false,
            updatePolicy: vi.fn(),
            applyPreset: vi.fn(),
        });
        vi.mocked(matchesApi.getLlmEvaluations).mockResolvedValue({
            data: { success: true, count: 0, evaluations: [] },
        } as never);
        vi.mocked(matchesApi.generateLlmEvaluation).mockResolvedValue({
            data: {
                success: true,
                reused: false,
                message: 'Generated LLM evaluation.',
                evaluation: {
                    id: 'eval-1',
                    match_id: 'match-1',
                    job_id: 'job-1',
                    status: 'succeeded',
                    llm_score: 91,
                    confidence: 0.9,
                    verdict: 'good',
                    summary: 'Relevant backend role.',
                    reason_codes: ['skills_match'],
                    requirement_verdicts: [],
                    provider: 'openai',
                    model: 'judge-model',
                    prompt_version: 'match-judge-v1',
                    schema_version: '1',
                    retryable: false,
                },
            },
        } as never);
        vi.mocked(matchesApi.deleteLlmEvaluation).mockResolvedValue({
            data: {
                success: true,
                reused: false,
                message: 'Deleted LLM evaluation.',
                evaluation: null,
            },
        } as never);
        vi.mocked(matchesApi.retryLlmEvaluation).mockResolvedValue({
            data: {
                success: true,
                reused: false,
                accepted: true,
                message: 'Queued LLM evaluation retry.',
                evaluation: {
                    id: 'eval-retry',
                    match_id: 'match-1',
                    job_id: 'job-1',
                    status: 'pending',
                    llm_score: null,
                    confidence: null,
                    verdict: null,
                    summary: null,
                    reason_codes: [],
                    requirement_verdicts: [],
                    provider: 'openai',
                    model: 'judge-model',
                    prompt_version: 'match-judge-v1',
                    schema_version: '1',
                    retryable: false,
                    queued_reason: 'retry_now',
                },
            },
        } as never);
    });

    it('renders nothing when matchId is null', () => {
        const { container } = render(<MatchDetailsModal matchId={null} onClose={vi.fn()} />, { wrapper: makeQueryWrapper() });
        expect(container.firstChild).toBeNull();
    });

    it('shows loading and error states', () => {
        const { rerender } = render(<MatchDetailsModal matchId="match-1" onClose={vi.fn()} />, { wrapper: makeQueryWrapper() });
        expect(screen.getByText(/Failed to load match details/i)).toBeInTheDocument();

        mockUseMatchDetails.mockReturnValue({ data: undefined, isLoading: true });
        rerender(<MatchDetailsModal matchId="match-1" onClose={vi.fn()} />);
        expect(screen.getByText('Loading match')).toBeInTheDocument();
    });

    it('closes from the close button, escape, and backdrop', () => {
        mockUseMatchDetails.mockReturnValue({ data: makeModalData(), isLoading: false });
        const onClose = vi.fn();
        const { container } = render(<MatchDetailsModal matchId="match-1" onClose={onClose} />, { wrapper: makeQueryWrapper() });

        fireEvent.click(screen.getAllByLabelText('Close match details')[0]);
        fireEvent.keyDown(document, { key: 'Escape' });
        fireEvent.click(container.querySelector('button.fixed.inset-0')!);

        expect(onClose).toHaveBeenCalledTimes(3);
    });

    it('restores focus to the opener when the modal closes', async () => {
        mockUseMatchDetails.mockReturnValue({ data: makeModalData(), isLoading: false });
        const onClose = vi.fn();
        const { rerender } = render(
            <>
                <button type="button">Open match</button>
                <MatchDetailsModal matchId="match-1" onClose={onClose} />
            </>,
            { wrapper: makeQueryWrapper() }
        );

        const opener = screen.getByRole('button', { name: /open match/i });
        opener.focus();
        fireEvent.click(screen.getAllByLabelText('Close match details')[0]);
        rerender(
            <>
                <button type="button">Open match</button>
                <MatchDetailsModal matchId={null} onClose={onClose} />
            </>
        );

        await waitFor(() => {
            expect(opener).toHaveFocus();
        });
    });

    it('renders the main job details', () => {
        mockUseMatchDetails.mockReturnValue({ data: makeModalData(), isLoading: false });
        render(<MatchDetailsModal matchId="match-1" onClose={vi.fn()} />, { wrapper: makeQueryWrapper() });
        expect(screen.getByText('Match details')).toBeInTheDocument();
        expect(screen.getByText('Staff Engineer')).toBeInTheDocument();
        expect(screen.getByText('Acme')).toBeInTheDocument();
        expect(screen.getByText('Remote')).toBeInTheDocument();
    });

    it('renders LLM evaluation controls and supports delete', async () => {
        vi.mocked(matchesApi.getLlmEvaluations).mockResolvedValue({
            data: {
                success: true,
                count: 1,
                evaluations: [
                    {
                        id: 'eval-delete',
                        match_id: 'match-1',
                        job_id: 'job-1',
                        status: 'succeeded',
                        llm_score: 91,
                        confidence: 0.9,
                        verdict: 'good',
                        summary: 'Relevant backend role.',
                        reason_codes: ['skills_match'],
                        requirement_verdicts: [],
                        provider: 'openai',
                        model: 'judge-model',
                        prompt_version: 'match-judge-v1',
                        schema_version: '1',
                        retryable: false,
                    },
                ],
            },
        } as never);
        mockUseMatchDetails.mockReturnValue({ data: makeModalData(), isLoading: false });

        render(<MatchDetailsModal matchId="match-1" onClose={vi.fn()} />, { wrapper: makeQueryWrapper() });

        expect(await screen.findByText('Second-pass relevance review')).toBeInTheDocument();
        expect(screen.getByText('Generate a second-pass relevance review')).toBeInTheDocument();
        expect(screen.getByText('Delete this cached LLM review')).toBeInTheDocument();
        expect(screen.getByRole('button', { name: /^generate llm evaluation$/i })).toHaveClass('hover:-translate-y-0.5');
        await screen.findByText('Relevant backend role.');
        fireEvent.click(screen.getByRole('button', { name: /delete llm evaluation/i }));

        await waitFor(() => {
            expect(matchesApi.deleteLlmEvaluation).toHaveBeenCalledWith('match-1', 'eval-delete');
        });
    });

    it('shows automatic progress without a spinner while an LLM evaluation is running', async () => {
        vi.mocked(matchesApi.getLlmEvaluations).mockResolvedValue({
            data: {
                success: true,
                count: 1,
                evaluations: [
                    {
                        id: 'eval-running',
                        match_id: 'match-1',
                        job_id: 'job-1',
                        status: 'running',
                        llm_score: null,
                        confidence: null,
                        verdict: null,
                        summary: null,
                        reason_codes: [],
                        requirement_verdicts: [],
                        provider: 'cerebras',
                        model: 'gpt-oss-120b',
                        prompt_version: 'match-judge-v1',
                        schema_version: '1',
                        retryable: false,
                    },
                ],
            },
        } as never);
        mockUseMatchDetails.mockReturnValue({ data: makeModalData(), isLoading: false });

        const { container } = render(<MatchDetailsModal matchId="match-1" onClose={vi.fn()} />, { wrapper: makeQueryWrapper() });

        expect((await screen.findAllByText(/LLM review is running against the full resume and job description/i)).length)
            .toBeGreaterThan(0);
        const generateButton = screen.getByRole('button', { name: /^generate llm evaluation$/i });
        expect(generateButton).toBeDisabled();
        expect(screen.getByTestId('sparkles-icon').parentElement).toHaveClass('animate-pulse');
        expect(container.querySelector('.animate-spin')).not.toBeInTheDocument();
    });

    it('shows retry action for retryable LLM failures', async () => {
        vi.mocked(matchesApi.getLlmEvaluations).mockResolvedValue({
            data: {
                success: true,
                count: 1,
                evaluations: [
                    {
                        id: 'eval-retryable',
                        match_id: 'match-1',
                        job_id: 'job-1',
                        status: 'failed',
                        llm_score: null,
                        confidence: null,
                        verdict: null,
                        summary: null,
                        reason_codes: [],
                        requirement_verdicts: [],
                        provider: 'nvidia',
                        model: 'nvidia-model',
                        prompt_version: 'match-judge-v1',
                        schema_version: '1',
                        retryable: true,
                        retry_after_seconds: 60,
                    },
                ],
            },
        } as never);
        mockUseMatchDetails.mockReturnValue({ data: makeModalData(), isLoading: false });

        render(<MatchDetailsModal matchId="match-1" onClose={vi.fn()} />, { wrapper: makeQueryWrapper() });

        expect((await screen.findAllByText(/Retryable failure/i)).length).toBeGreaterThan(0);
        fireEvent.click(screen.getByRole('button', { name: /retry llm evaluation/i }));

        await waitFor(() => {
            expect(matchesApi.retryLlmEvaluation).toHaveBeenCalledWith('match-1', 'eval-retryable');
        });
    });

    it('disables LLM generation when provider credentials are missing', async () => {
        mockUsePolicy.mockReturnValue({
            policy: {
                min_fit: 55,
                top_k: 50,
                min_jd_required_coverage: null,
                llm_judge_available: false,
                llm_judge_unavailable_reason: 'credentials_missing',
            },
            isLoading: false,
            updatePolicy: vi.fn(),
            applyPreset: vi.fn(),
        });
        mockUseMatchDetails.mockReturnValue({ data: makeModalData(), isLoading: false });

        render(<MatchDetailsModal matchId="match-1" onClose={vi.fn()} />, { wrapper: makeQueryWrapper() });

        expect(await screen.findByText('LLM judge unavailable: LLM judge provider credentials are missing.')).toBeInTheDocument();
        const generateButton = screen.getByRole('button', { name: /^generate llm evaluation$/i });
        expect(generateButton).toBeDisabled();

        fireEvent.click(generateButton);
        expect(matchesApi.generateLlmEvaluation).not.toHaveBeenCalled();
    });

    it('renders semantic fit details when present', () => {
        mockUseMatchDetails.mockReturnValue({
            data: makeModalData({
                match: {
                    fit_confidence: 0.84,
                    fit_scorer: { name: 'llm_semantic_fit', version: '1' },
                    fit_explanation: {
                        summary: 'Covered 2 of 3 required requirements (67%) and 1 of 1 preferred requirements (100%).',
                        diagnostics: {
                            effective_fit_mode: 'llm',
                            provider_route: 'remote',
                        },
                        retrieval: {
                            mode: 'hybrid',
                            sources: ['dense', 'lexical'],
                        },
                    },
                },
            }),
            isLoading: false,
        });

        render(<MatchDetailsModal matchId="match-1" onClose={vi.fn()} />, { wrapper: makeQueryWrapper() });
        expect(screen.getByText('Semantic fit')).toBeInTheDocument();
        expect(
            screen.getByText((_, node) => node?.textContent === 'Confidence 84%')
        ).toBeInTheDocument();
        expect(screen.getByText(/llm semantic fit/i)).toBeInTheDocument();
        expect(screen.getByText(/Hybrid retrieval/i)).toBeInTheDocument();
        expect(screen.getByText(/^llm$/i)).toBeInTheDocument();
        expect(screen.getAllByText(/^remote$/i)).toHaveLength(2);
        expect(screen.getByText(/Candidate generation used dense \+ lexical\./i)).toBeInTheDocument();
    });

    it('renders job metadata and requirements', () => {
        mockUseMatchDetails.mockReturnValue({
            data: makeModalData({
                job: {
                    salary_min: 100000,
                    currency: 'USD',
                    min_years_experience: 5,
                    job_level: 'Senior',
                    requires_degree: false,
                    description: 'Great job opportunity',
                },
                requirements: [
                    {
                        requirement_id: 'req-1',
                        req_type: 'required',
                        is_covered: true,
                        similarity_score: 0.92,
                        requirement_text: 'React experience',
                        evidence_text: 'Built React apps',
                        evidence_section: 'Work Experience',
                    },
                    {
                        requirement_id: 'req-2',
                        req_type: 'preferred',
                        is_covered: false,
                        similarity_score: 0.45,
                        requirement_text: 'GraphQL knowledge',
                        evidence_text: null,
                        evidence_section: null,
                    },
                ],
            }),
            isLoading: false,
        });

        render(<MatchDetailsModal matchId="match-1" onClose={vi.fn()} />, { wrapper: makeQueryWrapper() });
        expect(screen.getByText('Salary')).toBeInTheDocument();
        expect(screen.getByText('5+ years')).toBeInTheDocument();
        expect(screen.getByText('Senior')).toBeInTheDocument();
        expect(screen.getByText('Not required')).toBeInTheDocument();
        expect(screen.getAllByText('Required').length).toBeGreaterThan(0);
        expect(screen.getAllByText('Preferred').length).toBeGreaterThan(0);
        expect(screen.getByText('Covered')).toBeInTheDocument();
        expect(screen.getByText('Missing')).toBeInTheDocument();
        expect(screen.getByText('Built React apps')).toBeInTheDocument();
        expect(screen.getByText('Source: Work Experience')).toBeInTheDocument();
        expect(screen.getByText('Great job opportunity')).toBeInTheDocument();
    });

    it('prefers semantic verdict copy over raw percentages when available', () => {
        mockUseMatchDetails.mockReturnValue({
            data: makeModalData({
                requirements: [
                    {
                        requirement_id: 'req-8',
                        req_type: 'required',
                        is_covered: false,
                        similarity_score: 0.92,
                        requirement_text: 'Python API development',
                        evidence_text: 'Built internal APIs',
                        evidence_section: 'Work Experience',
                    },
                ],
                match: {
                    fit_explanation: {
                        summary: 'Covered 0 of 1 required requirements (0%) and 0 of 0 preferred requirements (0%).',
                        requirement_verdicts: [
                            {
                                requirement_id: 'req-8',
                                verdict: 'partial',
                                reason: 'Evidence is related but does not clearly satisfy the requirement.',
                                evidence_text: 'Built internal APIs',
                                evidence_section: 'Work Experience',
                            },
                        ],
                    },
                },
            }),
            isLoading: false,
        });

        render(<MatchDetailsModal matchId="match-1" onClose={vi.fn()} />, { wrapper: makeQueryWrapper() });
        expect(screen.getByText('Partial')).toBeInTheDocument();
        expect(screen.getByText('Why')).toBeInTheDocument();
        expect(screen.queryByText('92% match')).not.toBeInTheDocument();
    });
});
