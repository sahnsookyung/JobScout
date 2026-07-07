import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { LlmEvaluationPanel } from '../LlmEvaluationPanel';
import { toast } from '@/components/ui/Toast';
import { matchesApi } from '@/services/matchesApi';

vi.mock('lucide-react', () => ({
    RefreshCw: ({ className }: any) => <svg data-testid="refresh-icon" className={className} />,
    Sparkles: ({ className }: any) => <svg data-testid="sparkles-icon" className={className} />,
    RotateCcw: ({ className }: any) => <svg data-testid="rotate-icon" className={className} />,
    Trash2: ({ className }: any) => <svg data-testid="trash-icon" className={className} />,
}));

vi.mock('@/components/ui/Badge', () => ({
    Badge: ({ children, variant }: any) => (
        <span data-testid="badge" data-variant={variant}>{children}</span>
    ),
}));

vi.mock('@/components/ui/Button', () => ({
    Button: ({ children, onClick, disabled, ...props }: any) => (
        <button type="button" onClick={onClick} disabled={disabled} {...props}>
            {children}
        </button>
    ),
}));

vi.mock('@/components/ui/Toast', () => ({
    toast: { success: vi.fn(), error: vi.fn() },
}));

vi.mock('@/hooks/usePolicy', () => ({
    usePolicy: () => ({
        policy: {
            llm_judge_available: true,
            llm_judge_unavailable_reason: 'available',
        },
    }),
}));

vi.mock('@/services/matchesApi', () => ({
    matchesApi: {
        getLlmEvaluations: vi.fn(),
        generateLlmEvaluation: vi.fn(),
        deleteLlmEvaluation: vi.fn(),
        retryLlmEvaluation: vi.fn(),
    },
}));

vi.mock('@/utils/formatters', () => ({
    formatScore: (value: number) => `${Math.round(value)}%`,
}));

function renderPanel() {
    const queryClient = new QueryClient({
        defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
    });
    return render(
        <QueryClientProvider client={queryClient}>
            <LlmEvaluationPanel matchId="match-1" />
        </QueryClientProvider>,
    );
}

describe('LlmEvaluationPanel', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        vi.mocked(matchesApi.generateLlmEvaluation).mockResolvedValue({ data: {} } as never);
        vi.mocked(matchesApi.deleteLlmEvaluation).mockResolvedValue({ data: {} } as never);
    });

    it('shows queued auto top-N lifecycle copy', async () => {
        vi.mocked(matchesApi.getLlmEvaluations).mockResolvedValue({
            data: {
                success: true,
                count: 1,
                evaluations: [
                    {
                        id: 'eval-queued',
                        match_id: 'match-1',
                        job_id: 'job-1',
                        status: 'pending',
                        llm_score: null,
                        confidence: null,
                        verdict: null,
                        summary: null,
                        reason_codes: [],
                        requirement_verdicts: [],
                        provider: 'nvidia',
                        model: 'judge-model',
                        prompt_version: 'match-judge-v1',
                        schema_version: '1',
                        retryable: false,
                        queued_reason: 'auto_top_n',
                        queue_state: 'queued',
                    },
                ],
            },
        } as never);

        renderPanel();

        expect(await screen.findByText('Queued by auto top-N')).toBeInTheDocument();
        expect(screen.getByText('queued')).toBeInTheDocument();
        expect(
            screen.getAllByText(/Queued by auto top-N for LLM review/i).length,
        ).toBeGreaterThan(0);
    });

    it('retries retryable failures through the match API', async () => {
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
                        model: 'judge-model',
                        prompt_version: 'match-judge-v1',
                        schema_version: '1',
                        retryable: true,
                        retry_after_seconds: 40,
                    },
                ],
            },
        } as never);
        vi.mocked(matchesApi.retryLlmEvaluation).mockResolvedValue({
            data: {
                success: true,
                accepted: true,
                reused: false,
                message: 'Queued LLM evaluation retry.',
                evaluation: {
                    id: 'eval-retryable',
                    match_id: 'match-1',
                    job_id: 'job-1',
                    status: 'pending',
                    llm_score: null,
                    confidence: null,
                    verdict: null,
                    summary: null,
                    reason_codes: [],
                    requirement_verdicts: [],
                    provider: 'nvidia',
                    model: 'judge-model',
                    prompt_version: 'match-judge-v1',
                    schema_version: '1',
                    retryable: false,
                    queued_reason: 'retry_now',
                },
            },
        } as never);

        renderPanel();

        expect((await screen.findAllByText(/Retry available after 40s/i)).length).toBeGreaterThan(0);
        fireEvent.click(screen.getByRole('button', { name: /retry llm evaluation/i }));

        await waitFor(() => {
            expect(matchesApi.retryLlmEvaluation).toHaveBeenCalledWith('match-1', 'eval-retryable');
        });
        expect(toast.success).toHaveBeenCalledWith('Queued LLM evaluation retry');
    });

    it('shows stale successful analysis with ordered evidence fallback strengths', async () => {
        vi.mocked(matchesApi.getLlmEvaluations).mockResolvedValue({
            data: {
                success: true,
                count: 1,
                evaluations: [
                    {
                        id: 'eval-stale',
                        match_id: 'match-1',
                        job_id: 'job-1',
                        status: 'succeeded',
                        llm_score: 91,
                        confidence: 0.95,
                        verdict: 'strong',
                        summary: 'Historical review still useful for reading.',
                        reason_codes: [],
                        requirement_verdicts: [
                            { requirement_id: 'req_3', verdict: 'partial', reason: 'Partial third.' },
                            { requirement_id: 'req_2', verdict: 'strong', reason: 'Strong second.' },
                            { requirement_id: 'req_1', verdict: 'strong', reason: 'Strong first.' },
                            { requirement_id: 'req_4', verdict: 'missing', reason: 'Missing fourth.' },
                        ],
                        analysis: {
                            ranking_rationale: 'Good fit.',
                        },
                        effective_for_rerank: false,
                        ignored_for_rerank_reason: 'stale_job_content',
                        stale_status: 'stale',
                        freshness: {
                            status: 'stale',
                            reason: 'stale_job_content',
                            available_actions: ['regenerate_llm_evaluation'],
                        },
                        provider: 'nvidia',
                        model: 'judge-model',
                        prompt_version: 'match-judge-v1',
                        schema_version: '1',
                        retryable: false,
                    },
                ],
            },
        } as never);

        renderPanel();

        expect(await screen.findByText('Historical review still useful for reading.')).toBeInTheDocument();
        expect(screen.getByText('Evidence-based strengths')).toBeInTheDocument();
        expect(screen.getByText('Evidence-based gaps')).toBeInTheDocument();
        const first = screen.getByText('req_1: Strong first.');
        const second = screen.getByText('req_2: Strong second.');
        const third = screen.getByText('req_3: Partial third.');
        expect(first.compareDocumentPosition(second) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
        expect(second.compareDocumentPosition(third) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
        const firstVerdict = screen.getByText('req_1');
        const secondVerdict = screen.getByText('req_2');
        const thirdVerdict = screen.getByText('req_3');
        expect(firstVerdict.compareDocumentPosition(secondVerdict) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
        expect(secondVerdict.compareDocumentPosition(thirdVerdict) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
        expect(screen.getByText('req_4: Missing fourth.')).toBeInTheDocument();
        expect(screen.getByText(/Historical review shown\. Regenerate before ordering because stale job content\./i))
            .toBeInTheDocument();
    });

    it('trusts a valid LLM score even when the model verdict is harsh and renders evidence chips', async () => {
        vi.mocked(matchesApi.getLlmEvaluations).mockResolvedValue({
            data: {
                success: true,
                count: 1,
                evaluations: [
                    {
                        id: 'eval-valid-harsh-verdict',
                        match_id: 'match-1',
                        job_id: 'job-1',
                        status: 'succeeded',
                        llm_score: 65,
                        confidence: 0.95,
                        verdict: 'mismatch',
                        summary: 'Model text cites ev_1 and ev_15.',
                        reason_codes: [],
                        requirement_verdicts: [],
                        analysis: {
                            ranking_rationale: 'Strong evidence but bad score scale.',
                            evidence_references: [
                                {
                                    id: 'ev_1',
                                    source_section: 'Experience',
                                    source_text: 'Built high-throughput payment services.',
                                },
                                { id: 'ev_15' },
                            ],
                            score_quality: {
                                status: 'valid',
                                reason: null,
                                normalized_score: 65,
                                verdict: 'mismatch',
                            },
                        },
                        score_quality: {
                            status: 'valid',
                            reason: null,
                            normalized_score: 65,
                            verdict: 'mismatch',
                        },
                        effective_for_rerank: true,
                        ignored_for_rerank_reason: null,
                        stale_status: 'current',
                        provider: 'nvidia',
                        model: 'judge-model',
                        prompt_version: 'match-judge-v1',
                        schema_version: '1',
                        retryable: false,
                    },
                ],
            },
        } as never);

        renderPanel();

        expect(await screen.findByText('Model text cites ev_1 and ev_15.')).toBeInTheDocument();
        expect(screen.queryByText(/Score ignored for ranking/i)).not.toBeInTheDocument();
        expect(screen.queryByText(/Not used for ordering/i)).not.toBeInTheDocument();
        const referenceSummary = screen.getByText('Resume evidence references');
        const details = referenceSummary.closest('details');
        expect(details).toBeInTheDocument();
        expect(details).not.toHaveAttribute('open');
        expect(screen.getByText('2 refs')).toBeInTheDocument();
        expect(screen.getByText('ev_1')).toBeInTheDocument();
        expect(screen.getByText('Built high-throughput payment services.')).toBeInTheDocument();
        expect(screen.getByText('ev_15')).toBeInTheDocument();
    });

    it('shows structural invalid-score copy without expected verdict bands', async () => {
        vi.mocked(matchesApi.getLlmEvaluations).mockResolvedValue({
            data: {
                success: true,
                count: 1,
                evaluations: [
                    {
                        id: 'eval-invalid-score',
                        match_id: 'match-1',
                        job_id: 'job-1',
                        status: 'succeeded',
                        llm_score: null,
                        confidence: 0.95,
                        verdict: 'strong',
                        summary: 'Provider returned malformed score metadata.',
                        reason_codes: [],
                        requirement_verdicts: [],
                        analysis: {
                            score_quality: {
                                status: 'invalid',
                                reason: 'invalid_llm_score',
                                normalized_score: null,
                                verdict: 'strong',
                            },
                        },
                        score_quality: {
                            status: 'invalid',
                            reason: 'invalid_llm_score',
                            normalized_score: null,
                            verdict: 'strong',
                        },
                        effective_for_rerank: false,
                        ignored_for_rerank_reason: 'invalid_llm_score',
                        stale_status: 'ignored',
                        provider: 'nvidia',
                        model: 'judge-model',
                        prompt_version: 'match-judge-v1',
                        schema_version: '1',
                        retryable: false,
                    },
                ],
            },
        } as never);

        renderPanel();

        expect(await screen.findByText(/provider returned malformed score metadata/i)).toBeInTheDocument();
        expect(screen.getByText(/provider returned an invalid numeric score/i)).toBeInTheDocument();
        expect(screen.queryByText(/expected \d+-\d+%/i)).not.toBeInTheDocument();
    });
});
