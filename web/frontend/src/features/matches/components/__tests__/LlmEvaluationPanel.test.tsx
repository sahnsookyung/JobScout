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
});
