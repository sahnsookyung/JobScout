import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { JobInventoryPanel } from '../JobInventoryPanel';

const mockUseJobs = vi.hoisted(() => vi.fn());
const mockProcessJobs = vi.hoisted(() => vi.fn());
const mockGetPipelineStatus = vi.hoisted(() => vi.fn());
const mockGetProcessingBlockers = vi.hoisted(() => vi.fn());
const mockGetPipelineRuns = vi.hoisted(() => vi.fn());
const mockGetPipelineRun = vi.hoisted(() => vi.fn());
const mockCancelPipelineRun = vi.hoisted(() => vi.fn());
const mockRequeuePipelineRun = vi.hoisted(() => vi.fn());
const mockRetryPipelineRun = vi.hoisted(() => vi.fn());
const mockGetLlmEvaluationQueueStatus = vi.hoisted(() => vi.fn());
const mockPauseLlmEvaluationQueue = vi.hoisted(() => vi.fn());
const mockResumeLlmEvaluationQueue = vi.hoisted(() => vi.fn());
const mockRetryLlmEvaluationQueue = vi.hoisted(() => vi.fn());
const mockGetLlmProviderStatus = vi.hoisted(() => vi.fn());
const mockRunLlmProviderCanaries = vi.hoisted(() => vi.fn());
const mockResetLlmProviderCircuit = vi.hoisted(() => vi.fn());

vi.mock('@/hooks/useJobs', () => ({
    useJobs: (params: any, enabled: boolean) => mockUseJobs(params, enabled),
}));

vi.mock('@/services/pipelineApi', () => ({
    pipelineApi: {
        processJobs: mockProcessJobs,
        getPipelineStatus: mockGetPipelineStatus,
    },
}));

vi.mock('@/services/jobsApi', () => ({
    jobsApi: {
        getProcessingBlockers: mockGetProcessingBlockers,
    },
}));

vi.mock('@/services/pipelineRunsApi', () => ({
    pipelineRunsApi: {
        getPipelineRuns: mockGetPipelineRuns,
        getPipelineRun: mockGetPipelineRun,
        cancelPipelineRun: mockCancelPipelineRun,
        requeuePipelineRun: mockRequeuePipelineRun,
        retryPipelineRun: mockRetryPipelineRun,
        getLlmEvaluationQueueStatus: mockGetLlmEvaluationQueueStatus,
        pauseLlmEvaluationQueue: mockPauseLlmEvaluationQueue,
        resumeLlmEvaluationQueue: mockResumeLlmEvaluationQueue,
        retryLlmEvaluationQueue: mockRetryLlmEvaluationQueue,
        getLlmProviderStatus: mockGetLlmProviderStatus,
        runLlmProviderCanaries: mockRunLlmProviderCanaries,
        resetLlmProviderCircuit: mockResetLlmProviderCircuit,
    },
}));

vi.mock('lucide-react', () => ({
    Activity: ({ className, ...props }: any) => <div data-testid="activity-icon" className={className} {...props} />,
    AlertTriangle: ({ className, ...props }: any) => <div data-testid="alert-triangle-icon" className={className} {...props} />,
    ChevronLeft: ({ className, ...props }: any) => <div data-testid="chevron-left-icon" className={className} {...props} />,
    ChevronRight: ({ className, ...props }: any) => <div data-testid="chevron-right-icon" className={className} {...props} />,
    Clock3: ({ className, ...props }: any) => <div data-testid="clock-icon" className={className} {...props} />,
    ExternalLink: ({ className, ...props }: any) => <div data-testid="external-link-icon" className={className} {...props} />,
    ListChecks: ({ className, ...props }: any) => <div data-testid="list-checks-icon" className={className} {...props} />,
    RefreshCw: ({ className, ...props }: any) => <div data-testid="refresh-cw-icon" className={className} {...props} />,
    RotateCcw: ({ className, ...props }: any) => <div data-testid="rotate-cw-icon" className={className} {...props} />,
    Search: ({ className, ...props }: any) => <div data-testid="search-icon" className={className} {...props} />,
    XCircle: ({ className, ...props }: any) => <div data-testid="x-circle-icon" className={className} {...props} />,
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
    source_job_id: 'gh-1',
    source_is_active: false,
    source_last_seen_at: '2026-06-19T00:00:00Z',
    availability_status: 'source_inactive',
    availability_reason: 'source_sync_absent',
    availability_actions: ['open_posting', 'refresh_availability', 'retire'],
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

const pipelineRun = {
    id: 'run-1',
    task_id: 'process-jobs-1',
    run_type: 'pipeline',
    status: 'completed',
    current_stage: 'embedding',
    queued_count: 10,
    processed_count: 8,
    succeeded_count: 8,
    failed_count: 1,
    skipped_count: 1,
    retry_eligible: false,
    last_error: null,
    owner_id: null,
    tenant_id: null,
    resume_fingerprint: null,
    started_at: '2026-06-20T00:00:00Z',
    completed_at: '2026-06-20T00:03:00Z',
    heartbeat_at: '2026-06-20T00:03:00Z',
    created_at: '2026-06-20T00:00:00Z',
    updated_at: '2026-06-20T00:03:00Z',
    metadata: {},
    allowed_actions: ['requeue', 'retry'],
    stages: [
        {
            id: 'stage-1',
            stage: 'embedding',
            status: 'completed',
            queued_count: 10,
            processed_count: 8,
            succeeded_count: 8,
            failed_count: 1,
            skipped_count: 1,
            retry_count: 0,
            retry_eligible: false,
            last_error: null,
            started_at: '2026-06-20T00:00:00Z',
            completed_at: '2026-06-20T00:03:00Z',
            metadata: {},
        },
    ],
};

function renderPanel(panelStats = stats) {
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    return render(
        <QueryClientProvider client={queryClient}>
            <JobInventoryPanel stats={panelStats} />
        </QueryClientProvider>,
    );
}

describe('JobInventoryPanel', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        mockUseJobs.mockReturnValue({
            data: { success: true, count: 1, total: 1, limit: 50, offset: 0, jobs: [readyJob] },
            isLoading: false,
            error: null,
            refetch: vi.fn(),
        });
        mockProcessJobs.mockResolvedValue({
            data: { success: true, task_id: 'process-jobs-1', message: 'started' },
        });
        mockGetPipelineStatus.mockResolvedValue({
            data: { task_id: 'process-jobs-1', status: 'completed', stats: { jobs_extracted: 1, jobs_embedded: 1 } },
        });
        mockGetPipelineRuns.mockResolvedValue({
            data: { success: true, count: 1, total: 1, limit: 5, offset: 0, runs: [pipelineRun] },
        });
        mockGetPipelineRun.mockResolvedValue({
            data: { success: true, run: pipelineRun },
        });
        mockCancelPipelineRun.mockResolvedValue({
            data: { success: true, action: 'cancel', message: 'cancelled', run: pipelineRun },
        });
        mockRequeuePipelineRun.mockResolvedValue({
            data: {
                success: true,
                action: 'requeue',
                message: 'requeued',
                run: pipelineRun,
                source_run_id: 'run-1',
                enqueued_task_id: 'task-1-requeue',
            },
        });
        mockRetryPipelineRun.mockResolvedValue({
            data: {
                success: true,
                action: 'retry',
                message: 'retried',
                run: pipelineRun,
                source_run_id: 'run-1',
                enqueued_task_id: 'task-1-retry',
            },
        });
        mockGetLlmEvaluationQueueStatus.mockResolvedValue({
            data: {
                success: true,
                ready: true,
                queue: 'llm_evaluations',
                queued: 2,
                started: 1,
                deferred: 0,
                scheduled: 0,
                failed: 0,
                db_pending: 4,
                db_retryable_failed: 1,
                drain_estimate_seconds: 6,
                paused: false,
            },
        });
        mockPauseLlmEvaluationQueue.mockResolvedValue({
            data: {
                success: true,
                action: 'pause_llm_queue',
                message: 'paused',
                enqueued_count: 0,
                status: {
                    success: true,
                    ready: true,
                    queue: 'llm_evaluations',
                    queued: 2,
                    started: 1,
                    deferred: 0,
                    scheduled: 0,
                    failed: 0,
                    paused: true,
                    pause_reason: 'operator pause',
                    pause_ttl_seconds: null,
                },
            },
        });
        mockResumeLlmEvaluationQueue.mockResolvedValue({
            data: {
                success: true,
                action: 'resume_llm_queue',
                message: 'resumed',
                enqueued_count: 0,
                status: {
                    success: true,
                    ready: true,
                    queue: 'llm_evaluations',
                    queued: 2,
                    started: 1,
                    deferred: 0,
                    scheduled: 0,
                    failed: 0,
                    paused: false,
                },
            },
        });
        mockRetryLlmEvaluationQueue.mockResolvedValue({
            data: {
                success: true,
                action: 'retry_llm_queue',
                message: 'queued',
                enqueued_count: 1,
                status: {
                    success: true,
                    ready: true,
                    queue: 'llm_evaluations',
                    queued: 3,
                    started: 1,
                    deferred: 0,
                    scheduled: 0,
                    failed: 0,
                    paused: false,
                },
            },
        });
        mockGetLlmProviderStatus.mockResolvedValue({
            data: {
                success: true,
                count: 1,
                providers: [
                    {
                        name: 'nvidia',
                        provider: 'nvidia',
                        base_url: 'https://integrate.api.nvidia.com/v1',
                        model: 'nvidia-model',
                        structured_output_mode: 'auto',
                        timeout_seconds: 120,
                        max_input_tokens: 8192,
                        requests_per_minute: 40,
                        rate_limit_max_wait_seconds: 120,
                        fallback_on_rate_limit: false,
                        api_key_env: 'NVIDIA_API_KEY',
                        configured: true,
                        circuit_open: false,
                        circuit_retry_after_seconds: null,
                        circuit_failure_count: 0,
                    },
                ],
            },
        });
        mockRunLlmProviderCanaries.mockResolvedValue({
            data: {
                success: true,
                count: 1,
                results: [
                    {
                        name: 'nvidia',
                        provider: 'nvidia',
                        base_url: 'https://integrate.api.nvidia.com/v1',
                        model: 'nvidia-model',
                        structured_output_mode: 'auto',
                        timeout_seconds: 120,
                        max_input_tokens: 8192,
                        requests_per_minute: 40,
                        rate_limit_max_wait_seconds: 120,
                        fallback_on_rate_limit: false,
                        api_key_env: 'NVIDIA_API_KEY',
                        configured: true,
                        circuit_open: false,
                        circuit_retry_after_seconds: null,
                        circuit_failure_count: 0,
                        status: 'succeeded',
                        error_category: null,
                        retryable: false,
                        elapsed_ms: 25,
                    },
                ],
            },
        });
        mockResetLlmProviderCircuit.mockResolvedValue({
            data: {
                success: true,
                provider: 'nvidia',
                model: 'nvidia-model',
                circuit_open: false,
                circuit_retry_after_seconds: null,
                circuit_failure_count: 0,
                deleted_keys: 1,
            },
        });
        mockGetProcessingBlockers.mockResolvedValue({
            data: {
                success: true,
                count: 1,
                blockers: [
                    {
                        job_id: 'job-2',
                        stage: 'extraction',
                        blocker_code: 'retryable_extraction',
                        blocker_detail: 'Extraction failed retryably and is eligible to requeue.',
                        status: 'failed_retryable',
                        attempts: 2,
                        last_error: 'provider timeout',
                        retry_eligible: true,
                        first_seen_at: '2026-06-18T00:00:00Z',
                        last_seen_at: '2026-06-19T00:00:00Z',
                        last_attempt_at: '2026-06-20T00:00:00Z',
                        next_retry_at: null,
                    },
                ],
            },
        });
    });

    it('stays collapsed by default while showing inventory totals', async () => {
        renderPanel();

        expect(screen.getByText('Imported jobs')).toBeInTheDocument();
        expect(screen.getByText('1460')).toBeInTheDocument();
        expect(screen.getByText('1107')).toBeInTheDocument();
        expect(screen.getByRole('button', { name: /browse jobs/i })).toHaveAttribute('aria-expanded', 'false');
        expect(screen.queryByText('Software Engineer')).not.toBeInTheDocument();
        expect(mockUseJobs).toHaveBeenCalledWith(
            expect.objectContaining({ processing_status: 'all', limit: 50, offset: 0 }),
            false,
        );
        await waitFor(() => {
            expect(mockGetPipelineRuns).toHaveBeenCalledWith({ limit: 5, view: 'compact' });
            expect(mockGetProcessingBlockers).toHaveBeenCalledWith({
                stage: 'all',
                limit: 5,
                view: 'compact',
            });
        });
    });

    it('renders imported jobs when opened', () => {
        renderPanel();

        fireEvent.click(screen.getByRole('button', { name: /browse jobs/i }));

        expect(screen.getByRole('button', { name: /hide jobs/i })).toHaveAttribute('aria-expanded', 'true');
        expect(screen.getByText('Software Engineer')).toBeInTheDocument();
        expect(screen.getByText(/Ramp/)).toBeInTheDocument();
        expect(screen.getAllByText('Ready').length).toBeGreaterThan(0);
        expect(screen.getByText('source inactive')).toBeInTheDocument();
        expect(screen.getByText('id gh-1')).toBeInTheDocument();
        expect(screen.getByText('source_inactive')).toBeInTheDocument();
        expect(mockUseJobs).toHaveBeenLastCalledWith(
            expect.objectContaining({ processing_status: 'all', job_status: 'all' }),
            true,
        );
    });

    it('updates processing filters and resets to the first page', () => {
        renderPanel();
        fireEvent.click(screen.getByRole('button', { name: /browse jobs/i }));

        fireEvent.click(screen.getByRole('button', { name: /^Pending extract$/i }));

        expect(mockUseJobs).toHaveBeenLastCalledWith(
            expect.objectContaining({ processing_status: 'pending_extraction', offset: 0 }),
            true,
        );
    });

    it('starts queued job processing from the inventory header', async () => {
        renderPanel();

        fireEvent.click(screen.getByRole('button', { name: /process queued imported jobs/i }));

        await waitFor(() => expect(mockProcessJobs).toHaveBeenCalledTimes(1));
    });

    it('runs an allowed pipeline retry action from the ops panel', async () => {
        renderPanel();

        await waitFor(() => expect(screen.getByRole('button', { name: /retry pipeline run/i })).toBeInTheDocument());
        fireEvent.click(screen.getByRole('button', { name: /retry pipeline run/i }));

        await waitFor(() => expect(mockRetryPipelineRun).toHaveBeenCalledWith('run-1'));
    });

    it('shows LLM queue health in the ops panel', async () => {
        renderPanel();

        await waitFor(() => expect(screen.getByText('LLM queue')).toBeInTheDocument());
        await waitFor(() => expect(mockGetLlmEvaluationQueueStatus).toHaveBeenCalledTimes(1));
        await waitFor(() => expect(screen.getByText('3')).toBeInTheDocument());
        expect(screen.getByText('Active')).toBeInTheDocument();
        expect(screen.getAllByText('Retryable').length).toBeGreaterThan(0);
        expect(screen.getByText((_, element) => element?.textContent === 'nvidia · nvidia-model')).toBeInTheDocument();
    });

    it('runs LLM queue and provider operator actions from the ops panel', async () => {
        renderPanel();

        await waitFor(() => expect(screen.getByRole('button', { name: /pause llm queue/i })).toBeInTheDocument());
        fireEvent.click(screen.getByRole('button', { name: /pause llm queue/i }));
        await waitFor(() => expect(mockPauseLlmEvaluationQueue).toHaveBeenCalledWith('operator pause', 3600));

        fireEvent.click(screen.getByRole('button', { name: /retry pending llm evaluations/i }));
        await waitFor(() => expect(mockRetryLlmEvaluationQueue).toHaveBeenCalledWith(100));

        fireEvent.click(screen.getByRole('button', { name: /run llm provider canary/i }));
        await waitFor(() => expect(mockRunLlmProviderCanaries).toHaveBeenCalledTimes(1));
    });

    it('surfaces degraded LLM queue metadata in the ops panel', async () => {
        mockGetLlmEvaluationQueueStatus.mockResolvedValueOnce({
            data: {
                success: false,
                ready: false,
                queue: 'llm_evaluations',
                queued: 0,
                started: 0,
                deferred: 0,
                scheduled: 0,
                failed: 2,
                error: 'redis unavailable',
            },
        });

        renderPanel();

        await waitFor(() => expect(screen.getByText('Degraded')).toBeInTheDocument());
        expect(screen.getByText('redis unavailable')).toBeInTheDocument();
        expect(screen.getAllByText('Failed').length).toBeGreaterThan(0);
        expect(screen.getAllByText('2').length).toBeGreaterThan(0);
    });
});
