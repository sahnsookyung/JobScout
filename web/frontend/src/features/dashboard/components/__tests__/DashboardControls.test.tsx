import { fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { vi } from 'vitest';
import { toast } from 'sonner';

import { DashboardControls } from '../DashboardControls';
import { usePipeline } from '@/hooks/usePipeline';
import { useStats } from '@/hooks/useStats';
import { pipelineApi } from '@/services/pipelineApi';

vi.mock('@/hooks/usePipeline');
vi.mock('@/hooks/useStats');
vi.mock('@/services/pipelineApi', () => ({
        pipelineApi: {
            getSources: vi.fn(),
            fetchSource: vi.fn(),
            getCloudIntegrations: vi.fn(),
            getUserAtsSources: vi.fn(),
            getUserAtsSourceHistory: vi.fn(),
            discoverAtsSources: vi.fn(),
            createUserAtsSource: vi.fn(),
            updateUserAtsSource: vi.fn(),
            deleteUserAtsSource: vi.fn(),
            syncUserAtsSource: vi.fn(),
        },
}));
vi.mock('sonner');
vi.mock('@shared/constants', () => ({
    RESUME_MAX_SIZE_MB: 2,
    RESUME_MAX_SIZE: 2 * 1024 * 1024,
    RESUME_INDEXEDDB_NAME: 'jobscout-resume',
    RESUME_MAX_AGE_DAYS: 30,
}));

vi.mock('@/utils/indexedDB', () => ({
    getResumeFilename: vi.fn().mockResolvedValue(null),
}));

const mockUsePipeline = usePipeline as ReturnType<typeof vi.fn>;
const mockUseStats = useStats as ReturnType<typeof vi.fn>;
const mockPipelineApi = pipelineApi as unknown as {
    getSources: ReturnType<typeof vi.fn>;
    fetchSource: ReturnType<typeof vi.fn>;
    getCloudIntegrations: ReturnType<typeof vi.fn>;
    getUserAtsSources: ReturnType<typeof vi.fn>;
    getUserAtsSourceHistory: ReturnType<typeof vi.fn>;
    discoverAtsSources: ReturnType<typeof vi.fn>;
    createUserAtsSource: ReturnType<typeof vi.fn>;
    updateUserAtsSource: ReturnType<typeof vi.fn>;
    deleteUserAtsSource: ReturnType<typeof vi.fn>;
    syncUserAtsSource: ReturnType<typeof vi.fn>;
};

const createWrapper = () => {
    const queryClient = new QueryClient({
        defaultOptions: { queries: { retry: false } },
    });
    return ({ children }: { children: React.ReactNode }) => (
        <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );
};

describe('DashboardControls', () => {
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
            isPreparingResume: false,
            resumeProcessingStep: undefined,
        });

        mockUseStats.mockReturnValue({ data: null });

        mockPipelineApi.getSources.mockResolvedValue({
            data: {
                success: true,
                jobspy_url: 'https://jobspy.example',
                api_based_fetching: true,
                search_query: null,
                total_count: 3,
                filtered_count: 3,
                seed_websites: ['https://www.tokyodev.com/jobs'],
                sources: [
                    {
                        site_type: 'tokyodev',
                        display_name: 'TokyoDev',
                        seed_url: 'https://www.tokyodev.com/jobs',
                        description: 'English-friendly software roles in Japan.',
                        tags: ['japan', 'startup'],
                        search_keywords: ['tokyodev', 'japan', 'startup'],
                        fetch_mode: 'seed_website',
                        provider_name: 'Worker seed fetcher',
                        search_term: '',
                        location: null,
                        country: null,
                        results_wanted: 5,
                        hours_old: null,
                        options: { seniorities: ['junior'] },
                        api_health: null,
                        external_fetch_status: {
                            enabled: true,
                            configured: true,
                            status: 'configured',
                            provider: 'cloudflare_worker_seed',
                            last_attempt_at: null,
                            last_success_at: null,
                            next_eligible_at: null,
                            failure_class: null,
                            budget_remaining: 42,
                        },
                    },
                    {
                        site_type: 'indeed',
                        display_name: 'Indeed',
                        seed_url: 'https://www.indeed.com',
                        description: 'Broad job-board search through JobSpy.',
                        tags: ['job board'],
                        search_keywords: ['indeed', 'platform engineer'],
                        fetch_mode: 'jobspy_api',
                        provider_name: 'JobSpy',
                        search_term: 'platform engineer',
                        location: null,
                        country: null,
                        results_wanted: 3,
                        hours_old: null,
                        options: {},
                        api_health: {
                            available: true,
                            status: 'available',
                            endpoint: 'https://jobspy.example/health',
                            status_code: 200,
                            response_time_ms: 10,
                            error: null,
                        },
                    },
                    {
                        site_type: 'internal_feed',
                        display_name: 'Internal Feed',
                        seed_url: null,
                        description: 'Private source.',
                        tags: ['internal'],
                        search_keywords: ['internal', 'platform engineer'],
                        fetch_mode: 'custom_source',
                        provider_name: 'Custom source',
                        search_term: 'platform engineer',
                        location: null,
                        country: null,
                        results_wanted: 3,
                        hours_old: null,
                        options: {},
                        api_health: null,
                    },
                ],
            },
        });
        mockPipelineApi.getCloudIntegrations.mockResolvedValue({
            status: 200,
            data: [
                {
                    id: 'integration-1',
                    tenant_id: 'tenant-1',
                    provider: 'greenhouse',
                    display_name: 'HubSpot',
                    status: 'active',
                    sync_interval_minutes: 120,
                    config: {},
                    capabilities: ['list_jobs'],
                    validation_status: 'pending',
                    last_validated_at: null,
                    last_error: null,
                },
            ],
        });
        mockPipelineApi.getUserAtsSources.mockResolvedValue({
            status: 200,
            data: [],
        });
        mockPipelineApi.getUserAtsSourceHistory.mockResolvedValue({
            status: 200,
            data: [],
        });
        mockPipelineApi.discoverAtsSources.mockResolvedValue({
            data: [
                {
                    provider: 'lever',
                    identifier: 'acme',
                    config_key: 'site_identifier',
                    config: { site_identifier: 'acme', company_name: 'Acme Lever' },
                    display_name: 'Acme Lever',
                    source_url: 'https://jobs.lever.co/acme',
                    jobs_seen: 4,
                    match_reason: 'public ATS board returned active jobs',
                },
            ],
        });
        mockPipelineApi.createUserAtsSource.mockResolvedValue({
            data: {
                id: 'source-1',
                tenant_id: 'tenant-1',
                provider: 'lever',
                display_name: 'Acme Lever',
                status: 'active',
                sync_interval_minutes: 120,
                config: {},
                capabilities: ['list_jobs'],
                validation_status: 'pending',
                last_validated_at: null,
                last_error: null,
                is_user_source: true,
                owner_user_id: 'user-1',
                source_url: 'https://jobs.lever.co/acme',
                created_at: null,
                updated_at: null,
            },
        });
        mockPipelineApi.updateUserAtsSource.mockResolvedValue({
            data: {
                id: 'source-1',
                tenant_id: 'tenant-1',
                provider: 'lever',
                display_name: 'Acme Lever',
                status: 'disabled',
                sync_interval_minutes: 120,
                config: {},
                capabilities: ['list_jobs'],
                validation_status: 'pending',
                last_validated_at: null,
                last_error: null,
                is_user_source: true,
                owner_user_id: 'user-1',
                source_url: 'https://jobs.lever.co/acme',
                created_at: null,
                updated_at: null,
            },
        });
        mockPipelineApi.deleteUserAtsSource.mockResolvedValue({});
        mockPipelineApi.syncUserAtsSource.mockResolvedValue({
            data: {
                run_id: 'run-1',
                status: 'completed',
                jobs_seen: 4,
                jobs_imported: 2,
                jobs_deactivated: 0,
                provider: 'lever',
                dedupe_fingerprint_count: 2,
            },
        });
        mockPipelineApi.fetchSource.mockResolvedValue({
            data: {
                success: true,
                source: 'tokyodev',
                status: 'ok',
                fetched_count: 2,
                imported_count: 2,
                skipped_count: 0,
                warnings: [],
                next_eligible_at: null,
                failure_class: null,
                budget_remaining: 8,
            },
        });

        mockUploadResume.mockResolvedValue({
            alreadyExists: false,
            message: 'Resume uploaded successfully',
        });
    });

    describe('resume upload', () => {
        it('shows "Upload resume" when no file is present', () => {
            render(<DashboardControls />, { wrapper: createWrapper() });
            expect(screen.getByText('Upload resume')).toBeInTheDocument();
        });

        it('shows "Replace resume" and the filename after a successful upload', async () => {
            render(<DashboardControls />, { wrapper: createWrapper() });

            const fileInput = screen.getByTestId('resume-file-input');
            const file = new File([JSON.stringify({ name: 'Test' })], 'my-resume.json', {
                type: 'application/json',
            });

            await userEvent.upload(fileInput, file);

            await waitFor(() => {
                expect(screen.getByText('Replace resume')).toBeInTheDocument();
            });

            expect(screen.getByText('my-resume.json')).toBeInTheDocument();
            expect(toast.success).toHaveBeenCalledWith('Resume uploaded successfully');
        });

        it('shows upload failures with the new toast copy', async () => {
            mockUploadResume.mockRejectedValue(new Error('Network error'));

            render(<DashboardControls />, { wrapper: createWrapper() });

            const fileInput = screen.getByTestId('resume-file-input');
            const file = new File([JSON.stringify({ name: 'Test' })], 'resume.json', {
                type: 'application/json',
            });

            await userEvent.upload(fileInput, file);

            await waitFor(() => {
                expect(toast.error).toHaveBeenCalledWith('Resume upload failed: Network error');
            });
        });

        it('disables the upload affordance while uploading or running', () => {
            mockUsePipeline.mockReturnValue({
                runPipeline: mockRunPipeline,
                stopPipeline: mockStopPipeline,
                isRunning: false,
                isStopping: false,
                status: null,
                uploadResume: mockUploadResume,
                isUploading: true,
                isPreparingResume: false,
                resumeProcessingStep: undefined,
            });

            const { rerender } = render(<DashboardControls />, { wrapper: createWrapper() });
            expect(screen.getByText('Uploading').closest('button')).toBeDisabled();

            mockUsePipeline.mockReturnValue({
                runPipeline: mockRunPipeline,
                stopPipeline: mockStopPipeline,
                isRunning: true,
                isStopping: false,
                status: { status: 'running', step: 'loading_resume' },
                uploadResume: mockUploadResume,
                isUploading: false,
                isPreparingResume: false,
                resumeProcessingStep: undefined,
            });

            rerender(<DashboardControls />);
            expect(screen.getByText('Upload resume').closest('button')).toBeDisabled();
        });

        it('accepts supported file formats and forwards the uploaded file', async () => {
            render(<DashboardControls />, { wrapper: createWrapper() });

            const file = new File([JSON.stringify({ name: 'Test' })], 'resume.json', {
                type: 'application/json',
            });
            const fileInput = screen.getByTestId('resume-file-input');

            expect(fileInput).toHaveAttribute('accept', '.json,.yaml,.yml,.txt,.docx,.pdf');
            await userEvent.upload(fileInput, file);

            await waitFor(() => {
                expect(mockUploadResume).toHaveBeenCalledWith(file);
            });
        });

        it('blocks oversized files with the updated size warning', async () => {
            const file = new File(['x'], 'big.pdf', { type: 'application/pdf' });
            Object.defineProperty(file, 'size', { value: 3 * 1024 * 1024 });

            render(<DashboardControls />, { wrapper: createWrapper() });
            await userEvent.upload(screen.getByTestId('resume-file-input'), file);

            await waitFor(() => {
                expect(toast.error).toHaveBeenCalledWith('That file is over 2MB. Try a smaller one.');
            });

            expect(mockUploadResume).not.toHaveBeenCalled();
        });

        it('shows the saved-resume toast when the upload already exists', async () => {
            mockUploadResume.mockResolvedValue({
                alreadyExists: true,
                message: 'An identical resume has already been uploaded.',
            });

            render(<DashboardControls />, { wrapper: createWrapper() });
            const file = new File(['content'], 'existing-resume.json', { type: 'application/json' });
            await userEvent.upload(screen.getByTestId('resume-file-input'), file);

            await waitFor(() => {
                expect(toast.success).toHaveBeenCalledWith('This resume is already saved.');
            });
        });

        it('disables the run button while the resume is being prepared', () => {
            mockUsePipeline.mockReturnValue({
                runPipeline: mockRunPipeline,
                stopPipeline: mockStopPipeline,
                isRunning: false,
                isStopping: false,
                status: null,
                uploadResume: mockUploadResume,
                isUploading: false,
                isPreparingResume: true,
                resumeProcessingStep: undefined,
            });

            render(<DashboardControls />, { wrapper: createWrapper() });
            expect(screen.getByText('Preparing').closest('button')).toBeDisabled();
        });
    });

    describe('run matching', () => {
        it('calls runPipeline with an error callback when clicked', async () => {
            render(<DashboardControls />, { wrapper: createWrapper() });
            await userEvent.click(screen.getByText('Run matching'));

            await waitFor(() => {
                expect(mockRunPipeline).toHaveBeenCalledWith(expect.any(Function));
            });
        });

        it('surfaces runPipeline errors via toast', async () => {
            mockRunPipeline.mockImplementation((onError?: (msg: string) => void) => {
                onError?.('No resume found in browser storage. Please upload a resume first.');
            });

            render(<DashboardControls />, { wrapper: createWrapper() });
            await userEvent.click(screen.getByText('Run matching'));

            await waitFor(() => {
                expect(toast.error).toHaveBeenCalledWith(
                    'No resume found in browser storage. Please upload a resume first.'
                );
            });
        });
    });

    describe('mount behavior', () => {
        it('loads and displays an existing resume filename from IndexedDB', async () => {
            const { getResumeFilename } = await import('@/utils/indexedDB');
            (getResumeFilename as any).mockResolvedValue('saved-resume.pdf');

            render(<DashboardControls />, { wrapper: createWrapper() });

            await waitFor(() => {
                expect(screen.getByText('Replace resume')).toBeInTheDocument();
            });
        });

        it('stays on "Upload resume" when no filename is stored', async () => {
            const { getResumeFilename } = await import('@/utils/indexedDB');
            (getResumeFilename as any).mockResolvedValue(null);

            render(<DashboardControls />, { wrapper: createWrapper() });

            await waitFor(() => {
                expect(screen.getByText('Upload resume')).toBeInTheDocument();
            });
        });

        it('handles IndexedDB errors without crashing', async () => {
            const { getResumeFilename } = await import('@/utils/indexedDB');
            (getResumeFilename as any).mockRejectedValue(new Error('IndexedDB unavailable'));

            expect(() => render(<DashboardControls />, { wrapper: createWrapper() })).not.toThrow();
        });
    });

    describe('stats and status', () => {
        it('renders non-zero stats and the idle action label', () => {
            mockUseStats.mockReturnValue({
                data: {
                    total_matches: 100,
                    active_matches: 60,
                    hidden_count: 20,
                    below_threshold_count: 20,
                },
            });

            render(<DashboardControls />, { wrapper: createWrapper() });
            expect(screen.getByText('100')).toBeInTheDocument();
            expect(screen.getByText('Run matching')).toBeInTheDocument();
        });

        it('switches the action button to stop while the pipeline is running', () => {
            mockUsePipeline.mockReturnValue({
                runPipeline: mockRunPipeline,
                stopPipeline: mockStopPipeline,
                isRunning: true,
                isStopping: false,
                status: { status: 'running', step: 'matching' },
                uploadResume: mockUploadResume,
                isUploading: false,
                isPreparingResume: false,
                resumeProcessingStep: undefined,
            });

            render(<DashboardControls />, { wrapper: createWrapper() });
            expect(screen.getByText('Stop')).toBeInTheDocument();
        });

        it('renders the updated pending status copy', () => {
            mockUsePipeline.mockReturnValue({
                runPipeline: mockRunPipeline,
                stopPipeline: mockStopPipeline,
                isRunning: true,
                isStopping: false,
                status: { task_id: 'task-1', status: 'pending', step: 'initializing' },
                uploadResume: mockUploadResume,
                isUploading: false,
                isPreparingResume: false,
                resumeProcessingStep: undefined,
            });

            render(<DashboardControls />, { wrapper: createWrapper() });
            expect(screen.getByText('Active')).toBeInTheDocument();
            expect(screen.getByText('Starting up')).toBeInTheDocument();
        });

        it('renders the completed state alongside the idle action label', () => {
            mockUsePipeline.mockReturnValue({
                runPipeline: mockRunPipeline,
                stopPipeline: mockStopPipeline,
                isRunning: false,
                isStopping: false,
                status: { status: 'completed', step: 'done' },
                uploadResume: mockUploadResume,
                isUploading: false,
                isPreparingResume: false,
                resumeProcessingStep: undefined,
            });

            render(<DashboardControls />, { wrapper: createWrapper() });
            expect(screen.getByText('Complete')).toBeInTheDocument();
            expect(screen.getByText('Run matching')).toBeInTheDocument();
        });

        it('renders configured fetch sources', async () => {
            render(<DashboardControls />, { wrapper: createWrapper() });

            await waitFor(() => {
                expect(screen.getByText('TokyoDev')).toBeInTheDocument();
            });
            expect(screen.getByText('JobSpy + ATS')).toBeInTheDocument();
            expect(screen.getByText('Worker seed fetcher')).toBeInTheDocument();
            expect(screen.getByText('Worker ready')).toBeInTheDocument();
            expect(screen.getByText('JobSpy online')).toBeInTheDocument();
            expect(screen.getByText('Greenhouse ATS')).toBeInTheDocument();
            expect(screen.getByText('HubSpot')).toBeInTheDocument();
            expect(mockPipelineApi.getSources).toHaveBeenCalledWith({
                includeStatus: true,
            });
            expect(mockPipelineApi.getCloudIntegrations).toHaveBeenCalledTimes(1);
            expect(mockPipelineApi.getUserAtsSources).toHaveBeenCalledTimes(1);
        });

        it('lets admins trigger a Worker-backed seed fetch from the source card', async () => {
            render(<DashboardControls />, { wrapper: createWrapper() });

            await waitFor(() => {
                expect(screen.getByText('TokyoDev')).toBeInTheDocument();
            });

            await userEvent.click(screen.getByRole('button', { name: /fetch/i }));

            expect(mockPipelineApi.fetchSource).toHaveBeenCalledWith('tokyodev');
            await waitFor(() => {
                expect(toast.success).toHaveBeenCalledWith('2 jobs imported from Tokyodev');
            });
        });

        it('does not render private or missing source URLs as empty links', async () => {
            render(<DashboardControls />, { wrapper: createWrapper() });

            await waitFor(() => {
                expect(screen.getByText('Internal Feed')).toBeInTheDocument();
            });
            expect(screen.getByText('Internal Feed').closest('a')).toBeNull();
        });

        it('filters source search locally without refetching API status', async () => {
            render(<DashboardControls />, { wrapper: createWrapper() });

            await waitFor(() => {
                expect(screen.getByText('TokyoDev')).toBeInTheDocument();
            });

            await userEvent.type(screen.getByLabelText('Search sources'), 'internal');

            await waitFor(() => {
                expect(screen.queryByText('TokyoDev')).not.toBeInTheDocument();
            });
            expect(screen.getByText('Internal Feed')).toBeInTheDocument();
            expect(mockPipelineApi.getSources).toHaveBeenCalledTimes(1);
            expect(mockPipelineApi.getSources).not.toHaveBeenCalledWith(
                expect.objectContaining({ search: 'internal' })
            );
        });

        it('filters source views and renders an empty filtered state', async () => {
            render(<DashboardControls />, { wrapper: createWrapper() });

            await waitFor(() => {
                expect(screen.getByText('TokyoDev')).toBeInTheDocument();
            });

            await userEvent.click(screen.getByRole('button', { name: /API/i }));
            expect(screen.getByText('Indeed')).toBeInTheDocument();
            expect(screen.queryByText('TokyoDev')).not.toBeInTheDocument();

            await userEvent.click(screen.getByRole('button', { name: /Seed sites/i }));
            expect(screen.getByText('TokyoDev')).toBeInTheDocument();
            expect(screen.queryByText('Indeed')).not.toBeInTheDocument();

            await userEvent.click(screen.getByRole('button', { name: /ATS boards/i }));
            expect(screen.getByText('HubSpot')).toBeInTheDocument();
            expect(screen.queryByText('TokyoDev')).not.toBeInTheDocument();

            await userEvent.click(screen.getByRole('button', { name: /Paused/i }));
            expect(screen.getByText('No sources in this view.')).toBeInTheDocument();
        });

        it('renders source loading placeholders while catalog data is pending', () => {
            mockPipelineApi.getSources.mockReturnValue(new Promise(() => undefined));

            render(<DashboardControls />, { wrapper: createWrapper() });

            expect(document.querySelectorAll('.animate-pulse').length).toBeGreaterThan(0);
        });

        it('surfaces source status load errors and retries all source queries', async () => {
            mockPipelineApi.getSources.mockRejectedValueOnce(new Error('catalog offline'));
            mockPipelineApi.getCloudIntegrations.mockRejectedValueOnce(new Error('tenant offline'));
            mockPipelineApi.getUserAtsSources.mockRejectedValueOnce(new Error('user offline'));
            mockPipelineApi.getUserAtsSourceHistory.mockRejectedValueOnce(new Error('history offline'));

            render(<DashboardControls />, { wrapper: createWrapper() });

            const alert = await screen.findByRole('alert');
            expect(within(alert).getByText('Catalog: catalog offline')).toBeInTheDocument();
            expect(within(alert).getByText('Tenant ATS sources: tenant offline')).toBeInTheDocument();
            expect(within(alert).getByText('Your ATS sources: user offline')).toBeInTheDocument();
            expect(within(alert).getByText('Activity: history offline')).toBeInTheDocument();

            await userEvent.click(within(alert).getByRole('button', { name: /retry/i }));

            await waitFor(() => {
                expect(mockPipelineApi.getSources).toHaveBeenCalledTimes(2);
            });
            expect(mockPipelineApi.getCloudIntegrations).toHaveBeenCalledTimes(2);
            expect(mockPipelineApi.getUserAtsSources).toHaveBeenCalledTimes(2);
            expect(mockPipelineApi.getUserAtsSourceHistory).toHaveBeenCalledTimes(2);
        });

        it('lets users add their own ATS source from the fetch panel', async () => {
            render(<DashboardControls />, { wrapper: createWrapper() });

            await waitFor(() => {
                expect(screen.getByText('TokyoDev')).toBeInTheDocument();
            });

            await userEvent.click(screen.getByRole('button', { name: /add source/i }));
            await userEvent.type(screen.getByLabelText('Name'), 'Acme Lever');
            await userEvent.type(screen.getByLabelText('Careers URL'), 'https://jobs.lever.co/acme');
            await userEvent.selectOptions(screen.getByLabelText('Provider'), 'lever');
            await userEvent.click(screen.getByRole('button', { name: /^add$/i }));

            await waitFor(() => {
                expect(mockPipelineApi.createUserAtsSource).toHaveBeenCalledWith({
                    display_name: 'Acme Lever',
                    source_url: 'https://jobs.lever.co/acme',
                    provider: 'lever',
                    providers: ['lever'],
                    identifier: undefined,
                });
            });
            expect(toast.success).toHaveBeenCalledWith('Acme Lever added');
            expect(mockPipelineApi.syncUserAtsSource).not.toHaveBeenCalled();
        });

        it('shows initial sync completion and warning outcomes after source creation', async () => {
            mockPipelineApi.createUserAtsSource
                .mockResolvedValueOnce({
                    data: {
                        id: 'source-sync-ok',
                        tenant_id: 'tenant-1',
                        provider: 'lever',
                        display_name: 'Synced Lever',
                        status: 'active',
                        sync_interval_minutes: 120,
                        config: {},
                        capabilities: ['list_jobs'],
                        validation_status: 'pending',
                        last_validated_at: null,
                        last_error: null,
                        is_user_source: true,
                        owner_user_id: 'user-1',
                        source_url: 'https://jobs.lever.co/synced',
                        created_at: null,
                        updated_at: null,
                        initial_sync: {
                            status: 'completed',
                            jobs_seen: 4,
                            jobs_imported: 3,
                            jobs_deactivated: 0,
                            provider: 'lever',
                        },
                    },
                })
                .mockResolvedValueOnce({
                    data: {
                        id: 'source-sync-warn',
                        tenant_id: 'tenant-1',
                        provider: 'ashby',
                        display_name: 'Warned Ashby',
                        status: 'active',
                        sync_interval_minutes: 120,
                        config: {},
                        capabilities: ['list_jobs'],
                        validation_status: 'pending',
                        last_validated_at: null,
                        last_error: null,
                        is_user_source: true,
                        owner_user_id: 'user-1',
                        source_url: 'https://jobs.ashbyhq.com/warned',
                        created_at: null,
                        updated_at: null,
                        initial_sync: {
                            status: 'failed',
                            jobs_seen: 0,
                            jobs_imported: 0,
                            jobs_deactivated: 0,
                            provider: 'ashby',
                            error_summary: 'budget skipped',
                        },
                    },
                });

            render(<DashboardControls />, { wrapper: createWrapper() });

            await waitFor(() => {
                expect(screen.getByText('TokyoDev')).toBeInTheDocument();
            });

            await userEvent.click(screen.getByRole('button', { name: /add source/i }));
            await userEvent.type(screen.getByLabelText('Name'), 'Synced Lever');
            await userEvent.click(screen.getByRole('button', { name: /^add$/i }));
            await waitFor(() => {
                expect(toast.success).toHaveBeenCalledWith('3 jobs imported from Lever');
            });

            await userEvent.click(screen.getByRole('button', { name: /add source/i }));
            await userEvent.type(screen.getByLabelText('Name'), 'Warned Ashby');
            await userEvent.click(screen.getByRole('button', { name: /^add$/i }));
            await waitFor(() => {
                expect(toast).toHaveBeenCalledWith('Initial sync failed: budget skipped');
            });
        });

        it('lets users add a provider board identifier without a careers URL', async () => {
            render(<DashboardControls />, { wrapper: createWrapper() });

            await waitFor(() => {
                expect(screen.getByText('TokyoDev')).toBeInTheDocument();
            });

            await userEvent.click(screen.getByRole('button', { name: /add source/i }));
            await userEvent.selectOptions(screen.getByLabelText('Provider'), 'ashby');
            await userEvent.type(screen.getByLabelText('Board ID'), 'acme-board');
            await userEvent.click(screen.getByRole('button', { name: /^add$/i }));

            await waitFor(() => {
                expect(mockPipelineApi.createUserAtsSource).toHaveBeenCalledWith({
                    display_name: undefined,
                    source_url: undefined,
                    provider: 'ashby',
                    providers: undefined,
                    identifier: 'acme-board',
                });
            });
        });

        it('lets users check ATS discovery candidates before adding a source', async () => {
            render(<DashboardControls />, { wrapper: createWrapper() });

            await waitFor(() => {
                expect(screen.getByText('TokyoDev')).toBeInTheDocument();
            });

            await userEvent.click(screen.getByRole('button', { name: /add source/i }));
            await userEvent.type(screen.getByLabelText('Name'), 'Acme');
            await userEvent.click(screen.getByRole('button', { name: /check/i }));

            await waitFor(() => {
                expect(mockPipelineApi.discoverAtsSources).toHaveBeenCalledWith({
                    display_name: 'Acme',
                    source_url: undefined,
                    provider: undefined,
                    providers: undefined,
                    identifier: undefined,
                });
            });
            expect(screen.getByText('4 jobs')).toBeInTheDocument();

            const discoveryResult = screen.getByText('acme').closest('div[class*="border"]') as HTMLElement;
            await userEvent.click(within(discoveryResult).getByRole('button', { name: /^add$/i }));

            await waitFor(() => {
                expect(mockPipelineApi.createUserAtsSource).toHaveBeenCalledWith({
                    display_name: 'Acme',
                    source_url: 'https://jobs.lever.co/acme',
                    provider: 'lever',
                    identifier: 'acme',
                });
            });
        });

        it('shows discovery empty and error states without creating a source', async () => {
            mockPipelineApi.discoverAtsSources
                .mockResolvedValueOnce({ data: [] })
                .mockRejectedValueOnce(new Error('discovery offline'));

            render(<DashboardControls />, { wrapper: createWrapper() });

            await waitFor(() => {
                expect(screen.getByText('TokyoDev')).toBeInTheDocument();
            });

            await userEvent.click(screen.getByRole('button', { name: /add source/i }));
            await userEvent.type(screen.getByLabelText('Name'), 'Missing Board');
            await userEvent.click(screen.getByRole('button', { name: /check/i }));

            await waitFor(() => {
                expect(toast.error).toHaveBeenCalledWith('No supported ATS board found.');
            });
            expect(screen.getByText(/No supported ATS board matched/)).toBeInTheDocument();

            await userEvent.type(screen.getByLabelText('Board ID'), 'retry');
            await userEvent.click(screen.getByRole('button', { name: /check/i }));

            await waitFor(() => {
                expect(toast.error).toHaveBeenCalledWith('ATS source check failed: discovery offline');
            });
            expect(screen.getByText('discovery offline')).toBeInTheDocument();
            expect(mockPipelineApi.createUserAtsSource).not.toHaveBeenCalled();
        });

        it('clears discovery candidates when source inputs change', async () => {
            render(<DashboardControls />, { wrapper: createWrapper() });

            await waitFor(() => {
                expect(screen.getByText('TokyoDev')).toBeInTheDocument();
            });

            await userEvent.click(screen.getByRole('button', { name: /add source/i }));
            await userEvent.type(screen.getByLabelText('Name'), 'Acme');
            await userEvent.click(screen.getByRole('button', { name: /check/i }));

            await waitFor(() => {
                expect(screen.getByText('4 jobs')).toBeInTheDocument();
            });

            await userEvent.type(screen.getByLabelText('Board ID'), 'changed');

            expect(screen.queryByText('4 jobs')).not.toBeInTheDocument();
        });

        it('lets users search by board identifier alone across supported ATS providers', async () => {
            render(<DashboardControls />, { wrapper: createWrapper() });

            await waitFor(() => {
                expect(screen.getByText('TokyoDev')).toBeInTheDocument();
            });

            await userEvent.click(screen.getByRole('button', { name: /add source/i }));
            await userEvent.type(screen.getByLabelText('Board ID'), 'acme');
            await userEvent.click(screen.getByRole('button', { name: /^add$/i }));

            await waitFor(() => {
                expect(mockPipelineApi.createUserAtsSource).toHaveBeenCalledWith({
                    display_name: undefined,
                    source_url: undefined,
                    provider: undefined,
                    providers: undefined,
                    identifier: 'acme',
                });
            });
        });

        it('validates empty user ATS source submissions before calling the API', async () => {
            render(<DashboardControls />, { wrapper: createWrapper() });

            await waitFor(() => {
                expect(screen.getByText('TokyoDev')).toBeInTheDocument();
            });

            await userEvent.click(screen.getByRole('button', { name: /add source/i }));
            await userEvent.click(screen.getByRole('button', { name: /^add$/i }));

            expect(toast.error).toHaveBeenCalledWith(
                'Add a source name, careers URL, or board identifier.'
            );
            expect(mockPipelineApi.createUserAtsSource).not.toHaveBeenCalled();
        });

        it('shows source history and can re-add a deleted source snapshot', async () => {
            mockPipelineApi.getUserAtsSourceHistory.mockResolvedValue({
                status: 200,
                data: [
                    {
                        id: 'history-1',
                        action: 'integration.user_source_deleted',
                        resource_id: 'source-old',
                        provider: 'greenhouse',
                        display_name: 'Deleted Acme',
                        identifier: 'acme',
                        source_url: 'https://boards.greenhouse.io/acme',
                        status: 'active',
                        occurred_at: '2026-05-24T00:00:00Z',
                        readd_payload: {
                            display_name: 'Deleted Acme',
                            provider: 'greenhouse',
                            identifier: 'acme',
                            source_url: 'https://boards.greenhouse.io/acme',
                            sync_interval_minutes: 120,
                            status: 'active',
                        },
                    },
                    {
                        id: 'history-2',
                        action: 'integration.user_source_updated',
                        resource_id: 'source-updated',
                        provider: 'lever',
                        display_name: 'Updated Beta',
                        identifier: 'beta',
                        source_url: 'https://jobs.lever.co/beta',
                        status: 'active',
                        occurred_at: '2026-05-23T00:00:00Z',
                        readd_payload: null,
                    },
                ],
            });

            render(<DashboardControls />, { wrapper: createWrapper() });

            const activityButton = screen.getByRole('button', { name: /activity/i });
            expect(activityButton).toHaveAttribute('aria-expanded', 'false');
            await userEvent.click(activityButton);
            expect(activityButton).toHaveAttribute('aria-expanded', 'true');

            await waitFor(() => {
                expect(screen.getByText('Deleted Acme')).toBeInTheDocument();
            });
            expect(screen.getByText('Updated Beta')).toBeInTheDocument();

            await userEvent.click(screen.getByRole('button', { name: /deleted/i }));
            expect(screen.getByText('Deleted Acme')).toBeInTheDocument();
            expect(screen.queryByText('Updated Beta')).not.toBeInTheDocument();

            await userEvent.click(screen.getByRole('button', { name: /recoverable/i }));
            expect(screen.getByText('Deleted Acme')).toBeInTheDocument();
            expect(screen.queryByText('Updated Beta')).not.toBeInTheDocument();

            await userEvent.click(screen.getByRole('button', { name: /re-add/i }));

            await waitFor(() => {
                expect(mockPipelineApi.createUserAtsSource).toHaveBeenCalledWith({
                    display_name: 'Deleted Acme',
                    provider: 'greenhouse',
                    identifier: 'acme',
                    source_url: 'https://boards.greenhouse.io/acme',
                    sync_interval_minutes: 120,
                    status: 'active',
                });
            });
        });

        it('rejects unsupported careers URLs before creating a user ATS source', async () => {
            render(<DashboardControls />, { wrapper: createWrapper() });

            await waitFor(() => {
                expect(screen.getByText('TokyoDev')).toBeInTheDocument();
            });

            await userEvent.click(screen.getByRole('button', { name: /add source/i }));
            await userEvent.type(screen.getByLabelText('Name'), 'Acme Careers');
            await userEvent.type(screen.getByLabelText('Careers URL'), 'https://careers.example.com/acme');
            await userEvent.click(screen.getByRole('button', { name: /^add$/i }));

            expect(toast.error).toHaveBeenCalledWith('Use a Greenhouse, Lever, or Ashby board URL.');
            expect(mockPipelineApi.createUserAtsSource).not.toHaveBeenCalled();
        });

        it('renders seed, JobSpy, custom, and degraded source status labels', async () => {
            mockPipelineApi.getCloudIntegrations.mockResolvedValue({ status: 200, data: [] });
            mockPipelineApi.getUserAtsSources.mockResolvedValue({ status: 200, data: [] });
            mockPipelineApi.getSources.mockResolvedValue({
                data: {
                    success: true,
                    jobspy_url: null,
                    api_based_fetching: false,
                    search_query: null,
                    total_count: 8,
                    filtered_count: 8,
                    seed_websites: [],
                    sources: [
                        {
                            site_type: 'rate_limited_seed',
                            display_name: 'Rate Limited Seed',
                            seed_url: 'https://example.com/jobs',
                            description: null,
                            tags: [],
                            search_keywords: ['rate'],
                            fetch_mode: 'seed_website',
                            provider_name: null,
                            search_term: null,
                            location: null,
                            country: null,
                            results_wanted: 1,
                            hours_old: null,
                            options: {},
                            api_health: null,
                            external_fetch_status: { enabled: true, configured: true, status: 'rate_limited' },
                        },
                        {
                            site_type: 'degraded_seed',
                            display_name: 'Degraded Seed',
                            seed_url: null,
                            description: null,
                            tags: [],
                            search_keywords: ['degraded'],
                            fetch_mode: 'seed_website',
                            provider_name: null,
                            search_term: null,
                            location: null,
                            country: null,
                            results_wanted: 1,
                            hours_old: null,
                            options: {},
                            api_health: null,
                            external_fetch_status: { enabled: true, configured: true, status: 'degraded' },
                        },
                        {
                            site_type: 'disabled_seed',
                            display_name: 'Disabled Seed',
                            seed_url: null,
                            description: null,
                            tags: [],
                            search_keywords: ['disabled'],
                            fetch_mode: 'seed_website',
                            provider_name: null,
                            search_term: null,
                            location: null,
                            country: null,
                            results_wanted: 1,
                            hours_old: null,
                            options: {},
                            api_health: null,
                            external_fetch_status: { enabled: false, configured: true, status: 'disabled' },
                        },
                        {
                            site_type: 'unconfigured_seed',
                            display_name: 'Unconfigured Seed',
                            seed_url: null,
                            description: null,
                            tags: [],
                            search_keywords: ['unconfigured'],
                            fetch_mode: 'seed_website',
                            provider_name: null,
                            search_term: null,
                            location: null,
                            country: null,
                            results_wanted: 1,
                            hours_old: null,
                            options: {},
                            api_health: null,
                            external_fetch_status: { enabled: false, configured: false, status: 'not_configured' },
                        },
                        {
                            site_type: 'jobspy_missing',
                            display_name: 'JobSpy Missing',
                            seed_url: null,
                            description: null,
                            tags: [],
                            search_keywords: ['jobspy'],
                            fetch_mode: 'jobspy_api',
                            provider_name: null,
                            search_term: null,
                            location: null,
                            country: null,
                            results_wanted: 1,
                            hours_old: null,
                            options: {},
                            api_health: { available: false, status: 'not_configured' },
                        },
                        {
                            site_type: 'jobspy_timeout',
                            display_name: 'JobSpy Timeout',
                            seed_url: null,
                            description: null,
                            tags: [],
                            search_keywords: ['jobspy'],
                            fetch_mode: 'jobspy_api',
                            provider_name: null,
                            search_term: null,
                            location: null,
                            country: null,
                            results_wanted: 1,
                            hours_old: null,
                            options: {},
                            api_health: { available: false, status: 'timeout' },
                        },
                        {
                            site_type: 'jobspy_offline',
                            display_name: 'JobSpy Offline',
                            seed_url: null,
                            description: null,
                            tags: [],
                            search_keywords: ['jobspy'],
                            fetch_mode: 'jobspy_api',
                            provider_name: null,
                            search_term: null,
                            location: null,
                            country: null,
                            results_wanted: 1,
                            hours_old: null,
                            options: {},
                            api_health: { available: false, status: 'error' },
                        },
                        {
                            site_type: 'rss',
                            display_name: 'RSS Board',
                            seed_url: null,
                            description: null,
                            tags: [],
                            search_keywords: ['rss'],
                            fetch_mode: 'rss_feed',
                            provider_name: null,
                            search_term: null,
                            location: null,
                            country: null,
                            results_wanted: 1,
                            hours_old: null,
                            options: { nested: { source: 'rss' }, enabled: true },
                            api_health: null,
                        },
                    ],
                },
            });

            render(<DashboardControls />, { wrapper: createWrapper() });

            await waitFor(() => {
                expect(screen.getByText('Rate Limited Seed')).toBeInTheDocument();
            });
            expect(screen.getByText('Worker cooling down')).toBeInTheDocument();
            expect(screen.getByText('Worker degraded')).toBeInTheDocument();
            expect(screen.getByText('Worker disabled')).toBeInTheDocument();
            expect(screen.getByText('Worker unconfigured')).toBeInTheDocument();
            expect(screen.getByText('JobSpy not configured')).toBeInTheDocument();
            expect(screen.getByText('JobSpy timeout')).toBeInTheDocument();
            expect(screen.getByText('JobSpy offline')).toBeInTheDocument();
            expect(screen.getByText('rss feed')).toBeInTheDocument();
            expect(screen.getByText('Seed and custom sources')).toBeInTheDocument();
        });

        it('surfaces source fetch and user source mutation failures', async () => {
            mockPipelineApi.fetchSource.mockRejectedValueOnce({
                response: { data: { warnings: ['worker quota exhausted'] } },
            });
            mockPipelineApi.createUserAtsSource.mockRejectedValueOnce({
                response: { data: { error: 'duplicate source' } },
            });
            render(<DashboardControls />, { wrapper: createWrapper() });

            await waitFor(() => {
                expect(screen.getByText('TokyoDev')).toBeInTheDocument();
            });

            await userEvent.click(screen.getByRole('button', { name: /fetch/i }));
            await waitFor(() => {
                expect(toast.error).toHaveBeenCalledWith(
                    'Source fetch failed: worker quota exhausted'
                );
            });

            await userEvent.click(screen.getByRole('button', { name: /add source/i }));
            await userEvent.type(screen.getByLabelText('Name'), 'Duplicate Source');
            await userEvent.click(screen.getByRole('button', { name: /^add$/i }));

            await waitFor(() => {
                expect(toast.error).toHaveBeenCalledWith(
                    'ATS source add failed: duplicate source'
                );
            });
        });

        it('lets users sync, disable, and delete managed ATS sources', async () => {
            mockPipelineApi.getUserAtsSources.mockResolvedValue({
                status: 200,
                data: [
                    {
                        id: 'source-1',
                        tenant_id: 'tenant-1',
                        provider: 'lever',
                        display_name: 'Acme Lever',
                        status: 'active',
                        sync_interval_minutes: 120,
                        config: {},
                        capabilities: ['list_jobs'],
                        validation_status: 'pending',
                        last_validated_at: null,
                        last_error: null,
                        is_user_source: true,
                        owner_user_id: 'user-1',
                        source_url: 'https://jobs.lever.co/acme',
                        created_at: null,
                        updated_at: null,
                    },
                ],
            });
            render(<DashboardControls />, { wrapper: createWrapper() });

            await waitFor(() => {
                expect(screen.getByText('Acme Lever')).toBeInTheDocument();
            });
            const sourceCard = screen.getByText('Acme Lever').closest('div[class*="group"]') as HTMLElement;

            await userEvent.click(within(sourceCard).getByRole('button', { name: /sync/i }));
            await waitFor(() => {
                expect(mockPipelineApi.syncUserAtsSource).toHaveBeenCalledWith('source-1', true);
            });
            expect(toast.success).toHaveBeenCalledWith('2 jobs imported from Lever');

            await userEvent.click(within(sourceCard).getByRole('button', { name: /disable/i }));
            await waitFor(() => {
                expect(mockPipelineApi.updateUserAtsSource).toHaveBeenCalledWith('source-1', {
                    status: 'disabled',
                });
            });

            const deleteButton = within(sourceCard).getByRole('button', { name: /delete/i });
            await userEvent.click(deleteButton);
            expect(mockPipelineApi.deleteUserAtsSource).not.toHaveBeenCalled();
            expect(screen.getByRole('dialog', { name: /delete ATS source/i })).toBeInTheDocument();
            expect(screen.getByRole('button', { name: /cancel/i })).toHaveFocus();
            await userEvent.keyboard('{Escape}');
            expect(screen.queryByRole('dialog', { name: /delete ATS source/i })).not.toBeInTheDocument();
            expect(deleteButton).toHaveFocus();

            let resolveDelete: (value?: unknown) => void = () => undefined;
            mockPipelineApi.deleteUserAtsSource.mockImplementationOnce(() => new Promise((resolve) => {
                resolveDelete = resolve;
            }));

            await userEvent.click(deleteButton);
            await userEvent.click(screen.getByRole('button', { name: /delete source/i }));
            await waitFor(() => {
                expect(mockPipelineApi.deleteUserAtsSource).toHaveBeenCalledWith('source-1');
            });
            await userEvent.keyboard('{Escape}');
            expect(screen.getByRole('dialog', { name: /delete ATS source/i })).toBeInTheDocument();
            resolveDelete({});
            await waitFor(() => {
                expect(screen.queryByRole('dialog', { name: /delete ATS source/i })).not.toBeInTheDocument();
            });
        });

        it('lets users update a managed ATS source name and sync interval', async () => {
            mockPipelineApi.getUserAtsSources.mockResolvedValue({
                status: 200,
                data: [
                    {
                        id: 'source-edit',
                        tenant_id: 'tenant-1',
                        provider: 'greenhouse',
                        display_name: 'Editable Source',
                        status: 'active',
                        sync_interval_minutes: 120,
                        config: {},
                        capabilities: ['list_jobs'],
                        validation_status: 'pending',
                        last_validated_at: null,
                        last_error: null,
                        is_user_source: true,
                        owner_user_id: 'user-1',
                        source_url: 'https://boards.greenhouse.io/editable',
                        created_at: null,
                        updated_at: null,
                    },
                ],
            });
            mockPipelineApi.updateUserAtsSource.mockResolvedValueOnce({
                data: {
                    id: 'source-edit',
                    tenant_id: 'tenant-1',
                    provider: 'greenhouse',
                    display_name: 'Renamed Source',
                    status: 'active',
                    sync_interval_minutes: 240,
                    config: {},
                    capabilities: ['list_jobs'],
                    validation_status: 'pending',
                    last_validated_at: null,
                    last_error: null,
                    is_user_source: true,
                    owner_user_id: 'user-1',
                    source_url: 'https://boards.greenhouse.io/editable',
                    created_at: null,
                    updated_at: null,
                },
            });

            render(<DashboardControls />, { wrapper: createWrapper() });

            await waitFor(() => {
                expect(screen.getByText('Editable Source')).toBeInTheDocument();
            });
            const sourceCard = screen.getByText('Editable Source').closest('div[class*="group"]') as HTMLElement;

            await userEvent.click(within(sourceCard).getByRole('button', { name: /edit/i }));
            await userEvent.clear(within(sourceCard).getByLabelText('Name'));
            await userEvent.type(within(sourceCard).getByLabelText('Name'), 'Renamed Source');
            await userEvent.clear(within(sourceCard).getByLabelText('Sync minutes'));
            await userEvent.type(within(sourceCard).getByLabelText('Sync minutes'), '240');
            await userEvent.click(within(sourceCard).getByRole('button', { name: /save/i }));

            await waitFor(() => {
                expect(mockPipelineApi.updateUserAtsSource).toHaveBeenCalledWith('source-edit', {
                    display_name: 'Renamed Source',
                    sync_interval_minutes: 240,
                });
            });
            expect(toast.success).toHaveBeenCalledWith('Renamed Source updated');
        });

        it('validates managed source edits before sending updates', async () => {
            mockPipelineApi.getUserAtsSources.mockResolvedValue({
                status: 200,
                data: [
                    {
                        id: 'source-invalid-edit',
                        tenant_id: 'tenant-1',
                        provider: 'greenhouse',
                        display_name: 'Invalid Edit Source',
                        status: 'active',
                        sync_interval_minutes: 120,
                        config: {},
                        capabilities: ['list_jobs'],
                        validation_status: 'pending',
                        last_validated_at: null,
                        last_error: null,
                        is_user_source: true,
                        owner_user_id: 'user-1',
                        source_url: 'https://boards.greenhouse.io/invalid-edit',
                        created_at: null,
                        updated_at: null,
                    },
                ],
            });

            render(<DashboardControls />, { wrapper: createWrapper() });

            await waitFor(() => {
                expect(screen.getByText('Invalid Edit Source')).toBeInTheDocument();
            });
            const sourceCard = screen.getByText('Invalid Edit Source').closest('div[class*="group"]') as HTMLElement;

            await userEvent.click(within(sourceCard).getByRole('button', { name: /edit/i }));
            await userEvent.clear(within(sourceCard).getByLabelText('Name'));
            await userEvent.click(within(sourceCard).getByRole('button', { name: /save/i }));
            expect(toast.error).toHaveBeenCalledWith('Source name cannot be blank.');

            await userEvent.type(within(sourceCard).getByLabelText('Name'), 'Invalid Edit Source');
            await userEvent.clear(within(sourceCard).getByLabelText('Careers URL'));
            await userEvent.type(within(sourceCard).getByLabelText('Careers URL'), 'https://careers.example.com/jobs');
            await userEvent.click(within(sourceCard).getByRole('button', { name: /save/i }));
            expect(toast.error).toHaveBeenCalledWith('Use a Greenhouse, Lever, or Ashby board URL.');
            expect(mockPipelineApi.updateUserAtsSource).not.toHaveBeenCalled();
        });

        it('validates managed source sync interval bounds before sending updates', async () => {
            mockPipelineApi.getUserAtsSources.mockResolvedValue({
                status: 200,
                data: [
                    {
                        id: 'source-invalid-interval',
                        tenant_id: 'tenant-1',
                        provider: 'greenhouse',
                        display_name: 'Invalid Interval Source',
                        status: 'active',
                        sync_interval_minutes: 120,
                        config: {},
                        capabilities: ['list_jobs'],
                        validation_status: 'pending',
                        last_validated_at: null,
                        last_error: null,
                        is_user_source: true,
                        owner_user_id: 'user-1',
                        source_url: 'https://boards.greenhouse.io/invalid-interval',
                        created_at: null,
                        updated_at: null,
                    },
                ],
            });

            render(<DashboardControls />, { wrapper: createWrapper() });

            await waitFor(() => {
                expect(screen.getByText('Invalid Interval Source')).toBeInTheDocument();
            });
            const sourceCard = screen.getByText('Invalid Interval Source').closest('div[class*="group"]') as HTMLElement;

            await userEvent.click(within(sourceCard).getByRole('button', { name: /edit/i }));
            const syncInput = within(sourceCard).getByLabelText('Sync minutes');
            fireEvent.change(syncInput, { target: { value: '1' } });
            fireEvent.submit(syncInput.closest('form') as HTMLFormElement);
            expect(toast.error).toHaveBeenCalledWith('Sync interval must be between 5 and 1440 minutes.');
            expect(mockPipelineApi.updateUserAtsSource).not.toHaveBeenCalled();
        });

        it('lets users replace the ATS board for a managed source', async () => {
            mockPipelineApi.getUserAtsSources.mockResolvedValue({
                status: 200,
                data: [
                    {
                        id: 'source-edit-board',
                        tenant_id: 'tenant-1',
                        provider: 'greenhouse',
                        display_name: 'Editable Board',
                        status: 'active',
                        sync_interval_minutes: 120,
                        config: { board_token: 'old-board' },
                        capabilities: ['list_jobs'],
                        validation_status: 'pending',
                        last_validated_at: null,
                        last_error: null,
                        is_user_source: true,
                        owner_user_id: 'user-1',
                        source_url: 'https://boards.greenhouse.io/old-board',
                        created_at: null,
                        updated_at: null,
                    },
                ],
            });

            render(<DashboardControls />, { wrapper: createWrapper() });

            await waitFor(() => {
                expect(screen.getByText('Editable Board')).toBeInTheDocument();
            });
            const sourceCard = screen.getByText('Editable Board').closest('div[class*="group"]') as HTMLElement;

            await userEvent.click(within(sourceCard).getByRole('button', { name: /edit/i }));
            await userEvent.clear(within(sourceCard).getByLabelText('Careers URL'));
            await userEvent.type(within(sourceCard).getByLabelText('Careers URL'), 'https://jobs.lever.co/new-board');
            await userEvent.selectOptions(within(sourceCard).getByLabelText('Provider'), 'lever');
            await userEvent.clear(within(sourceCard).getByLabelText('Board ID'));
            await userEvent.type(within(sourceCard).getByLabelText('Board ID'), 'new-board');
            await userEvent.click(within(sourceCard).getByRole('button', { name: /save/i }));

            await waitFor(() => {
                expect(mockPipelineApi.updateUserAtsSource).toHaveBeenCalledWith('source-edit-board', {
                    display_name: 'Editable Board',
                    source_url: 'https://jobs.lever.co/new-board',
                    provider: 'lever',
                    identifier: 'new-board',
                    providers: undefined,
                    sync_interval_minutes: 120,
                });
            });
        });

        it('lets users re-enable a disabled managed ATS source', async () => {
            mockPipelineApi.getUserAtsSources.mockResolvedValue({
                status: 200,
                data: [
                    {
                        id: 'source-disabled',
                        tenant_id: 'tenant-1',
                        provider: 'ashby',
                        display_name: 'Disabled Ashby',
                        status: 'disabled',
                        sync_interval_minutes: 240,
                        config: {},
                        capabilities: ['list_jobs'],
                        validation_status: 'pending',
                        last_validated_at: null,
                        last_error: null,
                        is_user_source: true,
                        owner_user_id: 'user-1',
                        source_url: 'https://jobs.ashbyhq.com/acme',
                        created_at: null,
                        updated_at: null,
                    },
                ],
            });

            render(<DashboardControls />, { wrapper: createWrapper() });

            await waitFor(() => {
                expect(screen.getByText('Disabled Ashby')).toBeInTheDocument();
            });
            const sourceCard = screen.getByText('Disabled Ashby').closest('div[class*="group"]') as HTMLElement;

            expect(within(sourceCard).getByRole('button', { name: /sync/i })).toBeDisabled();
            await userEvent.click(within(sourceCard).getByRole('button', { name: /enable/i }));

            await waitFor(() => {
                expect(mockPipelineApi.updateUserAtsSource).toHaveBeenCalledWith('source-disabled', {
                    status: 'active',
                });
            });
        });

        it('surfaces managed ATS source action failures', async () => {
            mockPipelineApi.getUserAtsSources.mockResolvedValue({
                status: 200,
                data: [
                    {
                        id: 'source-errors',
                        tenant_id: 'tenant-1',
                        provider: 'greenhouse',
                        display_name: 'Error Source',
                        status: 'active',
                        sync_interval_minutes: 120,
                        config: {},
                        capabilities: ['list_jobs'],
                        validation_status: 'pending',
                        last_validated_at: null,
                        last_error: null,
                        is_user_source: true,
                        owner_user_id: 'user-1',
                        source_url: 'https://boards.greenhouse.io/acme',
                        created_at: null,
                        updated_at: null,
                    },
                ],
            });
            mockPipelineApi.syncUserAtsSource.mockRejectedValueOnce({
                response: { data: { message: 'sync offline' } },
            });
            mockPipelineApi.updateUserAtsSource.mockRejectedValueOnce({
                response: { data: { error: 'update rejected' } },
            });
            mockPipelineApi.deleteUserAtsSource.mockRejectedValueOnce(new Error('delete failed'));

            render(<DashboardControls />, { wrapper: createWrapper() });

            await waitFor(() => {
                expect(screen.getByText('Error Source')).toBeInTheDocument();
            });
            const sourceCard = screen.getByText('Error Source').closest('div[class*="group"]') as HTMLElement;

            await userEvent.click(within(sourceCard).getByRole('button', { name: /sync/i }));
            await waitFor(() => {
                expect(toast.error).toHaveBeenCalledWith('ATS source sync failed: sync offline');
            });

            await userEvent.click(within(sourceCard).getByRole('button', { name: /disable/i }));
            await waitFor(() => {
                expect(toast.error).toHaveBeenCalledWith('ATS source update failed: update rejected');
            });

            await userEvent.click(within(sourceCard).getByRole('button', { name: /delete/i }));
            await userEvent.click(screen.getByRole('button', { name: /delete source/i }));
            await waitFor(() => {
                expect(toast.error).toHaveBeenCalledWith('ATS source delete failed: delete failed');
            });
        });
    });
});
