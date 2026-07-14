import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { ResumeVariantPanel } from '../ResumeVariantPanel';
import { resumeVariantsApi } from '@/services/resumeVariantsApi';
import { toast } from '@/components/ui/Toast';
import type { ResumeVariant } from '@/types/api';

vi.mock('lucide-react', () => ({
    Download: ({ className }: any) => <svg data-testid="download-icon" className={className} />,
    RefreshCw: ({ className }: any) => <svg data-testid="refresh-icon" className={className} />,
    Wand2: ({ className }: any) => <svg data-testid="wand-icon" className={className} />,
}));

vi.mock('@/components/ui/Toast', () => ({
    toast: { success: vi.fn(), error: vi.fn() },
}));

vi.mock('@/services/resumeVariantsApi', () => ({
    resumeVariantsApi: {
        createForMatch: vi.fn(),
        downloadVariant: vi.fn(),
    },
}));

const createObjectURL = vi.fn(() => 'blob:resume-variant');
const revokeObjectURL = vi.fn();

function makeQueryWrapper() {
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
    return ({ children }: any) => (
        <QueryClientProvider client={qc}>{children}</QueryClientProvider>
    );
}

function makeVariant(overrides: Partial<ResumeVariant> = {}): ResumeVariant {
    return {
        id: 'variant-1',
        match_id: 'match-1',
        job_post_id: 'job-1',
        template_key: 'compact',
        generation_mode: 'deterministic',
        created_at: '2026-05-24T00:00:00Z',
        content: {
            contact: {
                name: 'Ada Engineer',
                email: 'ada@example.com',
                location: 'Tokyo',
                links: ['https://example.com/ada'],
            },
            summary: [{ text: 'Backend engineer with Python and Redis experience.' }],
            targeted_evidence: [{ text: 'Built FastAPI services with Redis queues.' }],
            skills: [{ text: 'Python' }, { text: 'Redis' }],
            experience: [
                {
                    entry_id: 'experience-0',
                    title: 'Backend Engineer',
                    company: 'ExampleCo',
                    start_date: '2022',
                    end_date: 'Present',
                    bullets: [{ text: 'Reduced API latency by 30% using Python.' }],
                },
            ],
            projects: [
                {
                    entry_id: 'project-0',
                    name: 'Queue Monitor',
                    technologies: ['Python', 'Redis'],
                    bullets: [{ text: 'Built queue monitoring dashboards.' }],
                },
            ],
            education: [
                {
                    degree: 'BSc',
                    field_of_study: 'Computer Science',
                    institution: 'Example University',
                    graduation_year: 2020,
                },
            ],
            generation: {
                tailored: true,
                provider: 'nvidia',
                model: 'mistralai/mistral-medium-3.5-128b',
            },
            gaps: [{ text: 'Kubernetes leadership' }],
            source_quality: {
                job_description_completeness: 'full',
                job_description_source: 'ats.greenhouse',
            },
        },
        evidence_map: {
            claim_count: 4,
            source_types: ['structured_resume', 'job_match_requirement'],
        },
        warnings: ['Unsupported requirement not claimed: Kubernetes leadership'],
        download_formats: ['markdown', 'html', 'docx'],
        reused: false,
        quota_status: { daily_remaining: 9, hourly_remaining: 2 },
        ...overrides,
    };
}

describe('ResumeVariantPanel', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        Object.defineProperty(URL, 'createObjectURL', {
            configurable: true,
            value: createObjectURL,
        });
        Object.defineProperty(URL, 'revokeObjectURL', {
            configurable: true,
            value: revokeObjectURL,
        });
    });

    it('generates a draft and shows evidence-backed preview details', async () => {
        const variant = makeVariant();
        vi.mocked(resumeVariantsApi.createForMatch).mockResolvedValueOnce({
            data: { success: true, variant },
        } as never);

        render(<ResumeVariantPanel matchId="match-1" />, { wrapper: makeQueryWrapper() });

        expect(screen.getByText('No resume draft for this match yet.')).toBeInTheDocument();
        fireEvent.click(screen.getByRole('button', { name: /generate draft/i }));

        await waitFor(() => {
            expect(resumeVariantsApi.createForMatch).toHaveBeenCalledWith('match-1', {
                template_key: 'compact',
                tone: 'concise',
                force: false,
            });
        });
        expect(screen.getByText('Backend engineer with Python and Redis experience.')).toBeInTheDocument();
        expect(screen.getByText('Ada Engineer')).toBeInTheDocument();
        expect(screen.getByText('Backend Engineer — ExampleCo')).toBeInTheDocument();
        expect(screen.getByText('Reduced API latency by 30% using Python.')).toBeInTheDocument();
        expect(screen.getByText('Queue Monitor')).toBeInTheDocument();
        expect(screen.getByText(/Tailored by nvidia/)).toBeInTheDocument();
        expect(screen.getByText(/4 sourced claims from structured_resume, job_match_requirement/i)).toBeInTheDocument();
        expect(screen.getByText('Not claimed')).toBeInTheDocument();
        expect(screen.getByText('Kubernetes leadership')).toBeInTheDocument();
        expect(screen.getByText('Source quality')).toBeInTheDocument();
        expect(screen.getByText('ats.greenhouse')).toBeInTheDocument();
        expect(screen.getByText('Unsupported requirement not claimed: Kubernetes leadership')).toBeInTheDocument();
        expect(screen.getByRole('button', { name: /download markdown/i })).toBeEnabled();
        expect(toast.success).toHaveBeenCalledWith('Resume draft generated.');
    });

    it('renders fallback copy and filters unsupported download formats', async () => {
        const variant = makeVariant({
            content: {
                summary: [{ text: '   ' }],
                targeted_evidence: [],
                skills: [{ text: '' }],
                experience: [],
            },
            evidence_map: {},
            warnings: [],
            download_formats: ['markdown', 'pdf' as any],
            reused: true,
        });
        vi.mocked(resumeVariantsApi.createForMatch).mockResolvedValueOnce({
            data: { success: true, variant },
        } as never);

        render(<ResumeVariantPanel matchId="match-1" />, { wrapper: makeQueryWrapper() });
        fireEvent.click(screen.getByRole('button', { name: /generate draft/i }));

        expect(await screen.findByText('No summary text generated.')).toBeInTheDocument();
        expect(screen.getByText('Sourced claims')).toBeInTheDocument();
        expect(screen.getByText('Current draft reused.')).toBeInTheDocument();
        expect(screen.getByRole('button', { name: /download markdown/i })).toBeInTheDocument();
        expect(screen.queryByRole('button', { name: /download pdf/i })).not.toBeInTheDocument();
        expect(toast.success).toHaveBeenCalledWith('Resume draft already current.');
    });

    it('surfaces generation failures without leaving a blank panel', async () => {
        vi.mocked(resumeVariantsApi.createForMatch).mockRejectedValueOnce(new Error('Quota exceeded.'));

        render(<ResumeVariantPanel matchId="match-1" />, { wrapper: makeQueryWrapper() });
        fireEvent.click(screen.getByRole('button', { name: /generate draft/i }));

        await waitFor(() => {
            expect(screen.getByRole('alert')).toHaveTextContent('Quota exceeded.');
        });
        expect(screen.getByText('No resume draft for this match yet.')).toBeInTheDocument();
        expect(toast.error).toHaveBeenCalledWith('Quota exceeded.');
    });

    it('downloads the generated draft in the selected format', async () => {
        const clickSpy = vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => undefined);
        vi.mocked(resumeVariantsApi.createForMatch).mockResolvedValueOnce({
            data: { success: true, variant: makeVariant() },
        } as never);
        vi.mocked(resumeVariantsApi.downloadVariant).mockResolvedValueOnce({
            blob: new Blob(['# Resume'], { type: 'text/markdown' }),
            filename: 'resume-variant.md',
        });

        render(<ResumeVariantPanel matchId="match-1" />, { wrapper: makeQueryWrapper() });
        fireEvent.click(screen.getByRole('button', { name: /generate draft/i }));

        await screen.findByRole('button', { name: /download markdown/i });
        fireEvent.click(screen.getByRole('button', { name: /download markdown/i }));

        await waitFor(() => {
            expect(resumeVariantsApi.downloadVariant).toHaveBeenCalledWith('variant-1', 'markdown');
        });
        expect(createObjectURL).toHaveBeenCalledOnce();
        expect(clickSpy).toHaveBeenCalledOnce();
        expect(revokeObjectURL).toHaveBeenCalledWith('blob:resume-variant');
    });
});
