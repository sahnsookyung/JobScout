/**
 * Tests for PolicyPanel component
 * Covers: src/features/config/components/PolicyPanel.tsx
 */

import { render, screen, fireEvent, act } from '@testing-library/react';
import { PolicyPanel } from '../PolicyPanel';
import { PreferenceRankingSettings } from '@/features/preferences/components/PreferenceRankingSettings';

vi.mock('@/hooks/usePolicy');
vi.mock('@tanstack/react-query', () => ({
    useQuery: () => ({
        data: {
            queued: 0,
            scheduled: 0,
            db_pending: 0,
            db_retryable_failed: 0,
        },
        isLoading: false,
    }),
}));
vi.mock('lucide-react', () => ({
    Sliders: () => <svg data-testid="sliders-icon" />,
    Minus: () => <svg data-testid="minus-icon" />,
    Plus: () => <svg data-testid="plus-icon" />,
}));

import { usePolicy } from '@/hooks/usePolicy';

const mockUsePolicy = vi.mocked(usePolicy);

const defaultHook = {
    policy: undefined,
    isLoading: false,
    updatePolicy: vi.fn(),
    updatePolicyAsync: vi.fn().mockResolvedValue({ data: {} }),
    isUpdatingPolicy: false,
    applyPreset: vi.fn(),
};

describe('PolicyPanel', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        vi.useFakeTimers();
        mockUsePolicy.mockReturnValue(defaultHook);
    });

    afterEach(() => {
        vi.useRealTimers();
    });

    describe('loading state', () => {
        it('renders loading skeleton when isLoading is true', () => {
            mockUsePolicy.mockReturnValue({ ...defaultHook, isLoading: true });
            const { container } = render(<PolicyPanel />);
            expect(container.querySelector('.animate-pulse')).toBeTruthy();
        });

        it('does not render sliders content while loading', () => {
            mockUsePolicy.mockReturnValue({ ...defaultHook, isLoading: true });
            render(<PolicyPanel />);
            expect(screen.queryByText('Result Policy')).toBeNull();
        });
    });

    describe('default state (no policy)', () => {
        it('renders Result Policy heading', () => {
            render(<PolicyPanel />);
            expect(screen.getByText('Result policy')).toBeTruthy();
        });

        it('renders Quick Presets label', () => {
            render(<PolicyPanel />);
            expect(screen.getByText('Quick presets')).toBeTruthy();
        });

        it('renders three preset buttons', () => {
            render(<PolicyPanel />);
            expect(screen.getByText('Strict')).toBeTruthy();
            expect(screen.getByRole('button', { name: 'Balanced' })).toBeTruthy();
            expect(screen.getByText('Discovery')).toBeTruthy();
        });

        it('renders Min Fit Score label', () => {
            render(<PolicyPanel />);
            expect(screen.getByText('Min fit score')).toBeTruthy();
        });

        it('renders Max Results label', () => {
            render(<PolicyPanel />);
            expect(screen.getByText('Max results')).toBeTruthy();
        });

        it('keeps preference ranking controls out of the shortlist sidebar', () => {
            render(<PolicyPanel />);

            expect(screen.queryByLabelText(/default order/i)).not.toBeInTheDocument();
            expect(screen.queryByText(/LLM second pass/i)).not.toBeInTheDocument();
        });

        it('shows default minFit value of 55', () => {
            render(<PolicyPanel />);
            const slider = screen.getByRole('slider', { name: /minimum fit score/i });
            expect((slider as HTMLInputElement).value).toBe('55');
        });

        it('shows default topK value of 50', () => {
            render(<PolicyPanel />);
            const slider = screen.getByRole('slider', { name: /maximum number of results/i });
            expect((slider as HTMLInputElement).value).toBe('50');
        });
    });

    describe('with policy data', () => {
        it('syncs minFit from policy when policy is set', async () => {
            mockUsePolicy.mockReturnValue({
                ...defaultHook,
                policy: { min_fit: 70, top_k: 100, min_jd_required_coverage: null },
            });
            render(<PolicyPanel />);
            const slider = screen.getByRole('slider', { name: /minimum fit score/i });
            expect((slider as HTMLInputElement).value).toBe('70');
        });

        it('syncs topK from policy when policy is set', async () => {
            mockUsePolicy.mockReturnValue({
                ...defaultHook,
                policy: { min_fit: 70, top_k: 150, min_jd_required_coverage: null },
            });
            render(<PolicyPanel />);
            const slider = screen.getByRole('slider', { name: /maximum number of results/i });
            expect((slider as HTMLInputElement).value).toBe('150');
        });

        it('hydrates ranking mode and balanced weights from policy', () => {
            mockUsePolicy.mockReturnValue({
                ...defaultHook,
                policy: {
                    min_fit: 55,
                    top_k: 50,
                    min_jd_required_coverage: null,
                    active_default_mode: 'preference_first',
                    balanced_w_pref: 0.7,
                    balanced_w_fit: 0.3,
                },
            });
            render(<PreferenceRankingSettings />);

            expect(screen.getByLabelText(/default order/i)).toHaveValue('preference_first');
            expect(screen.getByRole('slider', { name: /balanced preference weight/i })).toHaveValue('70');
            expect(screen.getByText('70% preference · 30% fit')).toBeInTheDocument();
        });

        it('marks the matching preset active when hydrated from policy', () => {
            mockUsePolicy.mockReturnValue({
                ...defaultHook,
                policy: { min_fit: 70, top_k: 25, min_jd_required_coverage: 0.8 },
            });
            render(<PolicyPanel />);
            const strictBtn = screen.getByText('Strict').closest('button');
            expect(strictBtn?.getAttribute('aria-pressed')).toBe('true');
        });

        it('does not auto-save policy values during hydration', async () => {
            mockUsePolicy.mockReturnValue({
                ...defaultHook,
                policy: { min_fit: 70, top_k: 150, min_jd_required_coverage: null },
            });
            render(<PolicyPanel />);

            await act(async () => {
                vi.advanceTimersByTime(300);
            });

            expect(defaultHook.updatePolicy).not.toHaveBeenCalled();
        });
    });

    describe('interactions', () => {
        it('calls applyPreset when Strict preset is clicked', () => {
            render(<PolicyPanel />);
            fireEvent.click(screen.getByText('Strict'));
            expect(defaultHook.applyPreset).toHaveBeenCalledWith('strict');
        });

        it('calls applyPreset when Discovery preset is clicked', () => {
            render(<PolicyPanel />);
            fireEvent.click(screen.getByText('Discovery'));
            expect(defaultHook.applyPreset).toHaveBeenCalledWith('discovery');
        });

        it('updates minFit slider value when changed', () => {
            render(<PolicyPanel />);
            const slider = screen.getByRole('slider', { name: /minimum fit score/i });
            fireEvent.change(slider, { target: { value: '75' } });
            expect((slider as HTMLInputElement).value).toBe('75');
        });

        it('updates topK slider value when changed', () => {
            render(<PolicyPanel />);
            const slider = screen.getByRole('slider', { name: /maximum number of results/i });
            fireEvent.change(slider, { target: { value: '100' } });
            expect((slider as HTMLInputElement).value).toBe('100');
        });

        it('calls updatePolicy after debounce when minFit changes', async () => {
            mockUsePolicy.mockReturnValue({
                ...defaultHook,
                policy: { min_fit: 55, top_k: 50, min_jd_required_coverage: null },
            });
            render(<PolicyPanel />);
            const slider = screen.getByRole('slider', { name: /minimum fit score/i });
            fireEvent.change(slider, { target: { value: '80' } });
            await act(async () => { vi.advanceTimersByTime(300); });
            expect(defaultHook.updatePolicy).toHaveBeenCalledWith(
                expect.objectContaining({ min_fit: 80, top_k: 50 })
            );
        });

        it('preserves an explicit zero coverage floor when auto-saving', async () => {
            mockUsePolicy.mockReturnValue({
                ...defaultHook,
                policy: { min_fit: 55, top_k: 50, min_jd_required_coverage: 0 },
            });
            render(<PolicyPanel />);
            const slider = screen.getByRole('slider', { name: /minimum fit score/i });
            fireEvent.change(slider, { target: { value: '30' } });
            await act(async () => { vi.advanceTimersByTime(300); });
            expect(defaultHook.updatePolicy).toHaveBeenCalledWith(
                expect.objectContaining({ min_jd_required_coverage: 0 })
            );
        });

        it('does not auto-save writable LLM judge settings before Apply', async () => {
            mockUsePolicy.mockReturnValue({
                ...defaultHook,
                policy: {
                    min_fit: 55,
                    top_k: 50,
                    min_jd_required_coverage: null,
                    llm_judge_enabled: false,
                    llm_judge_auto_enqueue_enabled: false,
                    llm_judge_top_n: 2,
                    llm_judge_top_n_max: 4,
                    llm_judge_available: true,
                    llm_judge_revision: 3,
                },
            });
            render(<PreferenceRankingSettings />);

            fireEvent.click(screen.getByRole('checkbox', { name: /enable llm judging/i }));
            fireEvent.click(screen.getByRole('checkbox', { name: /automatically queue top n llm judging/i }));
            fireEvent.click(screen.getByRole('button', { name: /increase llm judge top n/i }));

            await act(async () => { vi.advanceTimersByTime(300); });

            expect(defaultHook.updatePolicy).not.toHaveBeenCalled();
            expect(defaultHook.updatePolicyAsync).not.toHaveBeenCalled();
        });

        it('applies writable LLM judge settings explicitly', async () => {
            const updatePolicyAsync = vi.fn().mockResolvedValue({
                data: {
                    min_fit: 55,
                    top_k: 50,
                    min_jd_required_coverage: null,
                    llm_judge_enabled: true,
                    llm_judge_auto_enqueue_enabled: true,
                    llm_judge_top_n: 3,
                    llm_judge_top_n_max: 4,
                    llm_judge_available: true,
                    llm_judge_enqueue_state: 'scheduled',
    llm_judge_enqueue_job_id: 'llm-top-n-job-1',
                },
            });
            mockUsePolicy.mockReturnValue({
                ...defaultHook,
                updatePolicyAsync,
                policy: {
                    min_fit: 55,
                    top_k: 50,
                    min_jd_required_coverage: null,
                    llm_judge_enabled: false,
                    llm_judge_auto_enqueue_enabled: false,
                    llm_judge_top_n: 2,
                    llm_judge_top_n_max: 4,
                    llm_judge_available: true,
                    llm_judge_revision: 3,
                },
            });
            render(<PreferenceRankingSettings />);

            fireEvent.click(screen.getByRole('checkbox', { name: /enable llm judging/i }));
            fireEvent.click(screen.getByRole('checkbox', { name: /automatically queue top n llm judging/i }));
            fireEvent.click(screen.getByRole('button', { name: /increase llm judge top n/i }));

            await act(async () => {
                fireEvent.click(screen.getByRole('button', { name: /apply/i }));
            });

            expect(updatePolicyAsync).toHaveBeenCalledWith({
                min_fit: 55,
                top_k: 50,
                min_jd_required_coverage: null,
                llm_judge_enabled: true,
                llm_judge_auto_enqueue_enabled: true,
                llm_judge_top_n: 3,
            });
            expect(screen.getByText('Scheduled')).toBeTruthy();
        });

        it('turns off auto top-N when LLM judging is disabled', async () => {
            const updatePolicyAsync = vi.fn().mockResolvedValue({
                data: {
                    min_fit: 55,
                    top_k: 50,
                    min_jd_required_coverage: null,
                    llm_judge_enabled: false,
                    llm_judge_auto_enqueue_enabled: false,
                    llm_judge_top_n: 2,
                    llm_judge_top_n_max: 4,
                    llm_judge_available: true,
                },
            });
            mockUsePolicy.mockReturnValue({
                ...defaultHook,
                updatePolicyAsync,
                policy: {
                    min_fit: 55,
                    top_k: 50,
                    min_jd_required_coverage: null,
                    llm_judge_enabled: true,
                    llm_judge_auto_enqueue_enabled: true,
                    llm_judge_top_n: 2,
                    llm_judge_top_n_max: 4,
                    llm_judge_available: true,
                },
            });
            render(<PreferenceRankingSettings />);

            fireEvent.click(screen.getByRole('checkbox', { name: /enable llm judging/i }));

            await act(async () => {
                fireEvent.click(screen.getByRole('button', { name: /apply/i }));
            });

            expect(updatePolicyAsync).toHaveBeenCalledWith(
                expect.objectContaining({
                    llm_judge_enabled: false,
                    llm_judge_auto_enqueue_enabled: false,
                })
            );
        });

        it('calls updatePolicy after debounce when topK changes', async () => {
            mockUsePolicy.mockReturnValue({
                ...defaultHook,
                policy: { min_fit: 55, top_k: 50, min_jd_required_coverage: null },
            });
            render(<PolicyPanel />);
            const slider = screen.getByRole('slider', { name: /maximum number of results/i });
            fireEvent.change(slider, { target: { value: '120' } });
            await act(async () => { vi.advanceTimersByTime(300); });
            expect(defaultHook.updatePolicy).toHaveBeenCalledWith(
                expect.objectContaining({ min_fit: 55, top_k: 120 })
            );
        });

        it('saves ranking mode and complementary balanced weights', async () => {
            mockUsePolicy.mockReturnValue({
                ...defaultHook,
                policy: {
                    min_fit: 55,
                    top_k: 50,
                    min_jd_required_coverage: null,
                    active_default_mode: 'balanced',
                    balanced_w_pref: 0.6,
                    balanced_w_fit: 0.4,
                },
            });
            render(<PreferenceRankingSettings />);

            fireEvent.change(screen.getByLabelText(/default order/i), {
                target: { value: 'fit_first' },
            });
            fireEvent.change(screen.getByRole('slider', { name: /balanced preference weight/i }), {
                target: { value: '35' },
            });
            await act(async () => { vi.advanceTimersByTime(300); });

            expect(defaultHook.updatePolicy).toHaveBeenCalledWith(
                expect.objectContaining({
                    active_default_mode: 'fit_first',
                    balanced_w_pref: 0.35,
                    balanced_w_fit: 0.65,
                }),
            );
        });

        it('flushes a pending ranking change when the settings close', () => {
            const updatePolicy = vi.fn();
            mockUsePolicy.mockReturnValue({
                ...defaultHook,
                updatePolicy,
                policy: {
                    min_fit: 55,
                    top_k: 50,
                    min_jd_required_coverage: null,
                    active_default_mode: 'balanced',
                    balanced_w_pref: 0.6,
                    balanced_w_fit: 0.4,
                },
            });
            const { unmount } = render(<PreferenceRankingSettings />);

            fireEvent.change(screen.getByLabelText(/default order/i), {
                target: { value: 'fit_first' },
            });
            unmount();

            expect(updatePolicy).toHaveBeenCalledWith(expect.objectContaining({
                active_default_mode: 'fit_first',
                balanced_w_pref: 0.6,
                balanced_w_fit: 0.4,
            }));
        });

        it('marks Balanced preset as aria-pressed=true by default', () => {
            render(<PolicyPanel />);
            const balancedBtn = screen.getByRole('button', { name: 'Balanced' });
            expect(balancedBtn?.getAttribute('aria-pressed')).toBe('true');
        });

        it('marks Strict preset as aria-pressed=true after clicking', () => {
            render(<PolicyPanel />);
            fireEvent.click(screen.getByText('Strict'));
            const strictBtn = screen.getByText('Strict').closest('button');
            expect(strictBtn?.getAttribute('aria-pressed')).toBe('true');
        });

        it('updates slider values immediately when a preset is clicked', () => {
            render(<PolicyPanel />);

            fireEvent.click(screen.getByText('Discovery'));

            expect(screen.getByRole('slider', { name: /minimum fit score/i })).toHaveValue('40');
            expect(screen.getByRole('slider', { name: /maximum number of results/i })).toHaveValue('100');
            expect(defaultHook.applyPreset).toHaveBeenCalledWith('discovery');
        });

        it('resets preset to balanced when slider is manually adjusted', () => {
            render(<PolicyPanel />);
            // First click Strict
            fireEvent.click(screen.getByText('Strict'));
            // Then change the slider
            const slider = screen.getByRole('slider', { name: /minimum fit score/i });
            fireEvent.change(slider, { target: { value: '60' } });
            // Balanced should be active again
            const balancedBtn = screen.getByRole('button', { name: 'Balanced' });
            expect(balancedBtn?.getAttribute('aria-pressed')).toBe('true');
        });
    });

    describe('slider range labels', () => {
        it('renders min fit range 0-100', () => {
            render(<PolicyPanel />);
            const labels = screen.getAllByText('0');
            expect(labels.length).toBeGreaterThanOrEqual(1);
            const labels100 = screen.getAllByText('100');
            expect(labels100.length).toBeGreaterThanOrEqual(1);
        });

        it('renders top-k range 10-200', () => {
            render(<PolicyPanel />);
            expect(screen.getByText('10')).toBeTruthy();
            expect(screen.getByText('200')).toBeTruthy();
        });
    });
});
