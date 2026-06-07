/**
 * Tests for PolicyPanel component
 * Covers: src/features/config/components/PolicyPanel.tsx
 */

import { render, screen, fireEvent, act } from '@testing-library/react';
import { PolicyPanel } from '../PolicyPanel';

vi.mock('@/hooks/usePolicy');
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
            expect(screen.getByText('Balanced')).toBeTruthy();
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

        it('updates writable LLM judge settings after debounce', async () => {
            mockUsePolicy.mockReturnValue({
                ...defaultHook,
                policy: {
                    min_fit: 55,
                    top_k: 50,
                    min_jd_required_coverage: null,
                    llm_judge_enabled: false,
                    llm_judge_top_n: 2,
                    llm_judge_top_n_max: 4,
                    llm_judge_available: true,
                    llm_judge_revision: 3,
                },
            });
            render(<PolicyPanel />);

            fireEvent.click(screen.getByRole('checkbox', { name: /enable llm judging/i }));
            fireEvent.click(screen.getByRole('button', { name: /increase llm judge top n/i }));

            await act(async () => { vi.advanceTimersByTime(300); });

            expect(defaultHook.updatePolicy).toHaveBeenCalledWith({
                min_fit: 55,
                top_k: 50,
                min_jd_required_coverage: null,
                llm_judge_enabled: true,
                llm_judge_top_n: 3,
            });
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

        it('marks Balanced preset as aria-pressed=true by default', () => {
            render(<PolicyPanel />);
            const balancedBtn = screen.getByText('Balanced').closest('button');
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
            const balancedBtn = screen.getByText('Balanced').closest('button');
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
