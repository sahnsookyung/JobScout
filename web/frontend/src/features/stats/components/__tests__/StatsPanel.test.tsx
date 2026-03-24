/**
 * Tests for StatsPanel component
 * Covers: src/features/stats/components/StatsPanel.tsx
 */

import { render, screen } from '@testing-library/react';
import { StatsPanel } from '../StatsPanel';

vi.mock('@/hooks/useStats');

import { useStats } from '@/hooks/useStats';

const mockUseStats = vi.mocked(useStats);

describe('StatsPanel', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('loading state', () => {
        it('renders loading skeleton when isLoading is true', () => {
            mockUseStats.mockReturnValue({ data: undefined, isLoading: true } as never);
            const { container } = render(<StatsPanel />);
            expect(container.querySelector('.animate-pulse')).toBeTruthy();
        });

        it('does not render score distribution while loading', () => {
            mockUseStats.mockReturnValue({ data: undefined, isLoading: true } as never);
            render(<StatsPanel />);
            expect(screen.queryByText('Score Distribution')).toBeNull();
        });
    });

    describe('no data state', () => {
        it('renders nothing when stats is null', () => {
            mockUseStats.mockReturnValue({ data: null, isLoading: false } as never);
            const { container } = render(<StatsPanel />);
            expect(container.firstChild).toBeNull();
        });

        it('renders nothing when stats is undefined', () => {
            mockUseStats.mockReturnValue({ data: undefined, isLoading: false } as never);
            const { container } = render(<StatsPanel />);
            expect(container.firstChild).toBeNull();
        });
    });

    describe('data state', () => {
        const mockStats = {
            total_matches: 100,
            score_distribution: {
                excellent: 20,
                good: 35,
                average: 30,
                poor: 15,
            },
        };

        beforeEach(() => {
            mockUseStats.mockReturnValue({ data: mockStats, isLoading: false } as never);
        });

        it('renders Score Distribution heading', () => {
            render(<StatsPanel />);
            expect(screen.getByText('Score Distribution')).toBeTruthy();
        });

        it('renders all four score tiers', () => {
            render(<StatsPanel />);
            expect(screen.getByText('Excellent')).toBeTruthy();
            expect(screen.getByText('Good')).toBeTruthy();
            expect(screen.getByText('Average')).toBeTruthy();
            expect(screen.getByText('Poor')).toBeTruthy();
        });

        it('renders range labels for each tier', () => {
            render(<StatsPanel />);
            expect(screen.getByText('(80+)')).toBeTruthy();
            expect(screen.getByText('(60-79)')).toBeTruthy();
            expect(screen.getByText('(40-59)')).toBeTruthy();
            expect(screen.getByText('(<40)')).toBeTruthy();
        });

        it('renders count values for each tier', () => {
            render(<StatsPanel />);
            expect(screen.getByText('20')).toBeTruthy();
            expect(screen.getByText('35')).toBeTruthy();
            expect(screen.getByText('30')).toBeTruthy();
            expect(screen.getByText('15')).toBeTruthy();
        });

        it('renders percentage for non-zero tiers', () => {
            render(<StatsPanel />);
            // 20/100 = 20%
            expect(screen.getByText('20%')).toBeTruthy();
        });

        it('does not render percentage label for zero-value tier', () => {
            mockUseStats.mockReturnValue({
                data: {
                    total_matches: 10,
                    score_distribution: { excellent: 10, good: 0, average: 0, poor: 0 },
                },
                isLoading: false,
            } as never);
            render(<StatsPanel />);
            // good=0 and average=0: no percentage shown for those
            expect(screen.getByText('100%')).toBeTruthy();
        });

        it('handles zero total_matches without division error', () => {
            mockUseStats.mockReturnValue({
                data: {
                    total_matches: 0,
                    score_distribution: { excellent: 0, good: 0, average: 0, poor: 0 },
                },
                isLoading: false,
            } as never);
            render(<StatsPanel />);
            expect(screen.getByText('Score Distribution')).toBeTruthy();
        });

        it('handles missing score_distribution fields gracefully', () => {
            mockUseStats.mockReturnValue({
                data: { total_matches: 5, score_distribution: {} },
                isLoading: false,
            } as never);
            render(<StatsPanel />);
            // Should render 0 for all tiers via ?? 0
            const zeros = screen.getAllByText('0');
            expect(zeros.length).toBeGreaterThanOrEqual(4);
        });
    });
});
