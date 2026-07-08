/**
 * Tests for uncovered React hooks
 * Covers: useDebounce, useMatchDetails, useMatches, usePolicy
 */

import { renderHook, waitFor, act } from '@testing-library/react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

vi.mock('@/services/matchesApi', () => ({
    matchesApi: {
        getMatches: vi.fn(),
        getMatchDetails: vi.fn(),
    },
}));

vi.mock('@/services/configApi', () => ({
    configApi: {
        getPolicy: vi.fn(),
        updatePolicy: vi.fn(),
        applyPreset: vi.fn(),
    },
}));

vi.mock('@/services/candidatePreferencesApi', () => ({
    candidatePreferencesApi: {
        getPreferences: vi.fn(),
        updatePreferences: vi.fn(),
    },
}));

import { useDebounce } from '../useDebounce';
import { useCandidatePreferences } from '../useCandidatePreferences';
import { useMatchDetails } from '../useMatchDetails';
import { useMatches } from '../useMatches';
import { usePolicy } from '../usePolicy';

const createWrapper = () => {
    const queryClient = new QueryClient({
        defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
    });
    return ({ children }: { children: React.ReactNode }) => (
        <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );
};

// ---------------------------------------------------------------------------
// useDebounce
// ---------------------------------------------------------------------------

describe('useDebounce', () => {
    beforeEach(() => {
        vi.useFakeTimers();
    });

    afterEach(() => {
        vi.useRealTimers();
    });

    it('returns the initial value immediately', () => {
        const { result } = renderHook(() => useDebounce('hello', 300));
        expect(result.current).toBe('hello');
    });

    it('returns old value before delay elapses', () => {
        const { result, rerender } = renderHook(
            ({ value }: { value: string }) => useDebounce(value, 300),
            { initialProps: { value: 'initial' } }
        );
        rerender({ value: 'updated' });
        act(() => { vi.advanceTimersByTime(200); });
        expect(result.current).toBe('initial');
    });

    it('returns new value after delay elapses', () => {
        const { result, rerender } = renderHook(
            ({ value }: { value: string }) => useDebounce(value, 300),
            { initialProps: { value: 'initial' } }
        );
        rerender({ value: 'updated' });
        act(() => { vi.advanceTimersByTime(300); });
        expect(result.current).toBe('updated');
    });

    it('resets the timer when value changes before delay', () => {
        const { result, rerender } = renderHook(
            ({ value }: { value: string }) => useDebounce(value, 300),
            { initialProps: { value: 'a' } }
        );
        rerender({ value: 'b' });
        act(() => { vi.advanceTimersByTime(200); });
        rerender({ value: 'c' });
        act(() => { vi.advanceTimersByTime(200); });
        // Still hasn't been 300ms since last change
        expect(result.current).toBe('a');
        act(() => { vi.advanceTimersByTime(100); });
        expect(result.current).toBe('c');
    });

    it('uses default delay of 500ms when not specified', () => {
        const { result, rerender } = renderHook(
            ({ value }: { value: string }) => useDebounce(value),
            { initialProps: { value: 'start' } }
        );
        rerender({ value: 'end' });
        act(() => { vi.advanceTimersByTime(499); });
        expect(result.current).toBe('start');
        act(() => { vi.advanceTimersByTime(1); });
        expect(result.current).toBe('end');
    });

    it('works with number values', () => {
        const { result, rerender } = renderHook(
            ({ value }: { value: number }) => useDebounce(value, 100),
            { initialProps: { value: 1 } }
        );
        rerender({ value: 42 });
        act(() => { vi.advanceTimersByTime(100); });
        expect(result.current).toBe(42);
    });

    it('cleans up the timer on unmount', () => {
        const clearSpy = vi.spyOn(globalThis, 'clearTimeout');
        const { rerender, unmount } = renderHook(
            ({ value }: { value: string }) => useDebounce(value, 300),
            { initialProps: { value: 'a' } }
        );
        rerender({ value: 'b' });
        unmount();
        expect(clearSpy).toHaveBeenCalled();
    });
});

// ---------------------------------------------------------------------------
// useMatchDetails
// ---------------------------------------------------------------------------

describe('useMatchDetails', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('is disabled and does not fetch when matchId is null', async () => {
        const { matchesApi } = await import('@/services/matchesApi');
        const { result } = renderHook(() => useMatchDetails(null), {
            wrapper: createWrapper(),
        });
        expect(result.current.isFetching).toBe(false);
        expect(matchesApi.getMatchDetails).not.toHaveBeenCalled();
    });

    it('fetches match details when matchId is provided', async () => {
        const { matchesApi } = await import('@/services/matchesApi');
        const mockData = { id: 'match-1', fit_score: 85 };
        (matchesApi.getMatchDetails as any).mockResolvedValue({ data: mockData });

        const { result } = renderHook(() => useMatchDetails('match-1'), {
            wrapper: createWrapper(),
        });

        await waitFor(() => expect(result.current.isSuccess).toBe(true));
        expect(result.current.data).toEqual(mockData);
        expect(matchesApi.getMatchDetails).toHaveBeenCalledWith('match-1');
    });

    it('sets error state on fetch failure', async () => {
        const { matchesApi } = await import('@/services/matchesApi');
        (matchesApi.getMatchDetails as any).mockRejectedValue(new Error('Not found'));

        const { result } = renderHook(() => useMatchDetails('bad-id'), {
            wrapper: createWrapper(),
        });

        await waitFor(() => expect(result.current.isError).toBe(true));
        expect(result.current.error).toBeInstanceOf(Error);
    });

    it('is initially loading when matchId is provided', () => {
        const { result } = renderHook(() => useMatchDetails('match-1'), {
            wrapper: createWrapper(),
        });
        expect(result.current.isLoading).toBe(true);
    });
});

// ---------------------------------------------------------------------------
// useMatches
// ---------------------------------------------------------------------------

describe('useMatches', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('fetches matches with default params', async () => {
        const { matchesApi } = await import('@/services/matchesApi');
        const mockMatches = { matches: [{ id: '1' }], total: 1 };
        (matchesApi.getMatches as any).mockResolvedValue({ data: mockMatches });

        const { result } = renderHook(() => useMatches(), { wrapper: createWrapper() });

        await waitFor(() => expect(result.current.isSuccess).toBe(true));
        expect(result.current.data).toEqual(mockMatches);
        expect(matchesApi.getMatches).toHaveBeenCalledWith({});
    });

    it('passes custom params to the API', async () => {
        const { matchesApi } = await import('@/services/matchesApi');
        (matchesApi.getMatches as any).mockResolvedValue({ data: { matches: [] } });

        const params = { status: 'active' as const, min_fit: 70 };
        renderHook(() => useMatches(params), { wrapper: createWrapper() });

        await waitFor(() => expect(matchesApi.getMatches).toHaveBeenCalledWith(params));
    });

    it('handles fetch error', async () => {
        const { matchesApi } = await import('@/services/matchesApi');
        (matchesApi.getMatches as any).mockRejectedValue(new Error('Server error'));

        const { result } = renderHook(() => useMatches(), { wrapper: createWrapper() });

        await waitFor(() => expect(result.current.isError).toBe(true));
        expect(result.current.error).toBeInstanceOf(Error);
    });

    it('returns empty data initially before fetch resolves', () => {
        const { result } = renderHook(() => useMatches(), { wrapper: createWrapper() });
        expect(result.current.data).toBeUndefined();
    });
});

// ---------------------------------------------------------------------------
// usePolicy
// ---------------------------------------------------------------------------

describe('usePolicy', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('fetches policy on mount', async () => {
        const { configApi } = await import('@/services/configApi');
        const mockPolicy = { min_score: 70, preset: 'balanced' };
        (configApi.getPolicy as any).mockResolvedValue({ data: mockPolicy });

        const { result } = renderHook(() => usePolicy(), { wrapper: createWrapper() });

        await waitFor(() => expect(result.current.isLoading).toBe(false));
        expect(result.current.policy).toEqual(mockPolicy);
    });

    it('isLoading is true before policy resolves', () => {
        const { result } = renderHook(() => usePolicy(), { wrapper: createWrapper() });
        expect(result.current.isLoading).toBe(true);
    });

    it('updatePolicy calls configApi.updatePolicy', async () => {
        const { configApi } = await import('@/services/configApi');
        (configApi.getPolicy as any).mockResolvedValue({ data: { min_score: 60 } });
        (configApi.updatePolicy as any).mockResolvedValue({ data: { min_score: 80 } });

        const { result } = renderHook(() => usePolicy(), { wrapper: createWrapper() });
        await waitFor(() => expect(result.current.isLoading).toBe(false));

        act(() => {
            result.current.updatePolicy({ min_score: 80 } as any);
        });

        await waitFor(() => {
            expect(configApi.updatePolicy).toHaveBeenCalledWith({ min_score: 80 });
        });
    });

    it('updates policy cache optimistically before updatePolicy resolves', async () => {
        const { configApi } = await import('@/services/configApi');
        const initialPolicy = {
            min_fit: 55,
            top_k: 50,
            min_jd_required_coverage: null,
            llm_judge_available: true,
            llm_judge_top_n_max: 10,
        };
        const nextPolicy = { min_fit: 0, top_k: 100, min_jd_required_coverage: null };
        (configApi.getPolicy as any).mockResolvedValue({ data: initialPolicy });
        (configApi.updatePolicy as any).mockReturnValue(new Promise(() => undefined));

        const { result } = renderHook(() => usePolicy(), { wrapper: createWrapper() });
        await waitFor(() => expect(result.current.policy).toEqual(initialPolicy));

        act(() => {
            result.current.updatePolicy(nextPolicy);
        });

        await waitFor(() => {
            expect(result.current.policy).toEqual({
                ...initialPolicy,
                ...nextPolicy,
            });
        });
    });

    it('applyPreset calls configApi.applyPreset', async () => {
        const { configApi } = await import('@/services/configApi');
        (configApi.getPolicy as any).mockResolvedValue({ data: {} });
        (configApi.applyPreset as any).mockResolvedValue({ data: {} });

        const { result } = renderHook(() => usePolicy(), { wrapper: createWrapper() });
        await waitFor(() => expect(result.current.isLoading).toBe(false));

        act(() => {
            result.current.applyPreset('strict' as any);
        });

        await waitFor(() => {
            expect(configApi.applyPreset).toHaveBeenCalledWith('strict');
        });
    });

    it('updates policy cache optimistically when applying a preset', async () => {
        const { configApi } = await import('@/services/configApi');
        (configApi.getPolicy as any).mockResolvedValue({
            data: { min_fit: 55, top_k: 50, min_jd_required_coverage: null },
        });
        (configApi.applyPreset as any).mockReturnValue(new Promise(() => undefined));

        const { result } = renderHook(() => usePolicy(), { wrapper: createWrapper() });
        await waitFor(() => expect(result.current.isLoading).toBe(false));

        act(() => {
            result.current.applyPreset('discovery' as any);
        });

        await waitFor(() => {
            expect(result.current.policy).toEqual({
                min_fit: 40,
                top_k: 100,
                min_jd_required_coverage: null,
            });
        });
    });

    it('returns updatePolicy and applyPreset functions', async () => {
        const { configApi } = await import('@/services/configApi');
        (configApi.getPolicy as any).mockResolvedValue({ data: {} });

        const { result } = renderHook(() => usePolicy(), { wrapper: createWrapper() });
        await waitFor(() => expect(result.current.isLoading).toBe(false));

        expect(typeof result.current.updatePolicy).toBe('function');
        expect(typeof result.current.updatePolicyAsync).toBe('function');
        expect(typeof result.current.applyPreset).toBe('function');
    });

    it('handles policy fetch error gracefully', async () => {
        const { configApi } = await import('@/services/configApi');
        (configApi.getPolicy as any).mockRejectedValue(new Error('Not found'));

        const { result } = renderHook(() => usePolicy(), { wrapper: createWrapper() });

        await waitFor(() => expect(result.current.isLoading).toBe(false));
        expect(result.current.policy).toBeUndefined();
    });
});

describe('useCandidatePreferences', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('loads candidate preferences on mount', async () => {
        const { candidatePreferencesApi } = await import('@/services/candidatePreferencesApi');
        const mockPreferences = {
            remote_mode: 'remote',
            target_locations: ['Remote'],
            visa_sponsorship_required: false,
            salary_min: null,
            employment_types: [],
            soft_preferences: 'FastAPI',
            revision: 3,
        };
        vi.mocked(candidatePreferencesApi.getPreferences).mockResolvedValue({
            data: mockPreferences,
        } as never);

        const { result } = renderHook(() => useCandidatePreferences(), {
            wrapper: createWrapper(),
        });

        await waitFor(() => expect(result.current.isLoading).toBe(false));
        expect(result.current.preferences).toEqual(mockPreferences);
        expect(candidatePreferencesApi.getPreferences).toHaveBeenCalledTimes(1);
    });

    it('saves preferences and updates query state', async () => {
        const { candidatePreferencesApi } = await import('@/services/candidatePreferencesApi');
        const initialPreferences = {
            remote_mode: 'any',
            target_locations: [],
            visa_sponsorship_required: false,
            salary_min: null,
            employment_types: [],
            soft_preferences: '',
            revision: 1,
        };
        const updatedPreferences = {
            ...initialPreferences,
            remote_mode: 'remote',
            target_locations: ['Remote'],
            soft_preferences: 'Platform work',
            revision: 2,
        };
        vi.mocked(candidatePreferencesApi.getPreferences)
            .mockResolvedValueOnce({
                data: initialPreferences,
            } as never)
            .mockResolvedValueOnce({
                data: updatedPreferences,
            } as never);
        vi.mocked(candidatePreferencesApi.updatePreferences).mockResolvedValue({
            data: updatedPreferences,
        } as never);

        const { result } = renderHook(() => useCandidatePreferences(), {
            wrapper: createWrapper(),
        });

        await waitFor(() => expect(result.current.isLoading).toBe(false));

        await act(async () => {
            await result.current.savePreferences({
                remote_mode: 'remote',
                target_locations: ['Remote'],
                visa_sponsorship_required: false,
                salary_min: null,
                employment_types: [],
                soft_preferences: 'Platform work',
                preference_mode: 'semantic_rerank',
                preference_rerank_top_n: null,
            });
        });

        expect(candidatePreferencesApi.updatePreferences).toHaveBeenCalledWith({
            remote_mode: 'remote',
            target_locations: ['Remote'],
            visa_sponsorship_required: false,
            salary_min: null,
            employment_types: [],
            soft_preferences: 'Platform work',
            preference_mode: 'semantic_rerank',
            preference_rerank_top_n: null,
        });

        await waitFor(() => expect(result.current.preferences).toEqual(updatedPreferences));
    });
});
