/**
 * Tests for matchesApi
 * Covers: web/frontend/src/services/matchesApi.ts
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';

const mockGet = vi.fn();
const mockPost = vi.fn();

vi.mock('@/services/api', () => ({
    apiClient: {
        get: mockGet,
        post: mockPost,
    },
}));

describe('matchesApi', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('getMatches calls GET /matches with default empty params', async () => {
        mockGet.mockResolvedValue({ data: { matches: [] } });
        const { matchesApi } = await import('../matchesApi');

        await matchesApi.getMatches();

        expect(mockGet).toHaveBeenCalledWith('/matches', { params: {} });
    });

    it('getMatches passes query params to GET /matches', async () => {
        mockGet.mockResolvedValue({ data: { matches: [] } });
        const { matchesApi } = await import('../matchesApi');

        const params = { status: 'active' as const, min_fit: 70 };
        await matchesApi.getMatches(params);

        expect(mockGet).toHaveBeenCalledWith('/matches', { params });
    });

    it('getMatchDetails calls GET /matches/:id', async () => {
        const mockDetail = { id: 'match-1', overall_score: 90 };
        mockGet.mockResolvedValue({ data: mockDetail });
        const { matchesApi } = await import('../matchesApi');

        const result = await matchesApi.getMatchDetails('match-1');

        expect(mockGet).toHaveBeenCalledWith('/matches/match-1');
        expect(result).toEqual({ data: mockDetail });
    });

    it('getMatchExplanation calls GET /matches/:id/explanation', async () => {
        mockGet.mockResolvedValue({ data: { explanation: 'Strong match' } });
        const { matchesApi } = await import('../matchesApi');

        await matchesApi.getMatchExplanation('match-2');

        expect(mockGet).toHaveBeenCalledWith('/matches/match-2/explanation');
    });

    it('getStats calls GET /stats', async () => {
        const mockStats = { total_matches: 50 };
        mockGet.mockResolvedValue({ data: mockStats });
        const { matchesApi } = await import('../matchesApi');

        const result = await matchesApi.getStats();

        expect(mockGet).toHaveBeenCalledWith('/stats');
        expect(result).toEqual({ data: mockStats });
    });

    it('toggleHidden calls POST /matches/:id/hide', async () => {
        mockPost.mockResolvedValue({ data: { success: true, match_id: 'match-3', is_hidden: true } });
        const { matchesApi } = await import('../matchesApi');

        await matchesApi.toggleHidden('match-3');

        expect(mockPost).toHaveBeenCalledWith('/matches/match-3/hide');
    });

    it('toggleHidden returns the response', async () => {
        const mockResponse = { data: { success: true, match_id: 'match-4', is_hidden: false } };
        mockPost.mockResolvedValue(mockResponse);
        const { matchesApi } = await import('../matchesApi');

        const result = await matchesApi.toggleHidden('match-4');

        expect(result).toEqual(mockResponse);
    });

    it('getMatches returns the API response', async () => {
        const expected = { data: { matches: [{ id: '1' }], total: 1 } };
        mockGet.mockResolvedValue(expected);
        const { matchesApi } = await import('../matchesApi');

        const result = await matchesApi.getMatches();

        expect(result).toEqual(expected);
    });

    it('getMatchExplanation returns the API response', async () => {
        const expected = { data: { explanation: 'Great fit' } };
        mockGet.mockResolvedValue(expected);
        const { matchesApi } = await import('../matchesApi');

        const result = await matchesApi.getMatchExplanation('match-5');

        expect(result).toEqual(expected);
    });
});
