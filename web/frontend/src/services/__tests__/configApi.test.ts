/**
 * Tests for configApi
 * Covers: web/frontend/src/services/configApi.ts
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';

// Mock the api module
const mockGet = vi.fn();
const mockPut = vi.fn();
const mockPost = vi.fn();

vi.mock('@/services/api', () => ({
    apiClient: {
        get: mockGet,
        put: mockPut,
        post: mockPost,
    },
}));

describe('configApi', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('getScoringWeights calls GET /config/scoring-weights', async () => {
        mockGet.mockResolvedValue({ data: { fit: 0.6, want: 0.4 } });
        const { configApi } = await import('../configApi');

        await configApi.getScoringWeights();

        expect(mockGet).toHaveBeenCalledWith('/config/scoring-weights');
    });

    it('getPolicy calls GET /v1/policy', async () => {
        mockGet.mockResolvedValue({ data: { min_score: 60 } });
        const { configApi } = await import('../configApi');

        await configApi.getPolicy();

        expect(mockGet).toHaveBeenCalledWith('/v1/policy');
    });

    it('updatePolicy calls PUT /v1/policy with policy body', async () => {
        const policy = { min_score: 70, preset: 'balanced' };
        mockPut.mockResolvedValue({ data: policy });
        const { configApi } = await import('../configApi');

        await configApi.updatePolicy(policy as any);

        expect(mockPut).toHaveBeenCalledWith('/v1/policy', policy);
    });

    it('applyPreset calls POST /v1/policy/preset/:preset', async () => {
        mockPost.mockResolvedValue({ data: { preset: 'strict' } });
        const { configApi } = await import('../configApi');

        await configApi.applyPreset('strict' as any);

        expect(mockPost).toHaveBeenCalledWith('/v1/policy/preset/strict');
    });

    it('applyPreset uses the preset value in the URL path', async () => {
        mockPost.mockResolvedValue({ data: {} });
        const { configApi } = await import('../configApi');

        await configApi.applyPreset('discovery' as any);

        expect(mockPost).toHaveBeenCalledWith('/v1/policy/preset/discovery');
    });

    it('getScoringWeights returns the API response', async () => {
        const expected = { fit: 0.5, want: 0.5 };
        mockGet.mockResolvedValue(expected);
        const { configApi } = await import('../configApi');

        const result = await configApi.getScoringWeights();

        expect(result).toEqual(expected);
    });

    it('updatePolicy returns the updated policy', async () => {
        const policy = { min_score: 80 };
        const expected = { data: policy };
        mockPut.mockResolvedValue(expected);
        const { configApi } = await import('../configApi');

        const result = await configApi.updatePolicy(policy as any);

        expect(result).toEqual(expected);
    });
});
