import { beforeEach, describe, expect, it, vi } from 'vitest';

const mockGet = vi.fn();
const mockPut = vi.fn();

vi.mock('@/services/api', () => ({
    apiClient: {
        get: mockGet,
        put: mockPut,
    },
}));

describe('candidatePreferencesApi', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('uses the candidate preferences API routes', async () => {
        const payload = {
            remote_mode: 'hybrid' as const,
            target_locations: ['Tokyo'],
            visa_sponsorship_required: true,
            salary_min: 12000000,
            employment_types: ['full-time'],
            soft_preferences: 'Platform roles with developer tooling.',
            preference_mode: 'semantic_rerank' as const,
            preference_rerank_top_n: 25,
        };
        mockGet.mockResolvedValueOnce({ data: { role_titles: [] } });
        mockPut.mockResolvedValueOnce({ data: payload });
        const { candidatePreferencesApi } = await import('../candidatePreferencesApi');

        await candidatePreferencesApi.getPreferences();
        await candidatePreferencesApi.updatePreferences(payload);

        expect(mockGet).toHaveBeenCalledWith('/v1/candidate-preferences');
        expect(mockPut).toHaveBeenCalledWith('/v1/candidate-preferences', payload);
    });
});
