import { apiClient } from './api';
import type { ScoringWeights, PolicyConfig, PolicyPreset } from '@/types/api';

export const configApi = {
    getScoringWeights: () =>
        apiClient.get<ScoringWeights>('/config/scoring-weights'),

    getPolicy: () => apiClient.get<PolicyConfig>('/v1/policy'),

    updatePolicy: (policy: PolicyConfig) =>
        apiClient.put<PolicyConfig>('/v1/policy', policy),

    applyPreset: (preset: PolicyPreset) =>
        apiClient.post<PolicyConfig>(`/v1/policy/preset/${preset}`),
};
