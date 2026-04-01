import { apiClient } from '@/services/api';
import type {
    CandidatePreferences,
    CandidatePreferencesUpdateRequest,
} from '@/types/api';

export const candidatePreferencesApi = {
    getPreferences() {
        return apiClient.get<CandidatePreferences>('/v1/candidate-preferences');
    },

    updatePreferences(payload: CandidatePreferencesUpdateRequest) {
        return apiClient.put<CandidatePreferences>('/v1/candidate-preferences', payload);
    },
};
