import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';

import { candidatePreferencesApi } from '@/services/candidatePreferencesApi';
import type { CandidatePreferences, CandidatePreferencesUpdateRequest } from '@/types/api';

export const useCandidatePreferences = () => {
    const queryClient = useQueryClient();

    const query = useQuery({
        queryKey: ['candidate-preferences'],
        queryFn: async () => {
            const response = await candidatePreferencesApi.getPreferences();
            return response.data;
        },
    });

    const updateMutation = useMutation({
        mutationFn: (payload: CandidatePreferencesUpdateRequest) =>
            candidatePreferencesApi.updatePreferences(payload),
        onSuccess: (response) => {
            queryClient.setQueryData<CandidatePreferences>(
                ['candidate-preferences'],
                response.data,
            );
            queryClient.invalidateQueries({ queryKey: ['candidate-preferences'] });
        },
    });

    return {
        preferences: query.data,
        isLoading: query.isLoading,
        isSaving: updateMutation.isPending,
        savePreferences: updateMutation.mutateAsync,
    };
};
