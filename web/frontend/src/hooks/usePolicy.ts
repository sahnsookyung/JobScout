import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { configApi } from '@/services/configApi';
import type { PolicyConfig, PolicyPreset } from '@/types/api';

export const usePolicy = () => {
    const queryClient = useQueryClient();

    const query = useQuery({
        queryKey: ['policy'],
        queryFn: async () => {
            const response = await configApi.getPolicy();
            return response.data;
        },
    });

    const updateMutation = useMutation({
        mutationFn: (policy: PolicyConfig) => configApi.updatePolicy(policy),
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ['policy'] });
            queryClient.invalidateQueries({ queryKey: ['matches'] });
        },
    });

    const presetMutation = useMutation({
        mutationFn: (preset: PolicyPreset) => configApi.applyPreset(preset),
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ['policy'] });
            queryClient.invalidateQueries({ queryKey: ['matches'] });
        },
    });

    return {
        policy: query.data,
        isLoading: query.isLoading,
        updatePolicy: updateMutation.mutate,
        applyPreset: presetMutation.mutate,
    };
};
