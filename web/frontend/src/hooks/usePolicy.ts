import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { configApi } from '@/services/configApi';
import type { PolicyConfig, PolicyPreset, PolicyUpdatePayload } from '@/types/api';
import { POLICY_PRESET_VALUES } from '@/utils/constants';

interface PolicyMutationContext {
    hadPreviousPolicy: boolean;
    previousPolicy?: PolicyConfig;
}

const policyQueryKey = ['policy'] as const;

export const usePolicy = () => {
    const queryClient = useQueryClient();

    const query = useQuery({
        queryKey: policyQueryKey,
        queryFn: async () => {
            const response = await configApi.getPolicy();
            return response.data;
        },
    });

    const updateMutation = useMutation({
        mutationFn: (policy: PolicyUpdatePayload) => configApi.updatePolicy(policy),
        onMutate: async (nextPolicy): Promise<PolicyMutationContext> => {
            await queryClient.cancelQueries({ queryKey: policyQueryKey });
            const previousPolicy = queryClient.getQueryData<PolicyConfig>(policyQueryKey);
            queryClient.setQueryData<PolicyConfig>(policyQueryKey, nextPolicy);
            return { hadPreviousPolicy: previousPolicy !== undefined, previousPolicy };
        },
        onError: (_error, _nextPolicy, context) => {
            if (context?.hadPreviousPolicy) {
                queryClient.setQueryData<PolicyConfig>(policyQueryKey, context.previousPolicy);
            } else {
                queryClient.removeQueries({ queryKey: policyQueryKey });
            }
        },
        onSuccess: (response) => {
            queryClient.setQueryData<PolicyConfig>(policyQueryKey, response.data);
            queryClient.invalidateQueries({ queryKey: ['matches'] });
            queryClient.invalidateQueries({ queryKey: ['stats'] });
        },
        onSettled: () => {
            queryClient.invalidateQueries({ queryKey: policyQueryKey });
        },
    });

    const presetMutation = useMutation({
        mutationFn: (preset: PolicyPreset) => configApi.applyPreset(preset),
        onMutate: async (preset): Promise<PolicyMutationContext> => {
            await queryClient.cancelQueries({ queryKey: policyQueryKey });
            const previousPolicy = queryClient.getQueryData<PolicyConfig>(policyQueryKey);
            queryClient.setQueryData<PolicyConfig>(policyQueryKey, {
                ...(previousPolicy ?? {}),
                ...POLICY_PRESET_VALUES[preset],
            });
            return { hadPreviousPolicy: previousPolicy !== undefined, previousPolicy };
        },
        onError: (_error, _preset, context) => {
            if (context?.hadPreviousPolicy) {
                queryClient.setQueryData<PolicyConfig>(policyQueryKey, context.previousPolicy);
            } else {
                queryClient.removeQueries({ queryKey: policyQueryKey });
            }
        },
        onSuccess: (response) => {
            queryClient.setQueryData<PolicyConfig>(policyQueryKey, response.data);
            queryClient.invalidateQueries({ queryKey: ['matches'] });
            queryClient.invalidateQueries({ queryKey: ['stats'] });
        },
        onSettled: () => {
            queryClient.invalidateQueries({ queryKey: policyQueryKey });
        },
    });

    return {
        policy: query.data,
        isLoading: query.isLoading,
        updatePolicy: updateMutation.mutate,
        applyPreset: presetMutation.mutate,
    };
};
