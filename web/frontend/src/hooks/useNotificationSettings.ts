import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';

import { notificationSettingsApi } from '@/services/notificationSettingsApi';
import type {
    NotificationEmailOverrideRequest,
    NotificationSettingsTestRequest,
    NotificationSettingsUpdateRequest,
} from '@/types/api';

export const useNotificationSettings = () => {
    const queryClient = useQueryClient();

    const query = useQuery({
        queryKey: ['notification-settings'],
        queryFn: async () => {
            const response = await notificationSettingsApi.getSettings();
            return response.data;
        },
    });

    const updateMutation = useMutation({
        mutationFn: (payload: NotificationSettingsUpdateRequest) =>
            notificationSettingsApi.updateSettings(payload),
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ['notification-settings'] });
        },
    });

    const testMutation = useMutation({
        mutationFn: (payload: NotificationSettingsTestRequest) =>
            notificationSettingsApi.sendTest(payload),
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ['notification-settings'] });
        },
    });

    const emailOverrideMutation = useMutation({
        mutationFn: (payload: NotificationEmailOverrideRequest) =>
            notificationSettingsApi.sendEmailOverrideVerification(payload),
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ['notification-settings'] });
        },
    });

    const clearEmailOverrideMutation = useMutation({
        mutationFn: () => notificationSettingsApi.clearEmailOverride(),
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ['notification-settings'] });
        },
    });

    const verifyEmailOverride = async (token: string) => {
        const response = await notificationSettingsApi.verifyEmailOverride({ token });
        await queryClient.invalidateQueries({ queryKey: ['notification-settings'] });
        return response;
    };

    return {
        settings: query.data,
        isLoading: query.isLoading,
        isError: query.isError,
        error: query.error,
        refetch: query.refetch,
        isSaving: updateMutation.isPending,
        isTesting: testMutation.isPending,
        isSendingEmailVerification: emailOverrideMutation.isPending,
        isClearingEmailOverride: clearEmailOverrideMutation.isPending,
        saveSettings: updateMutation.mutateAsync,
        sendTest: testMutation.mutateAsync,
        sendEmailOverrideVerification: emailOverrideMutation.mutateAsync,
        clearEmailOverride: clearEmailOverrideMutation.mutateAsync,
        verifyEmailOverride,
    };
};
