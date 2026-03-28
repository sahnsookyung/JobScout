import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';

import { notificationSettingsApi } from '@/services/notificationSettingsApi';
import type {
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

    return {
        settings: query.data,
        isLoading: query.isLoading,
        isSaving: updateMutation.isPending,
        isTesting: testMutation.isPending,
        saveSettings: updateMutation.mutateAsync,
        sendTest: testMutation.mutateAsync,
    };
};
