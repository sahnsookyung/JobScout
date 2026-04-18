import { apiClient } from '@/services/api';
import type {
    NotificationEmailOverrideRequest,
    NotificationEmailVerificationRequest,
    NotificationEmailOverrideResponse,
    NotificationSettings,
    NotificationSettingsTestRequest,
    NotificationSettingsTestResponse,
    NotificationSettingsUpdateRequest,
} from '@/types/api';

export const notificationSettingsApi = {
    getSettings() {
        return apiClient.get<NotificationSettings>('/v1/notification-settings');
    },

    updateSettings(payload: NotificationSettingsUpdateRequest) {
        return apiClient.put<NotificationSettings>('/v1/notification-settings', payload);
    },

    sendTest(payload: NotificationSettingsTestRequest) {
        return apiClient.post<NotificationSettingsTestResponse>('/v1/notification-settings/test', payload);
    },

    sendEmailOverrideVerification(payload: NotificationEmailOverrideRequest) {
        return apiClient.post<NotificationEmailOverrideResponse>(
            '/v1/notification-settings/email/override',
            payload,
        );
    },

    verifyEmailOverride(payload: NotificationEmailVerificationRequest) {
        return apiClient.post<NotificationEmailOverrideResponse>(
            '/v1/notification-settings/email/verify',
            payload,
        );
    },

    clearEmailOverride() {
        return apiClient.delete<NotificationEmailOverrideResponse>('/v1/notification-settings/email/override');
    },
};
