import { beforeEach, describe, expect, it, vi } from 'vitest';

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

describe('notificationSettingsApi', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('getSettings calls GET /v1/notification-settings', async () => {
        const expected = { data: { revision: 1 } };
        mockGet.mockResolvedValue(expected);
        const { notificationSettingsApi } = await import('../notificationSettingsApi');

        const result = await notificationSettingsApi.getSettings();

        expect(mockGet).toHaveBeenCalledWith('/v1/notification-settings');
        expect(result).toEqual(expected);
    });

    it('updateSettings calls PUT /v1/notification-settings with payload', async () => {
        const payload = {
            notifications_enabled: true,
            min_score_threshold: 80,
            notify_on_new_match: true,
            notify_on_batch_complete: false,
            channels: {
                in_app: { enabled: true },
            },
        };
        const expected = { data: { revision: 2 } };
        mockPut.mockResolvedValue(expected);
        const { notificationSettingsApi } = await import('../notificationSettingsApi');

        const result = await notificationSettingsApi.updateSettings(payload as never);

        expect(mockPut).toHaveBeenCalledWith('/v1/notification-settings', payload);
        expect(result).toEqual(expected);
    });

    it('sendTest calls POST /v1/notification-settings/test with payload', async () => {
        const payload = { channel_type: 'in_app' };
        const expected = { data: { success: true, message: 'Queued', notification_id: 'notif-123' } };
        mockPost.mockResolvedValue(expected);
        const { notificationSettingsApi } = await import('../notificationSettingsApi');

        const result = await notificationSettingsApi.sendTest(payload as never);

        expect(mockPost).toHaveBeenCalledWith('/v1/notification-settings/test', payload);
        expect(result).toEqual(expected);
    });
});
