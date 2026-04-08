import { renderHook, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { vi } from 'vitest';

import { useNotificationSettings } from '../useNotificationSettings';
import { notificationSettingsApi } from '@/services/notificationSettingsApi';

vi.mock('@/services/notificationSettingsApi', () => ({
    notificationSettingsApi: {
        getSettings: vi.fn(),
        updateSettings: vi.fn(),
        sendTest: vi.fn(),
    },
}));

const createWrapper = () => {
    const queryClient = new QueryClient({
        defaultOptions: {
            queries: { retry: false },
            mutations: { retry: false },
        },
    });

    return ({ children }: { children: React.ReactNode }) => (
        <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );
};

describe('useNotificationSettings', () => {
    const settings = {
        notifications_enabled: true,
        min_fit_for_alerts: 70,
        notify_on_new_match: true,
        notify_on_batch_complete: false,
        revision: 1,
        channels: {
            email: {
                enabled: true,
                configured: true,
                available: true,
                availability_reason: null,
                masked_recipient: '***@example.com',
                last_test_status: null,
                last_tested_at: null,
                last_test_error: null,
            },
        },
    };

    beforeEach(() => {
        vi.clearAllMocks();
        vi.mocked(notificationSettingsApi.getSettings).mockResolvedValue({ data: settings } as never);
        vi.mocked(notificationSettingsApi.updateSettings).mockResolvedValue({ data: settings } as never);
        vi.mocked(notificationSettingsApi.sendTest).mockResolvedValue({
            data: { success: true, notification_id: 'notif-1', message: 'Queued test notification' },
        } as never);
    });

    it('loads notification settings', async () => {
        const { result } = renderHook(() => useNotificationSettings(), { wrapper: createWrapper() });

        await waitFor(() => expect(result.current.isLoading).toBe(false));

        expect(result.current.settings).toEqual(settings);
        expect(notificationSettingsApi.getSettings).toHaveBeenCalledTimes(1);
    });

    it('saves settings through the API', async () => {
        const { result } = renderHook(() => useNotificationSettings(), { wrapper: createWrapper() });
        await waitFor(() => expect(result.current.isLoading).toBe(false));

        await result.current.saveSettings({
            notifications_enabled: false,
        min_fit_for_alerts: 82,
            notify_on_new_match: false,
            notify_on_batch_complete: true,
            channels: {
                email: { enabled: true },
            },
        });

        expect(notificationSettingsApi.updateSettings).toHaveBeenCalledWith({
            notifications_enabled: false,
        min_fit_for_alerts: 82,
            notify_on_new_match: false,
            notify_on_batch_complete: true,
            channels: {
                email: { enabled: true },
            },
        });
    });

    it('sends a test notification through the API', async () => {
        const { result } = renderHook(() => useNotificationSettings(), { wrapper: createWrapper() });
        await waitFor(() => expect(result.current.isLoading).toBe(false));

        await result.current.sendTest({ channel_type: 'email' });

        expect(notificationSettingsApi.sendTest).toHaveBeenCalledWith({ channel_type: 'email' });
    });
});
