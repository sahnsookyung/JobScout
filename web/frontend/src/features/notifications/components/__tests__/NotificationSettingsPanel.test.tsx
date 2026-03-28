import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { vi } from 'vitest';
import { toast } from 'sonner';

import { NotificationSettingsPanel } from '../NotificationSettingsPanel';
import { useNotificationSettings } from '@/hooks/useNotificationSettings';

vi.mock('@/hooks/useNotificationSettings');
vi.mock('sonner');
vi.mock('lucide-react', () => ({
    BellRing: () => <svg data-testid="bell-icon" />,
    Send: () => <svg data-testid="send-icon" />,
}));

const mockUseNotificationSettings = vi.mocked(useNotificationSettings);

const createWrapper = () => {
    const queryClient = new QueryClient({
        defaultOptions: { queries: { retry: false } },
    });
    return ({ children }: { children: React.ReactNode }) => (
        <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );
};

describe('NotificationSettingsPanel', () => {
    const saveSettings = vi.fn();
    const sendTest = vi.fn();

    beforeEach(() => {
        vi.clearAllMocks();
        mockUseNotificationSettings.mockReturnValue({
            settings: {
                notifications_enabled: true,
                min_score_threshold: 70,
                notify_on_new_match: true,
                notify_on_batch_complete: false,
                revision: 2,
                channels: {
                    email: {
                        enabled: true,
                        configured: true,
                        available: true,
                        masked_recipient: '***@example.com',
                        availability_reason: null,
                        last_test_status: null,
                        last_tested_at: null,
                        last_test_error: null,
                    },
                    discord: {
                        enabled: false,
                        configured: true,
                        available: true,
                        masked_recipient: 'https://discord.com/api/webhooks/...',
                        availability_reason: null,
                        last_test_status: 'queued',
                        last_tested_at: null,
                        last_test_error: null,
                    },
                },
            },
            isLoading: false,
            isSaving: false,
            isTesting: false,
            saveSettings,
            sendTest,
        });
        saveSettings.mockResolvedValue({ data: {} });
        sendTest.mockResolvedValue({ data: { message: 'Queued test notification' } });
    });

    it('renders saved channel state', () => {
        render(<NotificationSettingsPanel />, { wrapper: createWrapper() });

        expect(screen.getByText('Notifications')).toBeInTheDocument();
        expect(screen.getByText('***@example.com')).toBeInTheDocument();
        expect(screen.getByText('https://discord.com/api/webhooks/...')).toBeInTheDocument();
    });

    it('saves edited settings explicitly', async () => {
        render(<NotificationSettingsPanel />, { wrapper: createWrapper() });

        const threshold = screen.getByDisplayValue('70');
        await userEvent.clear(threshold);
        await userEvent.type(threshold, '82');

        const passwordInputs = screen.getAllByPlaceholderText(/paste/i);
        await userEvent.type(passwordInputs[0], 'https://discord.com/api/webhooks/test');

        await userEvent.click(screen.getByRole('button', { name: /save settings/i }));

        await waitFor(() => {
            expect(saveSettings).toHaveBeenCalledWith({
                notifications_enabled: true,
                min_score_threshold: 82,
                notify_on_new_match: true,
                notify_on_batch_complete: false,
                channels: {
                    email: { enabled: true },
                    discord: {
                        enabled: false,
                        secret_value: 'https://discord.com/api/webhooks/test',
                    },
                },
            });
        });
        expect(toast.success).toHaveBeenCalledWith('Notification settings saved');
    });

    it('disables test actions while there are unsaved changes', async () => {
        render(<NotificationSettingsPanel />, { wrapper: createWrapper() });

        const threshold = screen.getByDisplayValue('70');
        await userEvent.clear(threshold);
        await userEvent.type(threshold, '81');

        const testButtons = screen.getAllByRole('button', { name: /test/i });
        expect(testButtons[0]).toBeDisabled();
        expect(testButtons[1]).toBeDisabled();
    });

    it('queues a test notification when settings are clean', async () => {
        render(<NotificationSettingsPanel />, { wrapper: createWrapper() });

        const testButtons = screen.getAllByRole('button', { name: /test/i });
        await userEvent.click(testButtons[0]);

        await waitFor(() => {
            expect(sendTest).toHaveBeenCalledWith({ channel_type: 'email' });
        });
        expect(toast.success).toHaveBeenCalledWith('Queued test notification');
    });
});
