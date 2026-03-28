import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { vi } from 'vitest';
import { toast } from 'sonner';

import { NotificationSettingsPanel } from '../NotificationSettingsPanel';
import { useNotificationSettings } from '@/hooks/useNotificationSettings';

vi.mock('@/hooks/useNotificationSettings');
vi.mock('sonner');

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

        expect(screen.getByText('Alert rules')).toBeInTheDocument();
        expect(screen.getByText('***@example.com')).toBeInTheDocument();
        expect(screen.getByText('https://discord.com/api/webhooks/...')).toBeInTheDocument();
        expect(screen.queryByText('In-App')).not.toBeInTheDocument();
        expect(screen.queryByText('Webhook')).not.toBeInTheDocument();
    });

    it('saves edited settings explicitly', async () => {
        render(<NotificationSettingsPanel />, { wrapper: createWrapper() });

        const threshold = screen.getByLabelText('Minimum score threshold');
        fireEvent.change(threshold, { target: { value: '75' } });

        const passwordInput = screen.getByPlaceholderText(/paste discord destination/i);
        await userEvent.type(passwordInput, 'https://discord.com/api/webhooks/test');

        await userEvent.click(screen.getByRole('button', { name: /save settings/i }));

        await waitFor(() => {
            expect(saveSettings).toHaveBeenCalledWith({
                notifications_enabled: true,
                min_score_threshold: 75,
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

        const threshold = screen.getByLabelText('Minimum score threshold');
        fireEvent.change(threshold, { target: { value: '71' } });

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

    it('clears a saved secret explicitly', async () => {
        render(<NotificationSettingsPanel />, { wrapper: createWrapper() });

        await userEvent.click(screen.getByRole('button', { name: /clear/i }));
        await userEvent.click(screen.getByRole('button', { name: /save settings/i }));

        await waitFor(() => {
            expect(saveSettings).toHaveBeenCalledWith({
                notifications_enabled: true,
                min_score_threshold: 70,
                notify_on_new_match: true,
                notify_on_batch_complete: false,
                channels: {
                    email: { enabled: true },
                    discord: {
                        enabled: false,
                        secret_value: null,
                    },
                },
            });
        });
    });

    it('shows unavailable channel messaging and disables enabling it', () => {
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
                    telegram: {
                        enabled: false,
                        configured: false,
                        available: false,
                        masked_recipient: null,
                        availability_reason: 'Telegram bot credentials are not configured',
                        last_test_status: null,
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

        render(<NotificationSettingsPanel />, { wrapper: createWrapper() });

        expect(screen.getByText('Telegram bot credentials are not configured')).toBeInTheDocument();
        expect(screen.getByRole('checkbox', { name: 'Enable Telegram' })).toBeDisabled();
        const testButtons = screen.getAllByRole('button', { name: /test/i });
        expect(testButtons[1]).toBeDisabled();
    });

    it('shows a save error toast', async () => {
        saveSettings.mockRejectedValueOnce(new Error('Save exploded'));
        render(<NotificationSettingsPanel />, { wrapper: createWrapper() });

        await userEvent.click(screen.getByRole('button', { name: /save settings/i }));

        await waitFor(() => {
            expect(toast.error).toHaveBeenCalledWith('Save exploded');
        });
    });

    it('shows a test error toast', async () => {
        sendTest.mockRejectedValueOnce(new Error('Test exploded'));
        render(<NotificationSettingsPanel />, { wrapper: createWrapper() });

        const testButtons = screen.getAllByRole('button', { name: /test/i });
        await userEvent.click(testButtons[0]);

        await waitFor(() => {
            expect(toast.error).toHaveBeenCalledWith('Test exploded');
        });
    });

    it('renders a loading skeleton while settings are loading', () => {
        mockUseNotificationSettings.mockReturnValue({
            settings: undefined,
            isLoading: true,
            isSaving: false,
            isTesting: false,
            saveSettings,
            sendTest,
        });

        const { container } = render(<NotificationSettingsPanel />, { wrapper: createWrapper() });

        expect(container.querySelectorAll('.animate-pulse')).toHaveLength(3);
    });
});
