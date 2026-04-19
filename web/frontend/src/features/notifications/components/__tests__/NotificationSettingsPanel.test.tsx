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
    const sendEmailOverrideVerification = vi.fn();
    const clearEmailOverride = vi.fn();
    const verifyEmailOverride = vi.fn();

    beforeEach(() => {
        vi.clearAllMocks();
        mockUseNotificationSettings.mockReturnValue({
            settings: {
                notifications_enabled: true,
                min_fit_for_alerts: 70,
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
            isSendingEmailVerification: false,
            isClearingEmailOverride: false,
            saveSettings,
            sendTest,
            sendEmailOverrideVerification,
            clearEmailOverride,
            verifyEmailOverride,
        });
        saveSettings.mockResolvedValue({ data: {} });
        sendTest.mockResolvedValue({ data: { message: 'Queued test notification' } });
        sendEmailOverrideVerification.mockResolvedValue({ data: { message: 'Verification email sent' } });
        clearEmailOverride.mockResolvedValue({ data: { message: 'Email override cleared' } });
        verifyEmailOverride.mockResolvedValue({ data: { message: 'Email override verified' } });
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

        const threshold = screen.getByLabelText('Minimum fit for alerts');
        fireEvent.change(threshold, { target: { value: '75' } });

        const passwordInput = screen.getByPlaceholderText(/paste discord destination/i);
        await userEvent.type(passwordInput, 'https://discord.com/api/webhooks/test');

        await userEvent.click(screen.getByRole('button', { name: /save settings/i }));

        await waitFor(() => {
            expect(saveSettings).toHaveBeenCalledWith({
                notifications_enabled: true,
                min_fit_for_alerts: 75,
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
        expect(toast.success).toHaveBeenCalledWith('Notification settings saved.');
    });

    it('disables test actions while there are unsaved changes', async () => {
        render(<NotificationSettingsPanel />, { wrapper: createWrapper() });

        const threshold = screen.getByLabelText('Minimum fit for alerts');
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
                min_fit_for_alerts: 70,
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
                min_fit_for_alerts: 70,
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
            isSendingEmailVerification: false,
            isClearingEmailOverride: false,
            saveSettings,
            sendTest,
            sendEmailOverrideVerification,
            clearEmailOverride,
            verifyEmailOverride,
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

        fireEvent.change(screen.getByLabelText('Minimum fit for alerts'), { target: { value: '72' } });
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

    it('sends and clears an email override', async () => {
        mockUseNotificationSettings.mockReturnValue({
            settings: {
                notifications_enabled: true,
                min_fit_for_alerts: 70,
                notify_on_new_match: true,
                notify_on_batch_complete: false,
                revision: 2,
                channels: {
                    email: {
                        enabled: true,
                        configured: true,
                        available: true,
                        masked_recipient: '***@example.com',
                        effective_recipient: 'ada+alerts@example.com',
                        override_address: 'ada+alerts@example.com',
                        override_status: 'pending',
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
            isSendingEmailVerification: false,
            isClearingEmailOverride: false,
            saveSettings,
            sendTest,
            sendEmailOverrideVerification,
            clearEmailOverride,
            verifyEmailOverride,
        });

        render(<NotificationSettingsPanel />, { wrapper: createWrapper() });

        await userEvent.clear(screen.getByPlaceholderText('name@example.com'));
        await userEvent.type(screen.getByPlaceholderText('name@example.com'), 'new@example.com');
        await userEvent.click(screen.getByRole('button', { name: /send verification/i }));
        await userEvent.click(screen.getByRole('button', { name: /clear override/i }));

        await waitFor(() => {
            expect(sendEmailOverrideVerification).toHaveBeenCalledWith({ address: 'new@example.com' });
        });
        await waitFor(() => {
            expect(clearEmailOverride).toHaveBeenCalledTimes(1);
        });
        expect(screen.getByText(/pending/i)).toBeInTheDocument();
    });

    it('renders verified and expired email helper copy', () => {
        const { rerender } = render(<NotificationSettingsPanel />, { wrapper: createWrapper() });

        mockUseNotificationSettings.mockReturnValue({
            settings: {
                notifications_enabled: true,
                min_fit_for_alerts: 70,
                notify_on_new_match: true,
                notify_on_batch_complete: false,
                revision: 3,
                channels: {
                    email: {
                        enabled: true,
                        configured: true,
                        available: true,
                        masked_recipient: '***@example.com',
                        override_address: 'ada@example.com',
                        override_status: 'verified',
                        availability_reason: null,
                        last_test_status: null,
                        last_tested_at: null,
                        last_test_error: null,
                    },
                },
            },
            isLoading: false,
            isSaving: false,
            isTesting: false,
            isSendingEmailVerification: false,
            isClearingEmailOverride: false,
            saveSettings,
            sendTest,
            sendEmailOverrideVerification,
            clearEmailOverride,
            verifyEmailOverride,
        });

        rerender(<NotificationSettingsPanel />);
        expect(screen.getByText('Verified override')).toBeInTheDocument();

        mockUseNotificationSettings.mockReturnValue({
            settings: {
                notifications_enabled: true,
                min_fit_for_alerts: 70,
                notify_on_new_match: true,
                notify_on_batch_complete: false,
                revision: 4,
                channels: {
                    email: {
                        enabled: true,
                        configured: true,
                        available: true,
                        masked_recipient: '***@example.com',
                        override_address: 'ada@example.com',
                        override_status: 'expired',
                        availability_reason: null,
                        last_test_status: null,
                        last_tested_at: null,
                        last_test_error: null,
                    },
                },
            },
            isLoading: false,
            isSaving: false,
            isTesting: false,
            isSendingEmailVerification: false,
            isClearingEmailOverride: false,
            saveSettings,
            sendTest,
            sendEmailOverrideVerification,
            clearEmailOverride,
            verifyEmailOverride,
        });

        rerender(<NotificationSettingsPanel />);
        expect(screen.getByText('Expired — resend')).toBeInTheDocument();
    });

    it('renders a loading skeleton while settings are loading', () => {
        mockUseNotificationSettings.mockReturnValue({
            settings: undefined,
            isLoading: true,
            isSaving: false,
            isTesting: false,
            isSendingEmailVerification: false,
            isClearingEmailOverride: false,
            saveSettings,
            sendTest,
            sendEmailOverrideVerification,
            clearEmailOverride,
            verifyEmailOverride,
        });

        const { container } = render(<NotificationSettingsPanel />, { wrapper: createWrapper() });

        expect(container.querySelectorAll('.animate-pulse')).toHaveLength(3);
    });

    it('renders email fallback copy and blocks unavailable email toggles', () => {
        mockUseNotificationSettings.mockReturnValue({
            settings: {
                notifications_enabled: true,
                min_fit_for_alerts: 70,
                notify_on_new_match: true,
                notify_on_batch_complete: false,
                revision: 5,
                channels: {
                    email: {
                        enabled: false,
                        configured: false,
                        available: false,
                        masked_recipient: null,
                        override_address: null,
                        override_status: 'none',
                        availability_reason: 'Email delivery is disabled for this account',
                        last_test_status: null,
                        last_tested_at: null,
                        last_test_error: null,
                    },
                },
            },
            isLoading: false,
            isSaving: false,
            isTesting: false,
            isSendingEmailVerification: false,
            isClearingEmailOverride: false,
            saveSettings,
            sendTest,
            sendEmailOverrideVerification,
            clearEmailOverride,
            verifyEmailOverride,
        });

        render(<NotificationSettingsPanel />, { wrapper: createWrapper() });

        expect(screen.getByText('No email configured')).toBeInTheDocument();
        expect(screen.getByText('Email delivery is disabled for this account')).toBeInTheDocument();
        expect(screen.getByRole('checkbox', { name: 'Enable Email' })).toBeDisabled();
        expect(screen.getByRole('button', { name: /test/i })).toBeDisabled();
    });

    it('shows default error toasts for non-error verification failures', async () => {
        mockUseNotificationSettings.mockReturnValue({
            settings: {
                notifications_enabled: true,
                min_fit_for_alerts: 70,
                notify_on_new_match: true,
                notify_on_batch_complete: false,
                revision: 6,
                channels: {
                    email: {
                        enabled: true,
                        configured: true,
                        available: true,
                        masked_recipient: '***@example.com',
                        effective_recipient: 'ada@example.com',
                        override_address: 'ada@example.com',
                        override_status: 'pending',
                        availability_reason: null,
                        last_test_status: null,
                        last_tested_at: null,
                        last_test_error: null,
                    },
                },
            },
            isLoading: false,
            isSaving: false,
            isTesting: false,
            isSendingEmailVerification: false,
            isClearingEmailOverride: false,
            saveSettings,
            sendTest,
            sendEmailOverrideVerification,
            clearEmailOverride,
            verifyEmailOverride,
        });
        sendEmailOverrideVerification.mockRejectedValueOnce('bad send');
        clearEmailOverride.mockRejectedValueOnce('bad clear');

        render(<NotificationSettingsPanel />, { wrapper: createWrapper() });

        await userEvent.click(screen.getByRole('button', { name: /send verification/i }));
        await userEvent.click(screen.getByRole('button', { name: /clear override/i }));

        await waitFor(() => {
            expect(toast.error).toHaveBeenCalledWith('Verification didn’t send.');
        });
        await waitFor(() => {
            expect(toast.error).toHaveBeenCalledWith('Couldn’t clear the override.');
        });
    });

    it('updates and tests a non-email channel', async () => {
        const { rerender } = render(<NotificationSettingsPanel />, { wrapper: createWrapper() });

        await userEvent.click(screen.getByRole('checkbox', { name: 'Enable Discord' }));
        const testButtons = screen.getAllByRole('button', { name: /test/i });
        expect(testButtons[1]).toBeDisabled();

        await userEvent.click(screen.getByRole('button', { name: /save settings/i }));

        await waitFor(() => {
            expect(saveSettings).toHaveBeenCalledWith({
                notifications_enabled: true,
                min_fit_for_alerts: 70,
                notify_on_new_match: true,
                notify_on_batch_complete: false,
                channels: {
                    email: { enabled: true },
                    discord: { enabled: true },
                },
            });
        });

        mockUseNotificationSettings.mockReturnValue({
            settings: {
                notifications_enabled: true,
                min_fit_for_alerts: 70,
                notify_on_new_match: true,
                notify_on_batch_complete: false,
                revision: 3,
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
                        enabled: true,
                        configured: false,
                        available: true,
                        masked_recipient: null,
                        availability_reason: null,
                        last_test_status: null,
                        last_tested_at: null,
                        last_test_error: null,
                    },
                },
            },
            isLoading: false,
            isSaving: false,
            isTesting: false,
            isSendingEmailVerification: false,
            isClearingEmailOverride: false,
            saveSettings,
            sendTest,
            sendEmailOverrideVerification,
            clearEmailOverride,
            verifyEmailOverride,
        });

        rerender(<NotificationSettingsPanel />);

        expect(screen.getByText('Not configured')).toBeInTheDocument();
        expect(screen.getAllByRole('button', { name: /test/i })[1]).toBeDisabled();
    });

    it('updates global and email toggles, then tests a configured non-email channel', async () => {
        const { rerender } = render(<NotificationSettingsPanel />, { wrapper: createWrapper() });

        await userEvent.click(screen.getByRole('checkbox', { name: 'Enable notifications' }));
        await userEvent.click(screen.getByRole('checkbox', { name: 'Notify on new saved matches' }));
        await userEvent.click(screen.getByRole('checkbox', { name: 'Notify when a batch finishes' }));
        await userEvent.click(screen.getByRole('checkbox', { name: 'Enable Email' }));
        await userEvent.click(screen.getByRole('button', { name: /save settings/i }));

        await waitFor(() => {
            expect(saveSettings).toHaveBeenCalledWith({
                notifications_enabled: false,
                min_fit_for_alerts: 70,
                notify_on_new_match: false,
                notify_on_batch_complete: true,
                channels: {
                    email: { enabled: false },
                    discord: { enabled: false },
                },
            });
        });

        mockUseNotificationSettings.mockReturnValue({
            settings: {
                notifications_enabled: false,
                min_fit_for_alerts: 70,
                notify_on_new_match: false,
                notify_on_batch_complete: true,
                revision: 7,
                channels: {
                    email: {
                        enabled: false,
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
                        last_test_status: null,
                        last_tested_at: null,
                        last_test_error: null,
                    },
                },
            },
            isLoading: false,
            isSaving: false,
            isTesting: false,
            isSendingEmailVerification: false,
            isClearingEmailOverride: false,
            saveSettings,
            sendTest,
            sendEmailOverrideVerification,
            clearEmailOverride,
            verifyEmailOverride,
        });

        rerender(<NotificationSettingsPanel />);

        await userEvent.click(screen.getAllByRole('button', { name: /test/i })[1]);

        await waitFor(() => {
            expect(sendTest).toHaveBeenCalledWith({ channel_type: 'discord' });
        });
    });
});
