import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { vi } from 'vitest';
import { toast } from 'sonner';

import { NotificationSettingsPanel } from '../NotificationSettingsPanel';
import { useNotificationSettings } from '@/hooks/useNotificationSettings';
import type { NotificationSettings } from '@/types/api';

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

const renderPanel = () => render(<NotificationSettingsPanel />, { wrapper: createWrapper() });

const makeChannel = (
    overrides: Partial<NotificationSettings['channels'][string]> = {}
): NotificationSettings['channels'][string] => ({
    enabled: true,
    configured: true,
    available: true,
    masked_recipient: '***@example.com',
    availability_reason: null,
    last_test_status: null,
    last_tested_at: null,
    last_test_error: null,
    ...overrides,
});

const makeSettings = (overrides: Partial<NotificationSettings> = {}): NotificationSettings => {
    const base: NotificationSettings = {
        notifications_enabled: true,
        min_fit_for_alerts: 70,
        notify_on_new_match: true,
        notify_on_batch_complete: false,
        revision: 2,
        channels: {
            email: makeChannel(),
            discord: makeChannel({
                enabled: false,
                masked_recipient: 'https://discord.com/api/webhooks/...',
                last_test_status: 'queued',
            }),
        },
    };

    return {
        ...base,
        ...overrides,
        channels: {
            ...base.channels,
            ...(overrides.channels ?? {}),
        },
    };
};

describe('NotificationSettingsPanel', () => {
    const saveSettings = vi.fn();
    const sendTest = vi.fn();
    const sendEmailOverrideVerification = vi.fn();
    const clearEmailOverride = vi.fn();
    const verifyEmailOverride = vi.fn();

    const makeHookState = (
        overrides: Partial<ReturnType<typeof useNotificationSettings>> = {}
    ): ReturnType<typeof useNotificationSettings> => ({
        settings: makeSettings(),
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
        ...overrides,
    });

    const setHookState = (overrides: Partial<ReturnType<typeof useNotificationSettings>> = {}) => {
        mockUseNotificationSettings.mockReturnValue(makeHookState(overrides));
    };

    beforeEach(() => {
        vi.clearAllMocks();
        setHookState();
        saveSettings.mockResolvedValue({ data: {} });
        sendTest.mockResolvedValue({ data: { message: 'Queued test notification' } });
        sendEmailOverrideVerification.mockResolvedValue({ data: { message: 'Verification email sent' } });
        clearEmailOverride.mockResolvedValue({ data: { message: 'Email override cleared' } });
        verifyEmailOverride.mockResolvedValue({ data: { message: 'Email override verified' } });
    });

    it('renders saved channel state', () => {
        renderPanel();

        expect(screen.getByText('Alert rules')).toBeInTheDocument();
        expect(screen.getByText('***@example.com')).toBeInTheDocument();
        expect(screen.getByText('https://discord.com/api/webhooks/...')).toBeInTheDocument();
        expect(screen.queryByText('In-App')).not.toBeInTheDocument();
        expect(screen.queryByText('Webhook')).not.toBeInTheDocument();
    });

    it('saves edited settings explicitly', async () => {
        renderPanel();

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
        renderPanel();

        const threshold = screen.getByLabelText('Minimum fit for alerts');
        fireEvent.change(threshold, { target: { value: '71' } });

        const testButtons = screen.getAllByRole('button', { name: /test/i });
        expect(testButtons[0]).toBeDisabled();
        expect(testButtons[1]).toBeDisabled();
    });

    it('queues a test notification when settings are clean', async () => {
        renderPanel();

        const testButtons = screen.getAllByRole('button', { name: /test/i });
        await userEvent.click(testButtons[0]);

        await waitFor(() => {
            expect(sendTest).toHaveBeenCalledWith({ channel_type: 'email' });
        });
        expect(toast.success).toHaveBeenCalledWith('Queued test notification');
    });

    it('clears a saved secret explicitly', async () => {
        renderPanel();

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
        setHookState({
            settings: makeSettings({
                channels: {
                    email: makeChannel(),
                    telegram: makeChannel({
                        enabled: false,
                        configured: false,
                        available: false,
                        masked_recipient: null,
                        availability_reason: 'Telegram bot credentials are not configured',
                        last_test_status: null,
                    }),
                },
            }),
        });

        renderPanel();

        expect(screen.getByText('Telegram bot credentials are not configured')).toBeInTheDocument();
        expect(screen.getByRole('checkbox', { name: 'Enable Telegram' })).toBeDisabled();
        const testButtons = screen.getAllByRole('button', { name: /test/i });
        expect(testButtons[2]).toBeDisabled();
    });

    it('shows a save error toast', async () => {
        saveSettings.mockRejectedValueOnce(new Error('Save exploded'));
        renderPanel();

        fireEvent.change(screen.getByLabelText('Minimum fit for alerts'), { target: { value: '72' } });
        await userEvent.click(screen.getByRole('button', { name: /save settings/i }));

        await waitFor(() => {
            expect(toast.error).toHaveBeenCalledWith('Save exploded');
        });
    });

    it('shows a test error toast', async () => {
        sendTest.mockRejectedValueOnce(new Error('Test exploded'));
        renderPanel();

        const testButtons = screen.getAllByRole('button', { name: /test/i });
        await userEvent.click(testButtons[0]);

        await waitFor(() => {
            expect(toast.error).toHaveBeenCalledWith('Test exploded');
        });
    });

    it('sends and clears an email override', async () => {
        setHookState({
            settings: makeSettings({
                channels: {
                    email: makeChannel({
                        effective_recipient: 'ada+alerts@example.com',
                        override_address: 'ada+alerts@example.com',
                        override_status: 'pending',
                        last_test_status: 'queued',
                    }),
                },
            }),
        });

        renderPanel();

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
        const { rerender } = renderPanel();

        setHookState({
            settings: makeSettings({
                revision: 3,
                channels: {
                    email: makeChannel({
                        override_address: 'ada@example.com',
                        override_status: 'verified',
                    }),
                },
            }),
        });

        rerender(<NotificationSettingsPanel />);
        expect(screen.getByText('Verified override')).toBeInTheDocument();

        setHookState({
            settings: makeSettings({
                revision: 4,
                channels: {
                    email: makeChannel({
                        override_address: 'ada@example.com',
                        override_status: 'expired',
                    }),
                },
            }),
        });

        rerender(<NotificationSettingsPanel />);
        expect(screen.getByText('Expired — resend')).toBeInTheDocument();
    });

    it('renders a loading skeleton while settings are loading', () => {
        setHookState({ settings: undefined, isLoading: true });

        const { container } = renderPanel();

        expect(container.querySelectorAll('.animate-pulse')).toHaveLength(3);
    });

    it('renders email fallback copy and blocks unavailable email toggles', () => {
        setHookState({
            settings: makeSettings({
                revision: 5,
                channels: {
                    email: makeChannel({
                        enabled: false,
                        configured: false,
                        available: false,
                        masked_recipient: null,
                        override_address: null,
                        override_status: 'none',
                        availability_reason: 'Email delivery is disabled for this account',
                    }),
                },
            }),
        });

        renderPanel();

        expect(screen.getByText('No email configured')).toBeInTheDocument();
        expect(screen.getByText('Email delivery is disabled for this account')).toBeInTheDocument();
        expect(screen.getByRole('checkbox', { name: 'Enable Email' })).toBeDisabled();
        expect(screen.getAllByRole('button', { name: /test/i })[0]).toBeDisabled();
    });

    it('shows default error toasts for non-error verification failures', async () => {
        setHookState({
            settings: makeSettings({
                revision: 6,
                channels: {
                    email: makeChannel({
                        effective_recipient: 'ada@example.com',
                        override_address: 'ada@example.com',
                        override_status: 'pending',
                    }),
                },
            }),
        });
        sendEmailOverrideVerification.mockRejectedValueOnce('bad send');
        clearEmailOverride.mockRejectedValueOnce('bad clear');

        renderPanel();

        await userEvent.click(screen.getByRole('button', { name: /send verification/i }));
        await userEvent.click(screen.getByRole('button', { name: /clear override/i }));

        await waitFor(() => {
            expect(toast.error).toHaveBeenCalledWith('Verification didn’t send.');
        });
        await waitFor(() => {
            expect(toast.error).toHaveBeenCalledWith('Couldn’t clear the override.');
        });
    });

    it('shows direct error messages for verification failures raised as Error objects', async () => {
        setHookState({
            settings: makeSettings({
                revision: 6,
                channels: {
                    email: makeChannel({
                        effective_recipient: 'ada@example.com',
                        override_address: 'ada@example.com',
                        override_status: 'pending',
                    }),
                },
            }),
        });
        sendEmailOverrideVerification.mockRejectedValueOnce(new Error('Verification exploded'));
        clearEmailOverride.mockRejectedValueOnce(new Error('Clear exploded'));

        renderPanel();

        await userEvent.click(screen.getByRole('button', { name: /send verification/i }));
        await userEvent.click(screen.getByRole('button', { name: /clear override/i }));

        await waitFor(() => {
            expect(toast.error).toHaveBeenCalledWith('Verification exploded');
        });
        await waitFor(() => {
            expect(toast.error).toHaveBeenCalledWith('Clear exploded');
        });
    });

    it('updates and tests a non-email channel', async () => {
        const { rerender } = renderPanel();

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

        setHookState({
            settings: makeSettings({
                revision: 3,
                channels: {
                    discord: makeChannel({
                        enabled: true,
                        configured: false,
                        masked_recipient: null,
                        last_test_status: null,
                    }),
                },
            }),
        });

        rerender(<NotificationSettingsPanel />);

        expect(screen.getByText('Not configured')).toBeInTheDocument();
        expect(screen.getAllByRole('button', { name: /test/i })[1]).toBeDisabled();
    });

    it('renders configured copy when a saved channel has no masked recipient', () => {
        setHookState({
            settings: makeSettings({
                revision: 8,
                channels: {
                    discord: makeChannel({
                        enabled: false,
                        masked_recipient: null,
                        last_test_status: null,
                    }),
                },
            }),
        });

        renderPanel();

        expect(screen.getByText('Configured')).toBeInTheDocument();
        expect(screen.getByText('Uses a saved Discord webhook destination.')).toBeInTheDocument();
    });

    it('updates global and email toggles, then tests a configured non-email channel', async () => {
        const { rerender } = renderPanel();

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

        setHookState({
            settings: makeSettings({
                notifications_enabled: false,
                notify_on_new_match: false,
                notify_on_batch_complete: true,
                revision: 7,
                channels: {
                    email: makeChannel({
                        enabled: false,
                    }),
                    discord: makeChannel({
                        enabled: false,
                        masked_recipient: 'https://discord.com/api/webhooks/...',
                        last_test_status: null,
                    }),
                },
            }),
        });

        rerender(<NotificationSettingsPanel />);

        await userEvent.click(screen.getAllByRole('button', { name: /test/i })[1]);

        await waitFor(() => {
            expect(sendTest).toHaveBeenCalledWith({ channel_type: 'discord' });
        });
    });
});
