import { useEffect, useMemo, useState } from 'react';
import { Mail, MessageSquare, Send } from 'lucide-react';
import { toast } from 'sonner';

import { Button } from '@/components/ui/Button';
import { useNotificationSettings } from '@/hooks/useNotificationSettings';
import type {
    NotificationSettings,
    NotificationSettingsUpdateRequest,
} from '@/types/api';

type EditableChannel = {
    enabled: boolean;
    secret_value?: string;
    clear_secret?: boolean;
};

type EditableSettings = {
    notifications_enabled: boolean;
    min_fit_for_alerts: number;
    notify_on_new_match: boolean;
    notify_on_batch_complete: boolean;
    channels: Record<string, EditableChannel>;
};

const CHANNEL_LABELS: Record<string, string> = {
    email: 'Email',
    discord: 'Discord',
    telegram: 'Telegram',
};

const CHANNEL_HELP: Record<string, string> = {
    email: 'Sends to the email address on your account.',
    discord: 'Uses a saved Discord webhook destination.',
    telegram: 'Uses a saved Telegram chat or channel target.',
};

const CHANNEL_ICONS: Record<string, typeof Mail> = {
    email: Mail,
    discord: MessageSquare,
    telegram: Send,
};

const SECRET_CHANNELS = new Set(['discord', 'telegram']);
const CHANNEL_ORDER = ['email', 'discord', 'telegram'] as const;

const inputClasses =
    'w-full rounded-md border border-rule bg-surface px-3 py-2.5 text-[14px] text-ink placeholder:text-ink-muted transition-colors focus:border-accent focus:outline-none';

function emailStatusText(channel: NotificationSettings['channels'][string]): string {
    switch (channel.override_status) {
        case 'verified':
            return 'Verified override';
        case 'pending':
            return 'Pending — check your inbox';
        case 'expired':
            return 'Expired — resend';
        default:
            return 'Using account email';
    }
}

function emailHelpText(channel: NotificationSettings['channels'][string]): string {
    switch (channel.override_status) {
        case 'verified':
            return 'Notifications go to your verified override address.';
        case 'pending':
            return 'Verification is required before the override will receive notifications.';
        case 'expired':
            return 'Your last verification link expired. Send a fresh one to continue.';
        default:
            return 'Sends to the email address on your account unless you verify an override.';
    }
}

function toDraft(settings: NotificationSettings): EditableSettings {
    const channels: Record<string, EditableChannel> = {};
    for (const [channelType, channel] of Object.entries(settings.channels)) {
        channels[channelType] = {
            enabled: channel.enabled,
            secret_value: '',
            clear_secret: false,
        };
    }
    return {
        notifications_enabled: settings.notifications_enabled,
        min_fit_for_alerts: settings.min_fit_for_alerts,
        notify_on_new_match: settings.notify_on_new_match,
        notify_on_batch_complete: settings.notify_on_batch_complete,
        channels,
    };
}

function buildPayload(draft: EditableSettings): NotificationSettingsUpdateRequest {
    const channels: NotificationSettingsUpdateRequest['channels'] = {};
    for (const [channelType, channel] of Object.entries(draft.channels)) {
        let secretPatch: { secret_value?: string | null } = {};
        if (channel.clear_secret) {
            secretPatch = { secret_value: null };
        } else if (channel.secret_value) {
            secretPatch = { secret_value: channel.secret_value };
        }

        channels[channelType] = {
            enabled: channel.enabled,
            ...secretPatch,
        };
    }
    return {
        notifications_enabled: draft.notifications_enabled,
        min_fit_for_alerts: draft.min_fit_for_alerts,
        notify_on_new_match: draft.notify_on_new_match,
        notify_on_batch_complete: draft.notify_on_batch_complete,
        channels,
    };
}

function AvailabilityNote({ reason }: Readonly<{ reason?: string | null }>) {
    if (!reason) {
        return null;
    }

    return (
        <p className="inline-block border border-warn/40 bg-warn-soft px-2 py-0.5 text-[12px] text-ink">
            {reason}
        </p>
    );
}

function ChannelEnabledToggle({
    channelLabel,
    checked,
    disabled,
    onChange,
}: Readonly<{
    channelLabel: string;
    checked: boolean;
    disabled: boolean;
    onChange: (checked: boolean) => void;
}>) {
    return (
        <label className="flex items-center gap-2 self-start border border-rule bg-surface px-3 py-1.5 text-[12px] text-ink-soft">
            <span>Enabled</span>
            <input
                type="checkbox"
                checked={checked}
                disabled={disabled}
                onChange={(event) => onChange(event.target.checked)}
                aria-label={`Enable ${channelLabel}`}
                className="h-4 w-4 rounded-sm border-rule accent-accent"
            />
        </label>
    );
}

function ChannelHeader({
    Icon,
    channelLabel,
    recipient,
    helperText,
    statusCaption,
    availabilityReason,
    toggle,
}: Readonly<{
    Icon: typeof Mail;
    channelLabel: string;
    recipient: string;
    helperText: string;
    statusCaption?: string | null;
    availabilityReason?: string | null;
    toggle: React.ReactNode;
}>) {
    return (
        <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
            <div className="flex items-start gap-3">
                <Icon className="mt-0.5 h-4 w-4 flex-shrink-0 text-ink-muted" aria-hidden="true" />
                <div className="space-y-1">
                    <div className="text-[14px] font-medium text-ink">{channelLabel}</div>
                    <div className="text-[13px] text-ink-soft">{recipient}</div>
                    <div className="text-[12px] text-ink-muted">{helperText}</div>
                    {statusCaption && <p className="caption">{statusCaption}</p>}
                    <AvailabilityNote reason={availabilityReason} />
                </div>
            </div>

            {toggle}
        </div>
    );
}

function ChannelTestRow({
    lastTestStatus,
    canTest,
    isTesting,
    onTest,
}: Readonly<{
    lastTestStatus?: string | null;
    canTest: boolean;
    isTesting: boolean;
    onTest: () => void;
}>) {
    return (
        <div className="mt-4 flex flex-col gap-3 border-t border-rule pt-4 sm:flex-row sm:items-center sm:justify-between">
            <span className="caption">
                Last test: <span className="text-ink-soft">{lastTestStatus || 'Never'}</span>
            </span>
            <Button
                type="button"
                size="sm"
                variant="secondary"
                disabled={!canTest || isTesting}
                onClick={onTest}
            >
                <Send className="h-3.5 w-3.5" aria-hidden="true" />
                Test
            </Button>
        </div>
    );
}

function ToggleRow({
    label,
    description,
    checked,
    onChange,
}: Readonly<{
    label: string;
    description: string;
    checked: boolean;
    onChange: (checked: boolean) => void;
}>) {
    return (
        <label className="flex items-start justify-between gap-4 border-b border-rule py-4 last:border-b-0">
            <div>
                <div className="text-[14px] font-medium text-ink">{label}</div>
                <div className="mt-1 text-[13px] text-ink-soft">{description}</div>
            </div>
            <input
                type="checkbox"
                checked={checked}
                onChange={(event) => onChange(event.target.checked)}
                aria-label={label}
                className="mt-1 h-4 w-4 rounded-sm border-rule accent-accent"
            />
        </label>
    );
}

export function NotificationSettingsPanel() {
    const {
        settings,
        isLoading,
        isSaving,
        isTesting,
        isSendingEmailVerification,
        isClearingEmailOverride,
        saveSettings,
        sendTest,
        sendEmailOverrideVerification,
        clearEmailOverride,
    } = useNotificationSettings();
    const [draft, setDraft] = useState<EditableSettings | null>(null);
    const [hasUnsavedChanges, setHasUnsavedChanges] = useState(false);
    const [emailOverrideAddress, setEmailOverrideAddress] = useState('');

    useEffect(() => {
        if (settings) {
            setDraft(toDraft(settings));
            setHasUnsavedChanges(false);
            setEmailOverrideAddress(settings.channels.email?.override_address || '');
        }
    }, [settings]);

    const orderedChannels = useMemo(() => {
        if (!settings) return [];
        return CHANNEL_ORDER.flatMap((channelType) =>
            settings.channels[channelType] ? [[channelType, settings.channels[channelType]] as const] : []
        );
    }, [settings]);

    if (isLoading || !draft || !settings) {
        return (
            <div className="space-y-3">
                <div className="h-28 animate-pulse border border-rule bg-surface-sunk" />
                <div className="h-40 animate-pulse border border-rule bg-surface-sunk" />
                <div className="h-64 animate-pulse border border-rule bg-surface-sunk" />
            </div>
        );
    }

    const updateGlobal = <K extends keyof EditableSettings>(key: K, value: EditableSettings[K]) => {
        setDraft((current) => (current ? { ...current, [key]: value } : current));
        setHasUnsavedChanges(true);
    };

    const updateChannel = (channelType: string, patch: Partial<EditableChannel>) => {
        setDraft((current) => {
            if (!current) return current;
            return {
                ...current,
                channels: {
                    ...current.channels,
                    [channelType]: {
                        ...current.channels[channelType],
                        ...patch,
                    },
                },
            };
        });
        setHasUnsavedChanges(true);
    };

    const handleSave = async () => {
        try {
            await saveSettings(buildPayload(draft));
            toast.success('Notification settings saved.');
            setHasUnsavedChanges(false);
        } catch (error) {
            const message = error instanceof Error ? error.message : 'Couldn’t save notification settings.';
            toast.error(message);
        }
    };

    const handleTest = async (channelType: string) => {
        try {
            const response = await sendTest({ channel_type: channelType });
            toast.success(response.data.message);
        } catch (error) {
            const message = error instanceof Error ? error.message : 'Test notification didn’t go through.';
            toast.error(message);
        }
    };

    const handleSendEmailVerification = async () => {
        try {
            const response = await sendEmailOverrideVerification({
                address: emailOverrideAddress.trim(),
            });
            toast.success(response.data.message);
        } catch (error) {
            const message = error instanceof Error ? error.message : 'Verification didn’t send.';
            toast.error(message);
        }
    };

    const handleClearEmailOverride = async () => {
        try {
            const response = await clearEmailOverride();
            toast.success(response.data.message);
        } catch (error) {
            const message = error instanceof Error ? error.message : 'Couldn’t clear the override.';
            toast.error(message);
        }
    };

    return (
        <div className="space-y-6">
            <section className="grid gap-4 lg:grid-cols-[1.2fr_0.8fr]">
                <div className="border border-rule bg-surface">
                    <div className="border-b border-rule px-5 py-4">
                        <p className="caption">Alert rules</p>
                        <h3 className="mt-1 text-[15px] font-medium text-ink">When to reach out</h3>
                    </div>

                    <div className="px-5">
                        <ToggleRow
                            label="Enable notifications"
                            description="Master switch for match and batch notifications."
                            checked={draft.notifications_enabled}
                            onChange={(checked) => updateGlobal('notifications_enabled', checked)}
                        />
                        <ToggleRow
                            label="Notify on new saved matches"
                            description="Alerts from your saved top matches that clear your minimum fit."
                            checked={draft.notify_on_new_match}
                            onChange={(checked) => updateGlobal('notify_on_new_match', checked)}
                        />
                        <ToggleRow
                            label="Notify when a batch finishes"
                            description="Useful when running a full pass and waiting on completion."
                            checked={draft.notify_on_batch_complete}
                            onChange={(checked) => updateGlobal('notify_on_batch_complete', checked)}
                        />
                    </div>
                </div>

                <div className="border border-rule bg-surface">
                    <div className="border-b border-rule px-5 py-4">
                        <p className="caption">Minimum fit</p>
                        <h3 className="mt-1 text-[15px] font-medium text-ink">Alert floor</h3>
                        <p className="mt-1 text-[13px] text-ink-soft">
                            Jobs must meet this fit score to ever page you.
                        </p>
                    </div>
                    <div className="px-5 py-5">
                        <div className="mb-3 flex items-baseline justify-between">
                            <span className="display-numeral text-[40px] text-ink tabular-nums">
                                {draft.min_fit_for_alerts}
                            </span>
                            <span className="caption">fit floor</span>
                        </div>
                        <input
                            className="wm-slider w-full"
                            type="range"
                            min={0}
                            max={100}
                            value={draft.min_fit_for_alerts}
                            onChange={(event) => updateGlobal('min_fit_for_alerts', Number(event.target.value))}
                            aria-label="Minimum fit for alerts"
                        />
                        <div className="mt-2 flex justify-between text-[11px] text-ink-muted tabular-nums">
                            <span>0</span>
                            <span>50</span>
                            <span>100</span>
                        </div>
                    </div>
                </div>
            </section>

            <section className="border border-rule bg-surface">
                <div className="flex flex-col gap-2 border-b border-rule px-5 py-4 sm:flex-row sm:items-end sm:justify-between">
                    <div>
                        <p className="caption">Delivery</p>
                        <h3 className="mt-1 text-[15px] font-medium text-ink">Channels</h3>
                        <p className="mt-1 text-[13px] text-ink-soft">
                            Save destinations explicitly, then test once your changes are stored.
                        </p>
                    </div>
                    <p className="caption tabular-nums">Revision {settings.revision}</p>
                </div>

                <div className="divide-y divide-rule">
                    {orderedChannels.map(([channelType, channel]) => {
                        const editableChannel = draft.channels[channelType];
                        const canEnable = channel.available || editableChannel.enabled;
                        const canTest = !hasUnsavedChanges && channel.available && channel.configured && !isTesting;
                        const Icon = CHANNEL_ICONS[channelType] ?? Mail;
                        const channelLabel = CHANNEL_LABELS[channelType] ?? channelType;

                        if (channelType === 'email') {
                            return (
                                <article key={channelType} className="px-5 py-5">
                                    <ChannelHeader
                                        Icon={Icon}
                                        channelLabel={channelLabel}
                                        recipient={channel.effective_recipient || channel.masked_recipient || 'No email configured'}
                                        helperText={emailHelpText(channel)}
                                        statusCaption={emailStatusText(channel)}
                                        availabilityReason={!channel.available ? channel.availability_reason : null}
                                        toggle={(
                                            <ChannelEnabledToggle
                                                channelLabel={channelLabel}
                                                checked={editableChannel.enabled}
                                                disabled={!canEnable}
                                                onChange={(checked) => updateChannel(channelType, { enabled: checked })}
                                            />
                                        )}
                                    />

                                    <div className="mt-4 border-t border-rule pt-4">
                                        <label className="block">
                                            <span className="caption">Override email</span>
                                            <input
                                                className={`${inputClasses} mt-2`}
                                                type="email"
                                                placeholder="name@example.com"
                                                value={emailOverrideAddress}
                                                onChange={(event) => setEmailOverrideAddress(event.target.value)}
                                            />
                                        </label>
                                        <div className="mt-3 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
                                            <span className="text-[12px] text-ink-muted">
                                                Ignored until verified.
                                            </span>
                                            <div className="flex flex-wrap gap-2">
                                                <Button
                                                    type="button"
                                                    size="sm"
                                                    variant="secondary"
                                                    disabled={!emailOverrideAddress.trim() || isSendingEmailVerification}
                                                    onClick={() => void handleSendEmailVerification()}
                                                >
                                                    Send verification
                                                </Button>
                                                {channel.override_address && (
                                                    <Button
                                                        type="button"
                                                        size="sm"
                                                        variant="ghost"
                                                        disabled={isClearingEmailOverride}
                                                        onClick={() => void handleClearEmailOverride()}
                                                    >
                                                        Clear override
                                                    </Button>
                                                )}
                                            </div>
                                        </div>
                                    </div>

                                    <ChannelTestRow
                                        lastTestStatus={channel.last_test_status}
                                        canTest={canTest}
                                        isTesting={isTesting}
                                        onTest={() => void handleTest(channelType)}
                                    />
                                </article>
                            );
                        }

                        return (
                            <article key={channelType} className="px-5 py-5">
                                <ChannelHeader
                                    Icon={Icon}
                                    channelLabel={channelLabel}
                                    recipient={channel.masked_recipient || (channel.configured ? 'Configured' : 'Not configured')}
                                    helperText={CHANNEL_HELP[channelType] ?? 'Saved destination for notifications.'}
                                    availabilityReason={!channel.available ? channel.availability_reason : null}
                                    toggle={(
                                        <ChannelEnabledToggle
                                            channelLabel={channelLabel}
                                            checked={editableChannel.enabled}
                                            disabled={!canEnable}
                                            onChange={(checked) => updateChannel(channelType, { enabled: checked })}
                                        />
                                    )}
                                />

                                {SECRET_CHANNELS.has(channelType) && (
                                    <div className="mt-4 border-t border-rule pt-4">
                                        <label className="block">
                                            <span className="caption">Destination</span>
                                            <input
                                                className={`${inputClasses} mt-2`}
                                                type="password"
                                                placeholder={`Paste ${channelLabel} destination`}
                                                value={editableChannel.secret_value || ''}
                                                onChange={(event) => updateChannel(channelType, {
                                                    secret_value: event.target.value,
                                                    clear_secret: false,
                                                })}
                                            />
                                        </label>
                                        <div className="mt-3 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
                                            <span className="text-[12px] text-ink-muted">
                                                Leave blank to keep the saved destination.
                                            </span>
                                            <Button
                                                type="button"
                                                size="sm"
                                                variant="ghost"
                                                onClick={() => updateChannel(channelType, {
                                                    secret_value: '',
                                                    clear_secret: true,
                                                    enabled: false,
                                                })}
                                            >
                                                Clear
                                            </Button>
                                        </div>
                                    </div>
                                )}

                                <ChannelTestRow
                                    lastTestStatus={channel.last_test_status}
                                    canTest={canTest}
                                    isTesting={isTesting}
                                    onTest={() => void handleTest(channelType)}
                                />
                            </article>
                        );
                    })}
                </div>
            </section>

            <section className="flex flex-col gap-4 border border-rule bg-surface-raised px-5 py-4 sm:flex-row sm:items-center sm:justify-between">
                <div>
                    <p className="text-[14px] text-ink">
                        {hasUnsavedChanges
                            ? 'You have unsaved changes. Save before sending a test.'
                            : 'Your notification settings are up to date.'}
                    </p>
                    <p className="mt-0.5 caption">
                        Tests always use your currently saved configuration.
                    </p>
                </div>
                <Button
                    type="button"
                    variant="primary"
                    onClick={() => void handleSave()}
                    isLoading={isSaving}
                    disabled={!hasUnsavedChanges}
                >
                    Save settings
                </Button>
            </section>
        </div>
    );
}
