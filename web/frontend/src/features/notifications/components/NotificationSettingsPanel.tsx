import { useEffect, useMemo, useState } from 'react';
import { Mail, MessageSquare, Send, ShieldCheck } from 'lucide-react';
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
    min_score_threshold: number;
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
        min_score_threshold: settings.min_score_threshold,
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
        min_score_threshold: draft.min_score_threshold,
        notify_on_new_match: draft.notify_on_new_match,
        notify_on_batch_complete: draft.notify_on_batch_complete,
        channels,
    };
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
        <label className="flex items-start justify-between gap-4 rounded-[1.5rem] border border-slate-200 bg-white px-4 py-4 shadow-sm">
            <div>
                <div className="text-sm font-bold text-slate-900">{label}</div>
                <div className="mt-1 text-sm text-slate-500">{description}</div>
            </div>
            <input
                type="checkbox"
                checked={checked}
                onChange={(event) => onChange(event.target.checked)}
                aria-label={label}
                className="mt-1 h-4 w-4 rounded border-slate-300 text-blue-600 focus:ring-blue-500"
            />
        </label>
    );
}

export function NotificationSettingsPanel() {
    const { settings, isLoading, isSaving, isTesting, saveSettings, sendTest } = useNotificationSettings();
    const [draft, setDraft] = useState<EditableSettings | null>(null);
    const [hasUnsavedChanges, setHasUnsavedChanges] = useState(false);

    useEffect(() => {
        if (settings) {
            setDraft(toDraft(settings));
            setHasUnsavedChanges(false);
        }
    }, [settings]);

    const orderedChannels = useMemo(() => {
        if (!settings) {
            return [];
        }

        return CHANNEL_ORDER.flatMap((channelType) => (
            settings.channels[channelType] ? [[channelType, settings.channels[channelType]] as const] : []
        ));
    }, [settings]);

    if (isLoading || !draft || !settings) {
        return (
            <div className="space-y-4">
                <div className="h-28 animate-pulse rounded-[1.75rem] bg-slate-200" />
                <div className="h-40 animate-pulse rounded-[1.75rem] bg-slate-200" />
                <div className="h-64 animate-pulse rounded-[1.75rem] bg-slate-200" />
            </div>
        );
    }

    const updateGlobal = <K extends keyof EditableSettings>(key: K, value: EditableSettings[K]) => {
        setDraft((current) => (current ? { ...current, [key]: value } : current));
        setHasUnsavedChanges(true);
    };

    const updateChannel = (channelType: string, patch: Partial<EditableChannel>) => {
        setDraft((current) => {
            if (!current) {
                return current;
            }

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
            toast.success('Notification settings saved');
            setHasUnsavedChanges(false);
        } catch (error) {
            const message = error instanceof Error ? error.message : 'Failed to save notification settings';
            toast.error(message);
        }
    };

    const handleTest = async (channelType: string) => {
        try {
            const response = await sendTest({ channel_type: channelType });
            toast.success(response.data.message);
        } catch (error) {
            const message = error instanceof Error ? error.message : 'Failed to send test notification';
            toast.error(message);
        }
    };

    return (
        <div className="space-y-6">
            <section className="grid gap-4 lg:grid-cols-[1.2fr_0.8fr]">
                <div className="rounded-[1.75rem] border border-slate-200 bg-white px-5 py-5 shadow-sm">
                    <div className="flex items-center gap-3">
                        <div className="rounded-2xl bg-gradient-to-br from-slate-950 to-blue-700 p-3 text-white shadow-lg">
                            <ShieldCheck className="h-5 w-5" />
                        </div>
                        <div>
                            <h3 className="text-lg font-black text-slate-950">Alert rules</h3>
                            <p className="text-sm text-slate-500">
                                Control when JobScout reaches out and how selective it should be.
                            </p>
                        </div>
                    </div>

                    <div className="mt-5 grid gap-3">
                        <ToggleRow
                            label="Enable notifications"
                            description="Master switch for match and batch notifications."
                            checked={draft.notifications_enabled}
                            onChange={(checked) => updateGlobal('notifications_enabled', checked)}
                        />
                        <ToggleRow
                            label="Notify on new high-score matches"
                            description="Highlights strong opportunities as soon as they are found."
                            checked={draft.notify_on_new_match}
                            onChange={(checked) => updateGlobal('notify_on_new_match', checked)}
                        />
                        <ToggleRow
                            label="Notify when a batch finishes"
                            description="Useful when running a full matching pass and waiting on completion."
                            checked={draft.notify_on_batch_complete}
                            onChange={(checked) => updateGlobal('notify_on_batch_complete', checked)}
                        />
                    </div>
                </div>

                <div className="rounded-[1.75rem] border border-slate-200 bg-white px-5 py-5 shadow-sm">
                    <div className="text-sm font-bold text-slate-900">Minimum score threshold</div>
                    <p className="mt-1 text-sm text-slate-500">
                        Only matches at or above this score can trigger a notification.
                    </p>
                    <div className="mt-5">
                        <div className="mb-3 flex items-end justify-between">
                            <span className="text-4xl font-black text-slate-950">{draft.min_score_threshold}</span>
                            <span className="rounded-full bg-sky-50 px-3 py-1 text-xs font-semibold uppercase tracking-[0.2em] text-sky-700">
                                threshold
                            </span>
                        </div>
                        <input
                            className="w-full accent-blue-600"
                            type="range"
                            min={0}
                            max={100}
                            value={draft.min_score_threshold}
                            onChange={(event) => updateGlobal('min_score_threshold', Number(event.target.value))}
                            aria-label="Minimum score threshold"
                        />
                        <div className="mt-3 flex justify-between text-xs font-semibold text-slate-400">
                            <span>0</span>
                            <span>50</span>
                            <span>100</span>
                        </div>
                    </div>
                </div>
            </section>

            <section className="rounded-[1.75rem] border border-slate-200 bg-white px-5 py-5 shadow-sm">
                <div className="flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between">
                    <div>
                        <h3 className="text-lg font-black text-slate-950">Delivery channels</h3>
                        <p className="text-sm text-slate-500">
                            Save destinations explicitly, then test once your changes are stored.
                        </p>
                    </div>
                    <div className="text-xs font-semibold uppercase tracking-[0.24em] text-slate-400">
                        Revision {settings.revision}
                    </div>
                </div>

                <div className="mt-5 space-y-4">
                    {orderedChannels.map(([channelType, channel]) => {
                        const editableChannel = draft.channels[channelType];
                        const canEnable = channel.available || editableChannel.enabled;
                        const canTest = !hasUnsavedChanges && channel.available && channel.configured && !isTesting;
                        const Icon = CHANNEL_ICONS[channelType] ?? Mail;
                        const channelLabel = CHANNEL_LABELS[channelType] ?? channelType;

                        return (
                            <article
                                key={channelType}
                                className="rounded-[1.5rem] border border-slate-200 bg-slate-50/80 p-4"
                            >
                                <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
                                    <div className="flex gap-3">
                                        <div className="rounded-2xl bg-white p-3 text-slate-700 shadow-sm">
                                            <Icon className="h-5 w-5" />
                                        </div>
                                        <div className="space-y-1">
                                            <div className="text-sm font-black text-slate-950">{channelLabel}</div>
                                            <div className="text-sm text-slate-500">
                                                {channel.masked_recipient || (channel.configured ? 'Configured' : 'Not configured')}
                                            </div>
                                            <div className="text-xs text-slate-400">
                                                {CHANNEL_HELP[channelType] ?? 'Saved destination for notifications.'}
                                            </div>
                                            {!channel.available && channel.availability_reason && (
                                                <div className="rounded-full bg-amber-100 px-3 py-1 text-xs font-semibold text-amber-800">
                                                    {channel.availability_reason}
                                                </div>
                                            )}
                                        </div>
                                    </div>

                                    <label className="flex items-center gap-2 rounded-full bg-white px-3 py-2 text-xs font-bold uppercase tracking-[0.2em] text-slate-500 shadow-sm">
                                        <span>Enabled</span>
                                        <input
                                            type="checkbox"
                                            checked={editableChannel.enabled}
                                            disabled={!canEnable}
                                            onChange={(event) => updateChannel(channelType, { enabled: event.target.checked })}
                                            aria-label={`Enable ${channelLabel}`}
                                            className="h-4 w-4 rounded border-slate-300 text-blue-600 focus:ring-blue-500"
                                        />
                                    </label>
                                </div>

                                {SECRET_CHANNELS.has(channelType) && (
                                    <div className="mt-4 rounded-[1.25rem] border border-slate-200 bg-white px-4 py-4">
                                        <input
                                            className="w-full rounded-2xl border border-slate-300 px-3 py-3 text-sm"
                                            type="password"
                                            placeholder={`Paste ${channelLabel} destination`}
                                            value={editableChannel.secret_value || ''}
                                            onChange={(event) => updateChannel(channelType, {
                                                secret_value: event.target.value,
                                                clear_secret: false,
                                            })}
                                        />
                                        <div className="mt-3 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
                                            <span className="text-xs text-slate-500">
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

                                <div className="mt-4 flex flex-col gap-3 border-t border-slate-200 pt-4 sm:flex-row sm:items-center sm:justify-between">
                                    <div className="text-xs font-medium text-slate-500">
                                        Last test: {channel.last_test_status || 'Never'}
                                    </div>
                                    <Button
                                        type="button"
                                        size="sm"
                                        variant="secondary"
                                        disabled={!canTest}
                                        onClick={() => void handleTest(channelType)}
                                    >
                                        <Send className="mr-2 h-4 w-4" />
                                        Test
                                    </Button>
                                </div>
                            </article>
                        );
                    })}
                </div>
            </section>

            <section className="flex flex-col gap-4 rounded-[1.75rem] border border-slate-200 bg-slate-950 px-5 py-5 text-white shadow-xl sm:flex-row sm:items-center sm:justify-between">
                <div>
                    <div className="text-sm font-bold">
                        {hasUnsavedChanges
                            ? 'You have unsaved changes. Save before sending a test notification.'
                            : 'Preferences are up to date.'}
                    </div>
                    <div className="mt-1 text-sm text-slate-300">
                        Tests always use your currently saved configuration.
                    </div>
                </div>
                <Button
                    type="button"
                    onClick={() => void handleSave()}
                    isLoading={isSaving}
                    className="rounded-2xl bg-white text-slate-950 hover:bg-slate-100"
                >
                    Save settings
                </Button>
            </section>
        </div>
    );
}
