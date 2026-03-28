import { useEffect, useState } from 'react';
import { BellRing, Send } from 'lucide-react';
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
    webhook: 'Webhook',
    in_app: 'In-App',
};

const SECRET_CHANNELS = new Set(['discord', 'telegram', 'webhook']);

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
        channels[channelType] = {
            enabled: channel.enabled,
            ...(channel.clear_secret
                ? { secret_value: null }
                : channel.secret_value
                    ? { secret_value: channel.secret_value }
                    : {}),
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

    if (isLoading || !draft || !settings) {
        return <div className="animate-pulse bg-gradient-to-br from-slate-100 to-slate-200 h-[32rem] rounded-3xl xl:max-w-sidebar" />;
    }

    const updateGlobal = <K extends keyof EditableSettings>(key: K, value: EditableSettings[K]) => {
        setDraft((current) => current ? { ...current, [key]: value } : current);
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
        <div className="relative bg-gradient-to-br from-amber-50 via-white to-sky-50 rounded-3xl overflow-hidden xl:max-w-sidebar border border-amber-100">
            <div className="absolute top-0 right-0 w-28 h-28 bg-amber-300/15 rounded-full blur-3xl" />
            <div className="absolute bottom-0 left-0 w-24 h-24 bg-sky-300/20 rounded-full blur-3xl" />

            <div className="relative p-6 space-y-5">
                <div className="flex items-center gap-2">
                    <div className="p-2 bg-gradient-to-br from-amber-500 to-orange-600 rounded-xl shadow-lg">
                        <BellRing className="w-5 h-5 text-white" />
                    </div>
                    <div>
                        <h3 className="text-lg font-black text-gray-900">Notifications</h3>
                        <p className="text-xs font-semibold text-gray-500 uppercase tracking-wider">
                            Explicit save for secure channel settings
                        </p>
                    </div>
                </div>

                <div className="grid grid-cols-1 gap-3">
                    <label className="flex items-center justify-between rounded-2xl bg-white/70 border border-gray-200 px-4 py-3">
                        <span className="text-sm font-semibold text-gray-800">Enable notifications</span>
                        <input
                            type="checkbox"
                            checked={draft.notifications_enabled}
                            onChange={(event) => updateGlobal('notifications_enabled', event.target.checked)}
                        />
                    </label>
                    <label className="flex items-center justify-between rounded-2xl bg-white/70 border border-gray-200 px-4 py-3">
                        <span className="text-sm font-semibold text-gray-800">Notify on new high-score matches</span>
                        <input
                            type="checkbox"
                            checked={draft.notify_on_new_match}
                            onChange={(event) => updateGlobal('notify_on_new_match', event.target.checked)}
                        />
                    </label>
                    <label className="flex items-center justify-between rounded-2xl bg-white/70 border border-gray-200 px-4 py-3">
                        <span className="text-sm font-semibold text-gray-800">Notify when a batch finishes</span>
                        <input
                            type="checkbox"
                            checked={draft.notify_on_batch_complete}
                            onChange={(event) => updateGlobal('notify_on_batch_complete', event.target.checked)}
                        />
                    </label>
                    <label className="rounded-2xl bg-white/70 border border-gray-200 px-4 py-3 block">
                        <span className="block text-sm font-semibold text-gray-800 mb-2">Minimum score threshold</span>
                        <input
                            className="w-full rounded-xl border border-gray-300 px-3 py-2"
                            type="number"
                            min={0}
                            max={100}
                            value={draft.min_score_threshold}
                            onChange={(event) => updateGlobal('min_score_threshold', Number(event.target.value))}
                        />
                    </label>
                </div>

                <div className="space-y-3">
                    {Object.entries(settings.channels).map(([channelType, channel]) => {
                        const editableChannel = draft.channels[channelType];
                        const canEnable = channel.available || editableChannel.enabled;
                        const canTest = !hasUnsavedChanges && channel.available && channel.configured && !isTesting;

                        return (
                            <div key={channelType} className="rounded-2xl bg-white/80 border border-gray-200 p-4 space-y-3">
                                <div className="flex items-start justify-between gap-3">
                                    <div>
                                        <div className="text-sm font-black text-gray-900">{CHANNEL_LABELS[channelType] ?? channelType}</div>
                                        <div className="text-xs text-gray-500">
                                            {channel.masked_recipient || (channel.configured ? 'Configured' : 'Not configured')}
                                        </div>
                                        {!channel.available && channel.availability_reason && (
                                            <div className="mt-1 text-xs text-amber-700">{channel.availability_reason}</div>
                                        )}
                                    </div>
                                    <label className="flex items-center gap-2 text-xs font-semibold text-gray-600">
                                        Enabled
                                        <input
                                            type="checkbox"
                                            checked={editableChannel.enabled}
                                            disabled={!canEnable}
                                            onChange={(event) => updateChannel(channelType, { enabled: event.target.checked })}
                                        />
                                    </label>
                                </div>

                                {SECRET_CHANNELS.has(channelType) && (
                                    <div className="space-y-2">
                                        <input
                                            className="w-full rounded-xl border border-gray-300 px-3 py-2 text-sm"
                                            type="password"
                                            placeholder={`Paste ${CHANNEL_LABELS[channelType]} destination`}
                                            value={editableChannel.secret_value || ''}
                                            onChange={(event) => updateChannel(channelType, {
                                                secret_value: event.target.value,
                                                clear_secret: false,
                                            })}
                                        />
                                        <div className="flex justify-between items-center gap-3">
                                            <span className="text-xs text-gray-500">
                                                Leave blank to keep the saved value.
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

                                <div className="flex items-center justify-between gap-3">
                                    <div className="text-xs text-gray-500">
                                        Last test: {channel.last_test_status || 'Never'}
                                    </div>
                                    <Button
                                        type="button"
                                        size="sm"
                                        variant="secondary"
                                        disabled={!canTest}
                                        onClick={() => void handleTest(channelType)}
                                    >
                                        <Send className="w-4 h-4 mr-2" />
                                        Test
                                    </Button>
                                </div>
                            </div>
                        );
                    })}
                </div>

                <div className="flex items-center justify-between gap-3">
                    <div className="text-xs text-gray-500">
                        {hasUnsavedChanges
                            ? 'You have unsaved changes. Save before sending a test notification.'
                            : `Revision ${settings.revision}`}
                    </div>
                    <Button type="button" onClick={() => void handleSave()} isLoading={isSaving}>
                        Save settings
                    </Button>
                </div>
            </div>
        </div>
    );
}
