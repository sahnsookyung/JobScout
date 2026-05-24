import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useEffect, useMemo, useRef, useState } from 'react';
import type { ComponentProps, SyntheticEvent } from 'react';
import {
    Activity,
    AlertTriangle,
    Check,
    Clock3,
    ExternalLink,
    Globe2,
    History,
    ListFilter,
    MapPin,
    PauseCircle,
    Pencil,
    Plus,
    RefreshCw,
    RotateCcw,
    Search,
    Server,
    ShieldCheck,
    Trash2,
    X,
    Zap,
} from 'lucide-react';
import { toast } from 'sonner';

import { pipelineApi } from '@/services/pipelineApi';
import type {
    AtsSourceCreateRequest,
    AtsSourceDiscoveryCandidate,
    AtsSourceHistoryEvent,
    AtsSourceUpdateRequest,
    CloudIntegration,
    FetchSource,
} from '@/types/api';

const OPERATIONAL_OPTION_KEYS = new Set([
    'status',
    'validation_status',
    'sync_interval_minutes',
    'last_error',
    'last_validated_at',
    'user_source_id',
    'is_user_source',
    'owner_user_id',
    'source_url',
    'ats_provider',
    'ats_identifier',
    'initial_sync_status',
    'initial_sync_jobs_seen',
    'initial_sync_jobs_imported',
    'initial_sync_jobs_deactivated',
]);
const SUPPORTED_ATS_SOURCE_HOSTS = new Set([
    'boards.greenhouse.io',
    'boards-api.greenhouse.io',
    'jobs.lever.co',
    'api.lever.co',
    'jobs.ashbyhq.com',
    'api.ashbyhq.com',
]);
const ATS_IDENTIFIER_CONFIG_KEYS: Record<string, string> = {
    greenhouse: 'board_token',
    lever: 'site_identifier',
    ashby: 'job_board_name',
};
const ATS_PROVIDER_SEARCH_ORDER = ['Greenhouse', 'Lever', 'Ashby'];
const SOURCE_VIEW_OPTIONS = [
    { key: 'all', label: 'All' },
    { key: 'ats', label: 'ATS boards' },
    { key: 'seed', label: 'Seed sites' },
    { key: 'api', label: 'API' },
    { key: 'paused', label: 'Paused' },
    { key: 'needs_attention', label: 'Needs attention' },
] as const;

type SourceView = typeof SOURCE_VIEW_OPTIONS[number]['key'];
type FormSubmitHandler = NonNullable<ComponentProps<'form'>['onSubmit']>;
type FormSubmitEvent = Parameters<FormSubmitHandler>[0];

const SOURCE_ACTIVITY_FILTER_OPTIONS = [
    { key: 'all', label: 'All' },
    { key: 'added', label: 'Added' },
    { key: 'updated', label: 'Updated' },
    { key: 'deleted', label: 'Deleted' },
    { key: 'synced', label: 'Synced' },
    { key: 'recoverable', label: 'Recoverable' },
] as const;

type SourceActivityFilter = typeof SOURCE_ACTIVITY_FILTER_OPTIONS[number]['key'];

function sourceScope(source: FetchSource): string {
    const parts = [source.location, source.country].filter(Boolean);
    return parts.length > 0 ? parts.join(', ') : 'Global';
}

function sourceQuery(source: FetchSource): string {
    return source.search_term?.trim() || 'Seed feed';
}

function optionCount(source: FetchSource): number {
    return Object.entries(source.options || {}).reduce((count, [key, value]) => {
        if (OPERATIONAL_OPTION_KEYS.has(key)) return count;
        if (Array.isArray(value)) return count + value.length;
        return value === undefined || value === null || value === '' ? count : count + 1;
    }, 0);
}

function healthLabel(source: FetchSource): string {
    if (source.fetch_mode !== 'jobspy_api') return '';
    if (!source.api_health) return 'JobSpy status off';
    if (source.api_health.available) return 'JobSpy online';
    if (source.api_health.status === 'not_configured') return 'JobSpy not configured';
    if (source.api_health.status === 'timeout') return 'JobSpy timeout';
    return 'JobSpy offline';
}

function externalSeedLabel(source: FetchSource): string {
    if (source.fetch_mode !== 'seed_website' || !source.external_fetch_status) return '';
    if (source.external_fetch_status.status === 'ok') return 'Worker updated';
    if (source.external_fetch_status.status === 'rate_limited') return 'Worker cooling down';
    if (source.external_fetch_status.status === 'configured') return 'Worker ready';
    if (source.external_fetch_status.status === 'degraded') return 'Worker degraded';
    if (source.external_fetch_status.status === 'disabled') return 'Worker disabled';
    return 'Worker unconfigured';
}

function modeLabel(source: FetchSource): string {
    if (source.fetch_mode === 'seed_website') return source.provider_name || 'Seed website';
    if (source.fetch_mode === 'ats_api') return source.provider_name || 'ATS API';
    if (source.fetch_mode === 'custom_source') return source.provider_name || 'Custom source';
    if (source.fetch_mode === 'jobspy_api') return source.provider_name || 'JobSpy API';
    return source.fetch_mode.replace(/_/g, ' ');
}

function healthTone(source: FetchSource): string {
    if (!source.api_health) return 'border-rule bg-surface-sunk text-ink-soft';
    if (source.api_health.available) return 'border-success/40 bg-success-soft text-ink';
    if (source.api_health.status === 'not_configured') return 'border-rule bg-surface-sunk text-ink-soft';
    return 'border-warn/40 bg-warn-soft text-ink';
}

function externalSeedTone(source: FetchSource): string {
    const status = source.external_fetch_status?.status;
    if (status === 'configured' || status === 'ok') return 'border-success/40 bg-success-soft text-ink';
    if (status === 'degraded' || status === 'rate_limited') return 'border-warn/40 bg-warn-soft text-ink';
    return 'border-rule bg-surface-sunk text-ink-soft';
}

function metaChipClasses(extra = ''): string {
    return `inline-flex min-h-7 items-center gap-1.5 border border-rule bg-surface-raised px-2 py-1 text-[12px] leading-none text-ink-soft ${extra}`;
}

function toTitleCase(value: string): string {
    return value
        .replace(/[_-]/g, ' ')
        .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function compactStrings(values: Array<string | null | undefined>): string[] {
    return values.filter((value): value is string => Boolean(value));
}

function atsStatus(source: FetchSource): string | null {
    const status = source.options?.status;
    return typeof status === 'string' && status.trim() ? status.trim() : null;
}

function atsInterval(source: FetchSource): number | null {
    const interval = source.options?.sync_interval_minutes;
    return typeof interval === 'number' && Number.isFinite(interval) ? interval : null;
}

function sourceKind(source: FetchSource): 'api' | 'ats' | 'custom' | 'seed' | 'other' {
    if (source.fetch_mode === 'ats_api') return 'ats';
    if (source.fetch_mode === 'seed_website') return 'seed';
    if (source.fetch_mode === 'jobspy_api') return 'api';
    if (source.fetch_mode === 'custom_source') return 'custom';
    return 'other';
}

function sourceKindLabel(source: FetchSource): string {
    const kind = sourceKind(source);
    if (kind === 'ats') return 'ATS board';
    if (kind === 'seed') return 'Seed site';
    if (kind === 'api') return 'API source';
    if (kind === 'custom') return 'Custom source';
    return toTitleCase(source.fetch_mode);
}

function sourceVolumeLabel(source: FetchSource): string {
    if (source.fetch_mode === 'ats_api') return 'ATS sync';
    return `${source.results_wanted} jobs`;
}

function userSourceId(source: FetchSource): string | null {
    const sourceId = source.options?.user_source_id;
    return typeof sourceId === 'string' && sourceId ? sourceId : null;
}

function sourceIsPaused(source: FetchSource): boolean {
    if (atsStatus(source) === 'disabled') return true;
    return source.fetch_mode === 'seed_website' && source.external_fetch_status?.status === 'disabled';
}

function sourceNeedsAttention(source: FetchSource): boolean {
    if (source.options?.last_error) return true;
    if (source.fetch_mode === 'ats_api') {
        const validation = source.options?.validation_status;
        return validation === 'failed' || validation === 'invalid' || validation === 'error';
    }
    if (source.fetch_mode === 'jobspy_api') {
        return Boolean(source.api_health && !source.api_health.available && source.api_health.status !== 'not_configured');
    }
    if (source.fetch_mode === 'seed_website') {
        const status = source.external_fetch_status?.status;
        return status === 'degraded' || status === 'rate_limited' || status === 'error';
    }
    return false;
}

function sourceStatusLabel(source: FetchSource): string {
    const status = atsStatus(source);
    if (sourceNeedsAttention(source)) return 'Needs attention';
    if (sourceIsPaused(source)) return 'Paused';
    if (status === 'active') return 'Active';
    if (status) return toTitleCase(status);
    if (source.fetch_mode === 'jobspy_api' && source.api_health?.available) return 'Online';
    if (source.fetch_mode === 'seed_website' && source.external_fetch_status?.configured) return 'Ready';
    return 'Available';
}

function sourceStatusTone(source: FetchSource): string {
    if (sourceNeedsAttention(source)) return 'border-warn/50 bg-warn-soft text-warn';
    if (sourceIsPaused(source)) return 'border-rule bg-surface-sunk text-ink-soft';
    return 'border-success/40 bg-success-soft text-ink';
}

function sourceMatchesView(source: FetchSource, view: SourceView): boolean {
    if (view === 'all') return true;
    if (view === 'paused') return sourceIsPaused(source);
    if (view === 'needs_attention') return sourceNeedsAttention(source);
    return sourceKind(source) === view;
}

function numericOption(source: FetchSource, key: string): number | null {
    const value = source.options?.[key];
    return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

function textOption(source: FetchSource, key: string): string | null {
    const value = source.options?.[key];
    return typeof value === 'string' && value.trim() ? value.trim() : null;
}

function sourceOutcomeLabel(source: FetchSource): string {
    if (source.fetch_mode === 'ats_api') {
        const seen = numericOption(source, 'initial_sync_jobs_seen');
        const imported = numericOption(source, 'initial_sync_jobs_imported');
        if (seen !== null || imported !== null) {
            return `${imported ?? 0} imported / ${seen ?? 0} seen`;
        }
        const validation = textOption(source, 'validation_status');
        if (validation) return `Validation ${toTitleCase(validation)}`;
        return 'Awaiting sync';
    }
    if (source.fetch_mode === 'seed_website') {
        const remaining = source.external_fetch_status?.budget_remaining;
        return remaining === null || remaining === undefined
            ? 'Ready to fetch'
            : `${remaining} fetches left`;
    }
    if (source.fetch_mode === 'jobspy_api') {
        return source.api_health?.available ? 'API reachable' : 'Check API health';
    }
    return `${optionCount(source)} filters`;
}

function sourceBoardReference(source: FetchSource): string {
    const identifier = textOption(source, 'ats_identifier');
    if (identifier) return identifier;
    if (source.seed_url) return source.seed_url;
    return source.site_type;
}

function formattedDateTime(value: string | null): string {
    if (!value) return 'Not recorded';
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return 'Not recorded';
    return new Intl.DateTimeFormat(undefined, {
        month: 'short',
        day: 'numeric',
        hour: 'numeric',
        minute: '2-digit',
    }).format(date);
}

function isSupportedAtsUrl(value: string): boolean {
    const candidate = value.trim();
    if (!candidate) return true;
    try {
        const url = new URL(candidate);
        return (
            url.protocol === 'https:'
            && !url.username
            && !url.password
            && !url.port
            && SUPPORTED_ATS_SOURCE_HOSTS.has(url.hostname.toLowerCase())
        );
    } catch {
        return false;
    }
}

function atsIdentifier(integration: CloudIntegration): string | null {
    const key = ATS_IDENTIFIER_CONFIG_KEYS[integration.provider];
    const value = key ? integration.config?.[key] : null;
    return typeof value === 'string' && value.trim() ? value.trim() : null;
}

function sourcePayloadKey(payload: AtsSourceCreateRequest | AtsSourceUpdateRequest): string {
    return JSON.stringify({
        display_name: payload.display_name ?? null,
        source_url: payload.source_url ?? null,
        provider: payload.provider ?? null,
        identifier: payload.identifier ?? null,
        providers: payload.providers ?? null,
    });
}

function cloudIntegrationSource(integration: CloudIntegration): FetchSource {
    const providerLabel = toTitleCase(integration.provider);
    const status = integration.status || 'unknown';
    const isUserSource = integration.is_user_source === true;
    const identifier = atsIdentifier(integration);
    return {
        site_type: integration.provider,
        display_name: integration.display_name,
        seed_url: integration.source_url ?? null,
        description: isUserSource
            ? `${providerLabel} ATS source from your board list.`
            : `${providerLabel} ATS sync for tenant company jobs.`,
        tags: compactStrings([
            'ats',
            integration.provider,
            status,
            integration.validation_status,
            isUserSource ? 'user' : null,
        ]),
        search_keywords: compactStrings([
            integration.provider,
            integration.display_name,
            integration.source_url,
            'ats',
            integration.status,
            integration.validation_status,
            ...(integration.capabilities || []),
        ]),
        fetch_mode: 'ats_api',
        provider_name: `${providerLabel} ATS`,
        search_term: null,
        location: null,
        country: null,
        results_wanted: 0,
        hours_old: null,
        options: {
            status,
            validation_status: integration.validation_status,
            sync_interval_minutes: integration.sync_interval_minutes,
            last_validated_at: integration.last_validated_at,
            last_error: integration.last_error,
            user_source_id: isUserSource ? integration.id : undefined,
            is_user_source: isUserSource || undefined,
            owner_user_id: integration.owner_user_id || undefined,
            source_url: integration.source_url || undefined,
            ats_provider: integration.provider,
            ats_identifier: identifier || undefined,
            initial_sync_status: integration.initial_sync?.status,
            initial_sync_jobs_seen: integration.initial_sync?.jobs_seen,
            initial_sync_jobs_imported: integration.initial_sync?.jobs_imported,
            initial_sync_jobs_deactivated: integration.initial_sync?.jobs_deactivated,
        },
        api_health: null,
    };
}

function sourceCatalogLabel(apiBasedFetching?: boolean, cloudCount = 0): string {
    if (apiBasedFetching && cloudCount > 0) return 'JobSpy + ATS';
    if (apiBasedFetching) return 'JobSpy API enabled';
    if (cloudCount > 0) return 'Seed + ATS sources';
    return 'Seed and custom sources';
}

function optionSearchValues(value: unknown): string[] {
    if (Array.isArray(value)) {
        return value.flatMap(optionSearchValues);
    }
    if (value === undefined || value === null || value === '') {
        return [];
    }
    if (typeof value === 'object') {
        return [JSON.stringify(value)];
    }
    if (
        typeof value === 'string' ||
        typeof value === 'number' ||
        typeof value === 'boolean' ||
        typeof value === 'bigint'
    ) {
        return [String(value)];
    }
    return [];
}

function sourceSearchText(source: FetchSource): string {
    return [
        source.site_type,
        source.display_name,
        source.seed_url,
        source.description,
        source.fetch_mode,
        source.provider_name,
        source.search_term,
        source.location,
        source.country,
        ...source.tags,
        ...source.search_keywords,
        ...Object.values(source.options || {}).flatMap(optionSearchValues),
    ]
        .filter(Boolean)
        .join(' ')
        .toLowerCase();
}

function sourceMatchesSearch(source: FetchSource, query: string): boolean {
    const terms = query.trim().toLowerCase().split(/\s+/).filter(Boolean);
    if (terms.length === 0) return true;

    const haystack = sourceSearchText(source);
    return terms.every((term) => haystack.includes(term));
}

function canFetchExternalSeed(source: FetchSource): boolean {
    const status = source.external_fetch_status;
    return source.fetch_mode === 'seed_website' && Boolean(status?.enabled && status.configured);
}

function apiErrorMessage(error: unknown): string {
    const data = (error as {
        response?: {
            data?: {
                error?: string;
                message?: string;
                warnings?: string[];
            };
        };
    }).response?.data;
    return data?.warnings?.[0] || data?.error || data?.message || (error instanceof Error ? error.message : 'Unknown error');
}

function historyActionLabel(action: string): string {
    if (action.endsWith('_created')) return 'Added';
    if (action.endsWith('_updated')) return 'Updated';
    if (action.endsWith('_deleted')) return 'Deleted';
    if (action.endsWith('_sync_triggered')) return 'Synced';
    return toTitleCase(action.replace(/^integration\.user_source_/, ''));
}

function historyActionKind(action: string): Exclude<SourceActivityFilter, 'all' | 'recoverable'> | 'other' {
    if (action.endsWith('_created')) return 'added';
    if (action.endsWith('_updated')) return 'updated';
    if (action.endsWith('_deleted')) return 'deleted';
    if (action.endsWith('_sync_triggered')) return 'synced';
    return 'other';
}

function historyEventMatchesFilter(event: AtsSourceHistoryEvent, filter: SourceActivityFilter): boolean {
    if (filter === 'all') return true;
    if (filter === 'recoverable') return Boolean(event.readd_payload);
    return historyActionKind(event.action) === filter;
}

function SourceCard({
    source,
    index,
    onFetchSource,
    isFetchingSource,
    onSyncAtsSource,
    onEditAtsSource,
    onToggleAtsSource,
    onDeleteAtsSource,
    onSubmitEdit,
    onCancelEdit,
    isSyncingAtsSource,
    isUpdatingAtsSource,
    isDeletingAtsSource,
    isEditing,
    editSourceName,
    editSourceUrl,
    editSourceProvider,
    editSourceIdentifier,
    editSyncInterval,
    onEditSourceNameChange,
    onEditSourceUrlChange,
    onEditSourceProviderChange,
    onEditSourceIdentifierChange,
    onEditSyncIntervalChange,
}: Readonly<{
    source: FetchSource;
    index: number;
    onFetchSource: (source: string) => void;
    isFetchingSource: boolean;
    onSyncAtsSource: (sourceId: string) => void;
    onEditAtsSource: (source: FetchSource) => void;
    onToggleAtsSource: (sourceId: string, status: string) => void;
    onDeleteAtsSource: (source: FetchSource) => void;
    onSubmitEdit: (sourceId: string) => void;
    onCancelEdit: () => void;
    isSyncingAtsSource: boolean;
    isUpdatingAtsSource: boolean;
    isDeletingAtsSource: boolean;
    isEditing: boolean;
    editSourceName: string;
    editSourceUrl: string;
    editSourceProvider: string;
    editSourceIdentifier: string;
    editSyncInterval: string;
    onEditSourceNameChange: (value: string) => void;
    onEditSourceUrlChange: (value: string) => void;
    onEditSourceProviderChange: (value: string) => void;
    onEditSourceIdentifierChange: (value: string) => void;
    onEditSyncIntervalChange: (value: string) => void;
}>) {
    const healthText = healthLabel(source);
    const externalText = externalSeedLabel(source);
    const statusText = atsStatus(source);
    const intervalMinutes = atsInterval(source);
    const canFetch = canFetchExternalSeed(source);
    const managedSourceId = userSourceId(source);
    const isDisabled = statusText === 'disabled';
    const isMutatingAtsSource = isSyncingAtsSource || isUpdatingAtsSource || isDeletingAtsSource;
    const providerLabel = modeLabel(source);
    const lastError = textOption(source, 'last_error');
    const lastValidated = textOption(source, 'last_validated_at');
    const boardReference = sourceBoardReference(source);
    const className = 'group min-h-48 border border-rule bg-surface px-4 py-3 transition-colors hover:border-rule-strong';

    return (
        <div key={`${source.site_type}-${index}`} className={className}>
            <div className="flex items-start justify-between gap-3">
                <div className="min-w-0">
                    <div className="flex flex-wrap items-center gap-2">
                        <span className={metaChipClasses('uppercase tracking-[0.12em]')}>
                            {sourceKindLabel(source)}
                        </span>
                        <span className={metaChipClasses(sourceStatusTone(source))}>
                            {sourceStatusLabel(source)}
                        </span>
                    </div>
                    <h4 className="mt-2 truncate text-[15px] font-medium text-ink">
                        {source.display_name}
                    </h4>
                    <div className="mt-1 flex items-center gap-1.5 text-[12px] text-ink-muted">
                        <Search className="h-3 w-3" aria-hidden="true" />
                        <span className="truncate">{sourceQuery(source)}</span>
                    </div>
                </div>
                {source.seed_url ? (
                    <a
                        href={source.seed_url}
                        target="_blank"
                        rel="noreferrer"
                        aria-label={`Open ${source.display_name}`}
                        className="mt-0.5 inline-flex h-8 w-8 flex-shrink-0 items-center justify-center border border-transparent text-ink-muted transition-colors hover:border-rule hover:text-accent"
                    >
                        <ExternalLink className="h-4 w-4" aria-hidden="true" />
                    </a>
                ) : (
                    <Globe2 className="mt-1 h-4 w-4 flex-shrink-0 text-ink-muted transition-colors group-hover:text-accent" aria-hidden="true" />
                )}
            </div>

            {source.description ? (
                <p className="mt-2 line-clamp-2 text-[12px] leading-5 text-ink-muted">
                    {source.description}
                </p>
            ) : null}

            <div className="mt-4 grid gap-2 sm:grid-cols-3">
                <div className="border border-rule bg-surface-sunk px-3 py-2">
                    <div className="flex items-center gap-1.5 text-[11px] uppercase tracking-[0.12em] text-ink-soft">
                        <MapPin className="h-3 w-3" aria-hidden="true" />
                        Scope
                    </div>
                    <div className="mt-1 truncate text-[12px] font-medium text-ink">{sourceScope(source)}</div>
                </div>
                <div className="border border-rule bg-surface-sunk px-3 py-2">
                    <div className="flex items-center gap-1.5 text-[11px] uppercase tracking-[0.12em] text-ink-soft">
                        <Activity className="h-3 w-3" aria-hidden="true" />
                        Outcome
                    </div>
                    <div className="mt-1 truncate text-[12px] font-medium text-ink">{sourceOutcomeLabel(source)}</div>
                </div>
                <div className="border border-rule bg-surface-sunk px-3 py-2">
                    <div className="flex items-center gap-1.5 text-[11px] uppercase tracking-[0.12em] text-ink-soft">
                        <Clock3 className="h-3 w-3" aria-hidden="true" />
                        Cadence
                    </div>
                    <div className="mt-1 truncate text-[12px] font-medium text-ink">
                        {intervalMinutes ? `${intervalMinutes}m sync` : sourceVolumeLabel(source)}
                    </div>
                </div>
            </div>

            {lastError ? (
                <div className="mt-3 flex gap-2 border border-warn/40 bg-warn-soft px-3 py-2 text-[12px] leading-5 text-ink">
                    <AlertTriangle className="mt-0.5 h-3.5 w-3.5 flex-shrink-0 text-warn" aria-hidden="true" />
                    <span className="min-w-0 break-words">{lastError}</span>
                </div>
            ) : null}

            <details className="mt-3 border-t border-rule pt-3">
                <summary className="cursor-pointer text-[12px] font-medium text-ink-soft transition-colors hover:text-accent">
                    Details
                </summary>
                <div className="mt-3 grid gap-2 text-[12px] text-ink-muted">
                    <div className="flex flex-wrap gap-1.5">
                        <span className={metaChipClasses()}>{providerLabel}</span>
                        {healthText ? (
                            <span className={metaChipClasses(`${healthTone(source)} tabular-nums`)}>
                                {healthText}
                            </span>
                        ) : null}
                        {externalText ? (
                            <span className={metaChipClasses(`${externalSeedTone(source)} tabular-nums`)}>
                                {externalText}
                            </span>
                        ) : null}
                        {optionCount(source) > 0 ? (
                            <span className={metaChipClasses('tabular-nums')}>{optionCount(source)} filters</span>
                        ) : null}
                        {lastValidated ? (
                            <span className={metaChipClasses('tabular-nums')}>
                                Checked {formattedDateTime(lastValidated)}
                            </span>
                        ) : null}
                    </div>
                    <div className="grid gap-1">
                        <span className="caption">Board reference</span>
                        <span className="break-all text-ink-soft">{boardReference}</span>
                    </div>
                    {source.tags.length > 0 ? (
                        <div className="flex flex-wrap gap-1">
                            {source.tags.slice(0, 6).map((tag) => (
                                <span
                                    key={tag}
                                    className="border border-rule bg-surface-sunk px-1.5 py-0.5 text-[11px] text-ink-soft"
                                >
                                    {tag}
                                </span>
                            ))}
                        </div>
                    ) : null}
                </div>
            </details>

            {canFetch ? (
                <div className="mt-3 flex justify-end">
                    <button
                        type="button"
                        onClick={() => onFetchSource(source.site_type)}
                        disabled={isFetchingSource}
                        className="inline-flex min-h-9 items-center gap-1.5 border border-accent px-3 py-1 text-[12px] font-medium text-accent transition-colors hover:bg-accent-soft disabled:cursor-not-allowed disabled:border-rule disabled:text-ink-soft"
                    >
                        <RefreshCw
                            className={`h-3.5 w-3.5 ${isFetchingSource ? 'animate-spin' : ''}`}
                            aria-hidden="true"
                        />
                        Fetch now
                    </button>
                </div>
            ) : null}

            {managedSourceId ? (
                <div className="mt-3 border-t border-rule pt-3">
                    {isEditing ? (
                        <form
                            onSubmit={(event) => {
                                event.preventDefault();
                                onSubmitEdit(managedSourceId);
                            }}
                            className="mb-4 grid gap-3 border border-rule bg-surface-sunk p-3 sm:grid-cols-2"
                        >
                            <label className="grid gap-1 text-[12px] text-ink-soft">
                                Name
                                <input
                                    value={editSourceName}
                                    onChange={(event) => onEditSourceNameChange(event.target.value)}
                                    className="h-9 min-w-0 border border-rule bg-surface-raised px-3 text-[13px] text-ink outline-none focus:border-accent"
                                />
                            </label>
                            <label className="grid gap-1 text-[12px] text-ink-soft">
                                Careers URL
                                <input
                                    value={editSourceUrl}
                                    onChange={(event) => onEditSourceUrlChange(event.target.value)}
                                    className="h-9 min-w-0 border border-rule bg-surface-raised px-3 text-[13px] text-ink outline-none focus:border-accent"
                                />
                            </label>
                            <label className="grid gap-1 text-[12px] text-ink-soft">
                                Provider
                                <select
                                    value={editSourceProvider}
                                    onChange={(event) => onEditSourceProviderChange(event.target.value)}
                                    className="h-9 min-w-0 border border-rule bg-surface-raised px-3 text-[13px] text-ink outline-none focus:border-accent"
                                >
                                    <option value="">Auto</option>
                                    <option value="greenhouse">Greenhouse</option>
                                    <option value="lever">Lever</option>
                                    <option value="ashby">Ashby</option>
                                </select>
                            </label>
                            <label className="grid gap-1 text-[12px] text-ink-soft">
                                Board ID
                                <input
                                    value={editSourceIdentifier}
                                    onChange={(event) => onEditSourceIdentifierChange(event.target.value)}
                                    className="h-9 min-w-0 border border-rule bg-surface-raised px-3 text-[13px] text-ink outline-none focus:border-accent"
                                />
                            </label>
                            <label className="grid gap-1 text-[12px] text-ink-soft">
                                Sync minutes
                                <input
                                    type="number"
                                    min={5}
                                    max={1440}
                                    value={editSyncInterval}
                                    onChange={(event) => onEditSyncIntervalChange(event.target.value)}
                                    className="h-9 min-w-0 border border-rule bg-surface-raised px-3 text-[13px] text-ink outline-none focus:border-accent"
                                />
                            </label>
                            <div className="flex items-end gap-2">
                                <button
                                    type="submit"
                                    disabled={isUpdatingAtsSource}
                                    className="inline-flex h-9 items-center gap-1.5 border border-accent px-3 text-[13px] font-medium text-accent transition-colors hover:bg-accent-soft disabled:cursor-not-allowed disabled:border-rule disabled:text-ink-soft"
                                >
                                    <Check className="h-3.5 w-3.5" aria-hidden="true" />
                                    Save
                                </button>
                                <button
                                    type="button"
                                    onClick={onCancelEdit}
                                    disabled={isUpdatingAtsSource}
                                    className="inline-flex h-9 items-center gap-1.5 border border-rule px-3 text-[13px] font-medium text-ink-soft transition-colors hover:border-accent hover:text-accent disabled:cursor-not-allowed disabled:text-ink-soft"
                                >
                                    <X className="h-3.5 w-3.5" aria-hidden="true" />
                                    Cancel
                                </button>
                            </div>
                        </form>
                    ) : null}

                    <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
                        <div className="text-[12px] leading-5 text-ink-soft">
                            User-managed ATS source
                        </div>
                        <div className="flex flex-wrap justify-end gap-2">
                            <button
                                type="button"
                                onClick={() => onSyncAtsSource(managedSourceId)}
                                disabled={isDisabled || isSyncingAtsSource}
                                className="inline-flex min-h-9 items-center gap-1.5 border border-accent px-3 py-1 text-[12px] font-medium text-accent transition-colors hover:bg-accent-soft disabled:cursor-not-allowed disabled:border-rule disabled:text-ink-soft"
                            >
                                <RefreshCw
                                    className={`h-3.5 w-3.5 ${isSyncingAtsSource ? 'animate-spin' : ''}`}
                                    aria-hidden="true"
                                />
                                Sync now
                            </button>
                            <button
                                type="button"
                                onClick={() => onToggleAtsSource(managedSourceId, isDisabled ? 'active' : 'disabled')}
                                disabled={isMutatingAtsSource}
                                className="inline-flex min-h-9 items-center gap-1.5 border border-rule px-3 py-1 text-[12px] font-medium text-ink-soft transition-colors hover:border-accent hover:text-accent disabled:cursor-not-allowed disabled:text-ink-soft"
                            >
                                {isDisabled ? (
                                    <Zap className="h-3.5 w-3.5" aria-hidden="true" />
                                ) : (
                                    <PauseCircle className="h-3.5 w-3.5" aria-hidden="true" />
                                )}
                                {isDisabled ? 'Enable' : 'Disable'}
                            </button>
                            <button
                                type="button"
                                onClick={() => onEditAtsSource(source)}
                                disabled={isMutatingAtsSource || isEditing}
                                className="inline-flex min-h-9 items-center gap-1.5 border border-rule px-3 py-1 text-[12px] font-medium text-ink-soft transition-colors hover:border-accent hover:text-accent disabled:cursor-not-allowed disabled:text-ink-soft"
                            >
                                <Pencil className="h-3.5 w-3.5" aria-hidden="true" />
                                Edit
                            </button>
                            <button
                                type="button"
                                onClick={() => onDeleteAtsSource(source)}
                                disabled={isMutatingAtsSource}
                                className="inline-flex min-h-9 items-center gap-1.5 border border-warn/50 px-3 py-1 text-[12px] font-medium text-warn transition-colors hover:bg-warn-soft disabled:cursor-not-allowed disabled:border-rule disabled:text-ink-soft"
                            >
                                <Trash2 className="h-3.5 w-3.5" aria-hidden="true" />
                                Delete
                            </button>
                        </div>
                    </div>
                </div>
            ) : source.fetch_mode === 'ats_api' ? (
                <div className="mt-3 border-t border-rule pt-3 text-[12px] leading-5 text-ink-soft">
                    Tenant-managed ATS source
                </div>
            ) : null}
        </div>
    );
}

function SourceDeleteDialog({
    source,
    isDeleting,
    onCancel,
    onConfirm,
}: Readonly<{
    source: FetchSource;
    isDeleting: boolean;
    onCancel: () => void;
    onConfirm: () => void;
}>) {
    const dialogRef = useRef<HTMLDialogElement>(null);
    const cancelButtonRef = useRef<HTMLButtonElement>(null);
    const previouslyFocusedRef = useRef<HTMLElement | null>(null);
    const isDeletingRef = useRef(isDeleting);
    const onCancelRef = useRef(onCancel);

    useEffect(() => {
        isDeletingRef.current = isDeleting;
        onCancelRef.current = onCancel;
    }, [isDeleting, onCancel]);

    useEffect(() => {
        const dialog = dialogRef.current;
        previouslyFocusedRef.current = document.activeElement instanceof HTMLElement
            ? document.activeElement
            : null;
        if (dialog && !dialog.open) {
            if (typeof dialog.showModal === 'function') {
                dialog.showModal();
            } else {
                dialog.setAttribute('open', '');
            }
        }
        cancelButtonRef.current?.focus();

        function handleWindowKeyDown(event: globalThis.KeyboardEvent) {
            if (event.key !== 'Escape') return;
            event.preventDefault();
            event.stopPropagation();
            if (!isDeletingRef.current) {
                onCancelRef.current();
            }
        }

        window.addEventListener('keydown', handleWindowKeyDown);

        return () => {
            window.removeEventListener('keydown', handleWindowKeyDown);
            if (dialog?.open) {
                if (typeof dialog.close === 'function') {
                    dialog.close();
                } else {
                    dialog.removeAttribute('open');
                }
            }
            const previouslyFocused = previouslyFocusedRef.current;
            if (previouslyFocused && document.contains(previouslyFocused)) {
                previouslyFocused.focus();
            }
        };
    }, []);

    function handleCancel(event: SyntheticEvent<HTMLDialogElement>) {
        event.preventDefault();
        if (!isDeleting) {
            onCancel();
        }
    }

    return (
        <dialog
            ref={dialogRef}
            aria-labelledby="delete-source-title"
            aria-describedby="delete-source-description"
            onCancel={handleCancel}
            className="m-auto w-[calc(100%-2rem)] max-w-md border border-rule bg-surface px-5 py-4 text-ink shadow-lg backdrop:bg-ink/20"
        >
            <div className="flex items-start gap-3">
                <AlertTriangle className="mt-0.5 h-5 w-5 flex-shrink-0 text-warn" aria-hidden="true" />
                <div>
                    <h4 id="delete-source-title" className="text-[15px] font-medium text-ink">
                        Delete ATS source?
                    </h4>
                    <p id="delete-source-description" className="mt-2 text-[13px] leading-5 text-ink-muted">
                        {source.display_name} will be removed from active syncing. Its recent activity remains available so it can be re-added later.
                    </p>
                </div>
            </div>
            <div className="mt-5 flex justify-end gap-2">
                <button
                    ref={cancelButtonRef}
                    type="button"
                    onClick={onCancel}
                    disabled={isDeleting}
                    className="inline-flex h-9 items-center justify-center border border-rule px-3 text-[13px] font-medium text-ink-soft transition-colors hover:border-accent hover:text-accent disabled:cursor-not-allowed disabled:text-ink-soft"
                >
                    Cancel
                </button>
                <button
                    type="button"
                    onClick={onConfirm}
                    disabled={isDeleting}
                    className="inline-flex h-9 items-center justify-center gap-1.5 border border-warn/50 px-3 text-[13px] font-medium text-warn transition-colors hover:bg-warn-soft disabled:cursor-not-allowed disabled:border-rule disabled:text-ink-soft"
                >
                    <Trash2 className="h-3.5 w-3.5" aria-hidden="true" />
                    {isDeleting ? 'Deleting' : 'Delete source'}
                </button>
            </div>
        </dialog>
    );
}

function sourceEmptyMessage(sourceSearch: string, sourceView: SourceView): string {
    if (sourceSearch.trim()) return 'No sources match that search.';
    if (sourceView !== 'all') return 'No sources in this view.';
    return 'No job sources configured.';
}

function sourceLoadMessages(args: {
    isSourcesError: boolean;
    sourcesError: unknown;
    isCloudError: boolean;
    cloudError: unknown;
    isUserSourcesError: boolean;
    userSourcesError: unknown;
    isHistoryError: boolean;
    historyError: unknown;
}): string[] {
    const messages: string[] = [];
    if (args.isSourcesError) messages.push(`Catalog: ${apiErrorMessage(args.sourcesError)}`);
    if (args.isCloudError) messages.push(`Tenant ATS sources: ${apiErrorMessage(args.cloudError)}`);
    if (args.isUserSourcesError) messages.push(`Your ATS sources: ${apiErrorMessage(args.userSourcesError)}`);
    if (args.isHistoryError) messages.push(`Activity: ${apiErrorMessage(args.historyError)}`);
    return messages;
}

function filterButtonClasses(isActive: boolean): string {
    const activeClasses = 'border-accent bg-accent-soft text-accent';
    const inactiveClasses = 'border-rule bg-surface text-ink-soft hover:border-accent hover:text-accent';
    return `inline-flex min-h-8 items-center gap-1.5 border px-2.5 py-1 text-[12px] font-medium transition-colors ${
        isActive ? activeClasses : inactiveClasses
    }`;
}

function SourcePanelHeader({
    visibleCount,
    totalCount,
    isAddingSource,
    showHistory,
    sourceSearch,
    sourceView,
    sourceViewCounts,
    catalogLabel,
    onToggleAddingSource,
    onToggleHistory,
    onSourceSearchChange,
    onSourceViewChange,
}: Readonly<{
    visibleCount: number;
    totalCount: number;
    isAddingSource: boolean;
    showHistory: boolean;
    sourceSearch: string;
    sourceView: SourceView;
    sourceViewCounts: Record<SourceView, number>;
    catalogLabel: string;
    onToggleAddingSource: () => void;
    onToggleHistory: () => void;
    onSourceSearchChange: (value: string) => void;
    onSourceViewChange: (view: SourceView) => void;
}>) {
    return (
        <div className="mb-4 flex flex-col gap-4">
            <div className="flex flex-col gap-3 xl:flex-row xl:items-end xl:justify-between">
                <div>
                    <p className="caption">Provider management</p>
                    <h3 className="mt-1 flex items-baseline gap-2 text-[16px] font-medium text-ink">
                        <span>Job Sources</span>
                        <span className="text-[12px] font-normal text-ink-soft">
                            {visibleCount}/{totalCount}
                        </span>
                    </h3>
                    <p className="mt-1 max-w-2xl text-[12px] leading-5 text-ink-muted">
                        Add supported ATS boards, monitor seed/API health, and keep paused or deleted sources recoverable from activity.
                    </p>
                </div>
                <div className="flex flex-col gap-2 sm:flex-row sm:items-center">
                    <button
                        type="button"
                        onClick={onToggleAddingSource}
                        aria-expanded={isAddingSource}
                        aria-controls="add-source-form"
                        className="inline-flex h-9 items-center justify-center gap-1.5 border border-accent px-3 text-[13px] font-medium text-accent transition-colors hover:bg-accent-soft"
                    >
                        <Plus className="h-3.5 w-3.5" aria-hidden="true" />
                        Add source
                    </button>
                    <button
                        type="button"
                        onClick={onToggleHistory}
                        aria-expanded={showHistory}
                        aria-controls="source-activity-panel"
                        className="inline-flex h-9 items-center justify-center gap-1.5 border border-rule px-3 text-[13px] font-medium text-ink-soft transition-colors hover:border-accent hover:text-accent"
                    >
                        <History className="h-3.5 w-3.5" aria-hidden="true" />
                        Activity
                    </button>
                    <label className="relative block">
                        <Search className="pointer-events-none absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-ink-soft" aria-hidden="true" />
                        <input
                            aria-label="Search sources"
                            value={sourceSearch}
                            onChange={(event) => onSourceSearchChange(event.target.value)}
                            placeholder="Search sources"
                            className="h-9 w-full border border-rule bg-surface pl-8 pr-3 text-[13px] text-ink outline-none transition-colors placeholder:text-ink-soft focus:border-accent sm:w-52"
                        />
                    </label>
                    <div className="inline-flex items-center gap-2 self-start border border-rule bg-surface px-2.5 py-1.5 text-[12px] text-ink-soft">
                        <Server className="h-3.5 w-3.5 text-accent" aria-hidden="true" />
                        <span>{catalogLabel}</span>
                    </div>
                </div>
            </div>

            <div className="flex flex-wrap gap-2" aria-label="Source views">
                {SOURCE_VIEW_OPTIONS.map((option) => (
                    <button
                        key={option.key}
                        type="button"
                        aria-pressed={sourceView === option.key}
                        onClick={() => onSourceViewChange(option.key)}
                        className={filterButtonClasses(sourceView === option.key)}
                    >
                        {option.label}
                        <span className="tabular-nums text-[11px]">{sourceViewCounts[option.key]}</span>
                    </button>
                ))}
            </div>
        </div>
    );
}

function SourceLoadErrorBanner({
    messages,
    onRetry,
}: Readonly<{
    messages: string[];
    onRetry: () => void;
}>) {
    return (
        <div role="alert" className="mb-4 border border-warn/40 bg-warn-soft px-4 py-3 text-[12px] text-ink">
            <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
                <div className="flex gap-2">
                    <AlertTriangle className="mt-0.5 h-4 w-4 flex-shrink-0 text-warn" aria-hidden="true" />
                    <div>
                        <p className="font-medium">Some source status could not be loaded.</p>
                        <ul className="mt-1 grid gap-1 text-ink-muted">
                            {messages.map((message) => (
                                <li key={message}>{message}</li>
                            ))}
                        </ul>
                    </div>
                </div>
                <button
                    type="button"
                    onClick={onRetry}
                    className="inline-flex h-8 items-center justify-center gap-1.5 border border-rule bg-surface px-2.5 text-[12px] font-medium text-ink-soft transition-colors hover:border-accent hover:text-accent"
                >
                    <RefreshCw className="h-3.5 w-3.5" aria-hidden="true" />
                    Retry
                </button>
            </div>
        </div>
    );
}

function AddSourceForm({
    newSourceName,
    newSourceUrl,
    newSourceProvider,
    newSourceIdentifier,
    discoveryError,
    hasCheckedDiscovery,
    discoveryCandidates,
    isChecking,
    isAdding,
    onSubmit,
    onCheck,
    onNameChange,
    onUrlChange,
    onProviderChange,
    onIdentifierChange,
    onAddDiscoveredSource,
}: Readonly<{
    newSourceName: string;
    newSourceUrl: string;
    newSourceProvider: string;
    newSourceIdentifier: string;
    discoveryError: string | null;
    hasCheckedDiscovery: boolean;
    discoveryCandidates: AtsSourceDiscoveryCandidate[];
    isChecking: boolean;
    isAdding: boolean;
    onSubmit: FormSubmitHandler;
    onCheck: () => void;
    onNameChange: (value: string) => void;
    onUrlChange: (value: string) => void;
    onProviderChange: (value: string) => void;
    onIdentifierChange: (value: string) => void;
    onAddDiscoveredSource: (candidate: AtsSourceDiscoveryCandidate) => void;
}>) {
    return (
        <form
            id="add-source-form"
            onSubmit={onSubmit}
            className="mb-4 grid gap-4 border border-rule bg-surface px-4 py-4"
        >
            <div className="flex flex-col gap-3 border-b border-rule pb-4 md:flex-row md:items-start md:justify-between">
                <div className="flex gap-3">
                    <ShieldCheck className="mt-0.5 h-5 w-5 text-accent" aria-hidden="true" />
                    <div>
                        <p className="text-[14px] font-medium text-ink">Add a supported ATS board</p>
                        <p className="mt-1 max-w-2xl text-[12px] leading-5 text-ink-muted">
                            Enter a company name, supported careers URL, or board ID. Arbitrary websites are blocked; discovery checks supported ATS providers only.
                        </p>
                    </div>
                </div>
                <div className="flex flex-wrap gap-1.5 text-[11px] text-ink-soft">
                    <span className={metaChipClasses('uppercase tracking-[0.12em]')}>Search order</span>
                    {ATS_PROVIDER_SEARCH_ORDER.map((provider) => (
                        <span key={provider} className={metaChipClasses()}>
                            {provider}
                        </span>
                    ))}
                </div>
            </div>

            <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-[minmax(0,1fr)_minmax(0,1.3fr)_10rem_minmax(0,1fr)]">
                <div>
                    <label htmlFor="new-source-name" className="text-[12px] text-ink-soft">
                        Name
                    </label>
                    <input
                        id="new-source-name"
                        value={newSourceName}
                        onChange={(event) => onNameChange(event.target.value)}
                        placeholder="Company or board"
                        className="mt-1 h-9 w-full border border-rule bg-surface-raised px-3 text-[13px] text-ink outline-none placeholder:text-ink-soft focus:border-accent"
                    />
                    <p className="mt-1 text-[11px] leading-4 text-ink-soft">
                        Works alone for discovery when the company name is distinctive.
                    </p>
                </div>
                <div>
                    <label htmlFor="new-source-url" className="text-[12px] text-ink-soft">
                        Careers URL
                    </label>
                    <input
                        id="new-source-url"
                        value={newSourceUrl}
                        onChange={(event) => onUrlChange(event.target.value)}
                        placeholder="https://boards.greenhouse.io/acme"
                        className="mt-1 h-9 w-full border border-rule bg-surface-raised px-3 text-[13px] text-ink outline-none placeholder:text-ink-soft focus:border-accent"
                    />
                    <p className="mt-1 text-[11px] leading-4 text-ink-soft">
                        Only Greenhouse, Lever, and Ashby board URLs are accepted.
                    </p>
                </div>
                <div>
                    <label htmlFor="new-source-provider" className="text-[12px] text-ink-soft">
                        Provider
                    </label>
                    <select
                        id="new-source-provider"
                        value={newSourceProvider}
                        onChange={(event) => onProviderChange(event.target.value)}
                        className="mt-1 h-9 w-full border border-rule bg-surface-raised px-3 text-[13px] text-ink outline-none focus:border-accent"
                    >
                        <option value="">Auto</option>
                        <option value="greenhouse">Greenhouse</option>
                        <option value="lever">Lever</option>
                        <option value="ashby">Ashby</option>
                    </select>
                </div>
                <div>
                    <label htmlFor="new-source-identifier" className="text-[12px] text-ink-soft">
                        Board ID
                    </label>
                    <input
                        id="new-source-identifier"
                        value={newSourceIdentifier}
                        onChange={(event) => onIdentifierChange(event.target.value)}
                        placeholder="acme"
                        className="mt-1 h-9 w-full border border-rule bg-surface-raised px-3 text-[13px] text-ink outline-none placeholder:text-ink-soft focus:border-accent"
                    />
                    <p className="mt-1 text-[11px] leading-4 text-ink-soft">
                        Use with a provider, or leave provider on Auto to probe all supported boards.
                    </p>
                </div>
            </div>

            <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-end">
                <button
                    type="button"
                    onClick={onCheck}
                    disabled={isChecking}
                    className="inline-flex h-9 items-center justify-center gap-1.5 border border-rule px-3 text-[13px] font-medium text-ink-soft transition-colors hover:border-accent hover:text-accent disabled:cursor-not-allowed disabled:border-rule disabled:text-ink-soft"
                >
                    <Search className="h-3.5 w-3.5" aria-hidden="true" />
                    {isChecking ? 'Checking' : 'Check'}
                </button>
                <button
                    type="submit"
                    disabled={isAdding}
                    className="inline-flex h-9 items-center justify-center border border-accent px-3 text-[13px] font-medium text-accent transition-colors hover:bg-accent-soft disabled:cursor-not-allowed disabled:border-rule disabled:text-ink-soft"
                >
                    {isAdding ? 'Adding' : 'Add'}
                </button>
            </div>

            {discoveryError ? (
                <div className="flex gap-2 border border-warn/40 bg-warn-soft px-3 py-2 text-[12px] text-ink">
                    <AlertTriangle className="mt-0.5 h-3.5 w-3.5 flex-shrink-0 text-warn" aria-hidden="true" />
                    <span>{discoveryError}</span>
                </div>
            ) : null}

            {hasCheckedDiscovery && discoveryCandidates.length === 0 && !discoveryError ? (
                <div className="border border-dashed border-rule bg-surface-sunk px-3 py-3 text-[12px] leading-5 text-ink-muted">
                    No supported ATS board matched those inputs. Try a provider-specific board ID or a supported board URL.
                </div>
            ) : null}

            {discoveryCandidates.length > 0 ? (
                <div className="grid gap-2 border-t border-rule pt-4">
                    <div className="flex items-center justify-between gap-3">
                        <p className="caption">Discovery candidates</p>
                        <span className="text-[12px] text-ink-soft">
                            {discoveryCandidates.length} found
                        </span>
                    </div>
                    {discoveryCandidates.map((candidate) => (
                        <div
                            key={`${candidate.provider}-${candidate.identifier}`}
                            className="flex flex-col gap-3 border border-rule bg-surface-raised px-3 py-3 sm:flex-row sm:items-start sm:justify-between"
                        >
                            <div className="min-w-0">
                                <div className="flex flex-wrap items-center gap-2">
                                    <span className="text-[13px] font-medium text-ink">
                                        {candidate.display_name}
                                    </span>
                                    <span className={metaChipClasses('capitalize')}>
                                        {candidate.provider}
                                    </span>
                                    <span className={metaChipClasses('tabular-nums')}>
                                        {candidate.jobs_seen} jobs
                                    </span>
                                </div>
                                <p className="mt-1 break-all text-[12px] text-ink-soft">
                                    {candidate.identifier}
                                </p>
                                {candidate.source_url ? (
                                    <p className="mt-1 break-all text-[12px] text-ink-soft">
                                        {candidate.source_url}
                                    </p>
                                ) : null}
                                <p className="mt-2 text-[12px] leading-5 text-ink-muted">
                                    {candidate.match_reason}
                                </p>
                            </div>
                            <button
                                type="button"
                                onClick={() => onAddDiscoveredSource(candidate)}
                                disabled={isAdding}
                                className="inline-flex h-9 items-center justify-center border border-accent px-3 text-[12px] font-medium text-accent transition-colors hover:bg-accent-soft disabled:cursor-not-allowed disabled:border-rule disabled:text-ink-soft"
                            >
                                Add
                            </button>
                        </div>
                    ))}
                </div>
            ) : null}
        </form>
    );
}

function SourceActivityPanel({
    sourceActivityFilter,
    sourceActivityCounts,
    sourceHistory,
    filteredSourceHistory,
    isAdding,
    onFilterChange,
    onHide,
    onReaddSource,
}: Readonly<{
    sourceActivityFilter: SourceActivityFilter;
    sourceActivityCounts: Record<SourceActivityFilter, number>;
    sourceHistory: AtsSourceHistoryEvent[];
    filteredSourceHistory: AtsSourceHistoryEvent[];
    isAdding: boolean;
    onFilterChange: (filter: SourceActivityFilter) => void;
    onHide: () => void;
    onReaddSource: (event: AtsSourceHistoryEvent) => void;
}>) {
    const emptyHistoryMessage = sourceHistory.length === 0
        ? 'No source activity recorded yet.'
        : 'No activity matches this filter.';

    return (
        <div id="source-activity-panel" className="mb-4 border border-rule bg-surface px-4 py-3">
            <div className="mb-3 flex items-center justify-between gap-3">
                <div>
                    <p className="caption">Source activity</p>
                    <p className="mt-1 text-[12px] text-ink-muted">
                        Recent add, update, pause, delete, and re-add events.
                    </p>
                </div>
                <button
                    type="button"
                    onClick={onHide}
                    className="inline-flex h-8 w-8 items-center justify-center border border-rule text-ink-soft transition-colors hover:border-accent hover:text-accent"
                    aria-label="Hide source activity"
                >
                    <X className="h-3.5 w-3.5" aria-hidden="true" />
                </button>
            </div>
            <div className="mb-3 flex flex-wrap gap-2" aria-label="Source activity filters">
                {SOURCE_ACTIVITY_FILTER_OPTIONS.map((option) => (
                    <button
                        key={option.key}
                        type="button"
                        aria-pressed={sourceActivityFilter === option.key}
                        onClick={() => onFilterChange(option.key)}
                        className={filterButtonClasses(sourceActivityFilter === option.key)}
                    >
                        {option.label}
                        <span className="tabular-nums text-[11px]">{sourceActivityCounts[option.key]}</span>
                    </button>
                ))}
            </div>
            {filteredSourceHistory.length > 0 ? (
                <div className="grid gap-2">
                    {filteredSourceHistory.map((event) => (
                        <div
                            key={event.id}
                            className="flex flex-col gap-2 border border-rule bg-surface-raised px-3 py-2 sm:flex-row sm:items-center sm:justify-between"
                        >
                            <div className="min-w-0">
                                <div className="flex flex-wrap items-center gap-2">
                                    <span className="text-[13px] font-medium text-ink">
                                        {event.display_name || event.identifier || event.provider || 'ATS source'}
                                    </span>
                                    <span className={metaChipClasses()}>
                                        {historyActionLabel(event.action)}
                                    </span>
                                    {event.provider ? (
                                        <span className={metaChipClasses('capitalize')}>
                                            {event.provider}
                                        </span>
                                    ) : null}
                                    <span className={metaChipClasses('tabular-nums')}>
                                        {formattedDateTime(event.occurred_at)}
                                    </span>
                                </div>
                                {event.identifier ? (
                                    <p className="mt-1 truncate text-[12px] text-ink-soft">
                                        {event.identifier}
                                    </p>
                                ) : null}
                            </div>
                            {event.readd_payload ? (
                                <button
                                    type="button"
                                    onClick={() => onReaddSource(event)}
                                    disabled={isAdding}
                                    className="inline-flex h-8 items-center justify-center gap-1.5 border border-rule px-2.5 text-[12px] font-medium text-ink-soft transition-colors hover:border-accent hover:text-accent disabled:cursor-not-allowed disabled:text-ink-soft"
                                >
                                    <RotateCcw className="h-3.5 w-3.5" aria-hidden="true" />
                                    Re-add
                                </button>
                            ) : null}
                        </div>
                    ))}
                </div>
            ) : (
                <div className="border border-dashed border-rule bg-surface-sunk px-3 py-4 text-[12px] text-ink-muted">
                    {emptyHistoryMessage}
                </div>
            )}
        </div>
    );
}

function SourcesContent({
    isLoading,
    sources,
    emptyMessage,
    fetchingSourceSiteType,
    syncingSourceId,
    updatingSourceId,
    deletingSourceId,
    editingSourceId,
    editSourceName,
    editSourceUrl,
    editSourceProvider,
    editSourceIdentifier,
    editSyncInterval,
    onFetchSource,
    onSyncAtsSource,
    onEditAtsSource,
    onToggleAtsSource,
    onDeleteAtsSource,
    onSubmitEdit,
    onCancelEdit,
    onEditSourceNameChange,
    onEditSourceUrlChange,
    onEditSourceProviderChange,
    onEditSourceIdentifierChange,
    onEditSyncIntervalChange,
}: Readonly<{
    isLoading: boolean;
    sources: FetchSource[];
    emptyMessage: string;
    fetchingSourceSiteType: string | null;
    syncingSourceId: string | null;
    updatingSourceId: string | null;
    deletingSourceId: string | null;
    editingSourceId: string | null;
    editSourceName: string;
    editSourceUrl: string;
    editSourceProvider: string;
    editSourceIdentifier: string;
    editSyncInterval: string;
    onFetchSource: (siteType: string) => void;
    onSyncAtsSource: (sourceId: string) => void;
    onEditAtsSource: (source: FetchSource) => void;
    onToggleAtsSource: (sourceId: string, status: string) => void;
    onDeleteAtsSource: (source: FetchSource) => void;
    onSubmitEdit: (sourceId: string) => void;
    onCancelEdit: () => void;
    onEditSourceNameChange: (value: string) => void;
    onEditSourceUrlChange: (value: string) => void;
    onEditSourceProviderChange: (value: string) => void;
    onEditSourceIdentifierChange: (value: string) => void;
    onEditSyncIntervalChange: (value: string) => void;
}>) {
    if (isLoading) {
        return (
            <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
                {[0, 1, 2].map((item) => (
                    <div key={item} className="h-48 animate-pulse border border-rule bg-surface-sunk" />
                ))}
            </div>
        );
    }

    if (sources.length === 0) {
        return (
            <div className="border border-dashed border-rule bg-surface px-4 py-6 text-[13px] text-ink-muted">
                <div className="flex items-start gap-3">
                    <ListFilter className="mt-0.5 h-4 w-4 text-accent" aria-hidden="true" />
                    <div>
                        <p className="font-medium text-ink">{emptyMessage}</p>
                        <p className="mt-1 leading-5">
                            Add a supported ATS board or relax the current search and status filters.
                        </p>
                    </div>
                </div>
            </div>
        );
    }

    return (
        <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
            {sources.map((source, index) => {
                const managedSourceId = userSourceId(source);
                return (
                    <SourceCard
                        key={`${source.site_type}-${index}`}
                        source={source}
                        index={index}
                        onFetchSource={onFetchSource}
                        isFetchingSource={fetchingSourceSiteType === source.site_type}
                        onSyncAtsSource={onSyncAtsSource}
                        onEditAtsSource={onEditAtsSource}
                        onToggleAtsSource={onToggleAtsSource}
                        onDeleteAtsSource={onDeleteAtsSource}
                        onSubmitEdit={onSubmitEdit}
                        onCancelEdit={onCancelEdit}
                        isSyncingAtsSource={syncingSourceId === managedSourceId}
                        isUpdatingAtsSource={updatingSourceId === managedSourceId}
                        isDeletingAtsSource={deletingSourceId === managedSourceId}
                        isEditing={editingSourceId === managedSourceId}
                        editSourceName={editSourceName}
                        editSourceUrl={editSourceUrl}
                        editSourceProvider={editSourceProvider}
                        editSourceIdentifier={editSourceIdentifier}
                        editSyncInterval={editSyncInterval}
                        onEditSourceNameChange={onEditSourceNameChange}
                        onEditSourceUrlChange={onEditSourceUrlChange}
                        onEditSourceProviderChange={onEditSourceProviderChange}
                        onEditSourceIdentifierChange={onEditSourceIdentifierChange}
                        onEditSyncIntervalChange={onEditSyncIntervalChange}
                    />
                );
            })}
        </div>
    );
}

export function FetchSourcesPanel() {
    const [sourceSearch, setSourceSearch] = useState('');
    const [sourceView, setSourceView] = useState<SourceView>('all');
    const [sourceActivityFilter, setSourceActivityFilter] = useState<SourceActivityFilter>('all');
    const [isAddingSource, setIsAddingSource] = useState(false);
    const [showHistory, setShowHistory] = useState(false);
    const [newSourceName, setNewSourceName] = useState('');
    const [newSourceUrl, setNewSourceUrl] = useState('');
    const [newSourceProvider, setNewSourceProvider] = useState('');
    const [newSourceIdentifier, setNewSourceIdentifier] = useState('');
    const [discoveryError, setDiscoveryError] = useState<string | null>(null);
    const [hasCheckedDiscovery, setHasCheckedDiscovery] = useState(false);
    const [editingSourceId, setEditingSourceId] = useState<string | null>(null);
    const [editSourceName, setEditSourceName] = useState('');
    const [editSourceUrl, setEditSourceUrl] = useState('');
    const [editSourceProvider, setEditSourceProvider] = useState('');
    const [editSourceIdentifier, setEditSourceIdentifier] = useState('');
    const [editSyncInterval, setEditSyncInterval] = useState('');
    const [editOriginalSource, setEditOriginalSource] = useState<{
        sourceUrl: string;
        provider: string;
        identifier: string;
    } | null>(null);
    const [discoveryCandidates, setDiscoveryCandidates] = useState<AtsSourceDiscoveryCandidate[]>([]);
    const [discoveryPayload, setDiscoveryPayload] = useState<AtsSourceCreateRequest | null>(null);
    const [sourcePendingDelete, setSourcePendingDelete] = useState<FetchSource | null>(null);
    const latestDiscoveryKeyRef = useRef('');
    const queryClient = useQueryClient();
    const {
        data,
        isLoading,
        isError: isSourcesError,
        error: sourcesError,
        refetch: refetchSources,
    } = useQuery({
        queryKey: ['pipeline', 'sources'],
        queryFn: async () => {
            const response = await pipelineApi.getSources({
                includeStatus: true,
            });
            return response.data;
        },
        staleTime: 5 * 60 * 1000,
    });
    const fetchSourceMutation = useMutation({
        mutationFn: async (source: string) => {
            const response = await pipelineApi.fetchSource(source);
            return response.data;
        },
        onSuccess: (result) => {
            toast.success(`${result.imported_count} jobs imported from ${toTitleCase(result.source)}`);
            void queryClient.invalidateQueries({ queryKey: ['pipeline', 'sources'] });
        },
        onError: (error) => {
            toast.error(`Source fetch failed: ${apiErrorMessage(error)}`);
        },
    });
    const {
        data: cloudIntegrations = [],
        isLoading: isLoadingCloud,
        isError: isCloudError,
        error: cloudError,
        refetch: refetchCloudIntegrations,
    } = useQuery({
        queryKey: ['cloud', 'integrations', 'source-panel'],
        queryFn: async () => {
            const response = await pipelineApi.getCloudIntegrations();
            return response.status === 200 && Array.isArray(response.data) ? response.data : [];
        },
        staleTime: 5 * 60 * 1000,
    });
    const {
        data: userAtsSources = [],
        isLoading: isLoadingUserSources,
        isError: isUserSourcesError,
        error: userSourcesError,
        refetch: refetchUserSources,
    } = useQuery({
        queryKey: ['cloud', 'integrations', 'user-sources'],
        queryFn: async () => {
            const response = await pipelineApi.getUserAtsSources();
            return response.status === 200 && Array.isArray(response.data) ? response.data : [];
        },
        staleTime: 60 * 1000,
    });
    const {
        data: sourceHistory = [],
        isError: isHistoryError,
        error: historyError,
        refetch: refetchSourceHistory,
    } = useQuery({
        queryKey: ['cloud', 'integrations', 'user-sources-history'],
        queryFn: async () => {
            const response = await pipelineApi.getUserAtsSourceHistory();
            return response.status === 200 && Array.isArray(response.data) ? response.data : [];
        },
        staleTime: 60 * 1000,
    });
    const discoverUserSourceMutation = useMutation({
        mutationFn: async (payload: AtsSourceCreateRequest) => {
            const response = await pipelineApi.discoverAtsSources(payload);
            return response.data;
        },
        onSuccess: (candidates, variables) => {
            if (sourcePayloadKey(variables) !== latestDiscoveryKeyRef.current) return;
            setDiscoveryPayload(variables);
            setDiscoveryCandidates(candidates);
            setDiscoveryError(null);
            setHasCheckedDiscovery(true);
            if (candidates.length === 0) {
                toast.error('No supported ATS board found.');
            } else {
                toast.success(`${candidates.length} ATS board${candidates.length === 1 ? '' : 's'} found`);
            }
        },
        onError: (error) => {
            const message = apiErrorMessage(error);
            setDiscoveryError(message);
            setHasCheckedDiscovery(true);
            toast.error(`ATS source check failed: ${message}`);
        },
    });
    const createUserSourceMutation = useMutation({
        mutationFn: async (payload: AtsSourceCreateRequest) => {
            const response = await pipelineApi.createUserAtsSource(payload);
            return response.data;
        },
        onSuccess: (source) => {
            toast.success(`${source.display_name} added`);
            if (source.initial_sync?.status === 'completed') {
                toast.success(`${source.initial_sync.jobs_imported} jobs imported from ${toTitleCase(source.initial_sync.provider)}`);
            } else if (source.initial_sync?.error_summary) {
                toast(`Initial sync ${source.initial_sync.status}: ${source.initial_sync.error_summary}`);
            }
            setIsAddingSource(false);
            setNewSourceName('');
            setNewSourceUrl('');
            setNewSourceProvider('');
            setNewSourceIdentifier('');
            setDiscoveryCandidates([]);
            setDiscoveryPayload(null);
            setDiscoveryError(null);
            setHasCheckedDiscovery(false);
            void queryClient.invalidateQueries({ queryKey: ['cloud', 'integrations', 'source-panel'] });
            void queryClient.invalidateQueries({ queryKey: ['cloud', 'integrations', 'user-sources'] });
            void queryClient.invalidateQueries({ queryKey: ['cloud', 'integrations', 'user-sources-history'] });
        },
        onError: (error) => {
            toast.error(`ATS source add failed: ${apiErrorMessage(error)}`);
        },
    });
    const updateUserSourceMutation = useMutation({
        mutationFn: async ({ sourceId, payload }: { sourceId: string; payload: AtsSourceUpdateRequest }) => {
            const response = await pipelineApi.updateUserAtsSource(sourceId, payload);
            return response.data;
        },
        onSuccess: (source, variables) => {
            const updatedStatus = variables.payload.status;
            if (updatedStatus) {
                toast.success(`${source.display_name} ${source.status === 'disabled' ? 'disabled' : 'enabled'}`);
            } else {
                toast.success(`${source.display_name} updated`);
                setEditingSourceId(null);
                setEditOriginalSource(null);
            }
            void queryClient.invalidateQueries({ queryKey: ['cloud', 'integrations', 'source-panel'] });
            void queryClient.invalidateQueries({ queryKey: ['cloud', 'integrations', 'user-sources'] });
            void queryClient.invalidateQueries({ queryKey: ['cloud', 'integrations', 'user-sources-history'] });
        },
        onError: (error) => {
            toast.error(`ATS source update failed: ${apiErrorMessage(error)}`);
        },
    });
    const deleteUserSourceMutation = useMutation({
        mutationFn: async (sourceId: string) => {
            await pipelineApi.deleteUserAtsSource(sourceId);
            return sourceId;
        },
        onSuccess: () => {
            toast.success('ATS source deleted');
            setSourcePendingDelete(null);
            void queryClient.invalidateQueries({ queryKey: ['cloud', 'integrations', 'source-panel'] });
            void queryClient.invalidateQueries({ queryKey: ['cloud', 'integrations', 'user-sources'] });
            void queryClient.invalidateQueries({ queryKey: ['cloud', 'integrations', 'user-sources-history'] });
        },
        onError: (error) => {
            toast.error(`ATS source delete failed: ${apiErrorMessage(error)}`);
        },
    });
    const syncUserSourceMutation = useMutation({
        mutationFn: async (sourceId: string) => {
            const response = await pipelineApi.syncUserAtsSource(sourceId, true);
            return response.data;
        },
        onSuccess: (result) => {
            toast.success(`${result.jobs_imported} jobs imported from ${toTitleCase(result.provider)}`);
            void queryClient.invalidateQueries({ queryKey: ['cloud', 'integrations', 'source-panel'] });
            void queryClient.invalidateQueries({ queryKey: ['cloud', 'integrations', 'user-sources'] });
            void queryClient.invalidateQueries({ queryKey: ['cloud', 'integrations', 'user-sources-history'] });
        },
        onError: (error) => {
            toast.error(`ATS source sync failed: ${apiErrorMessage(error)}`);
        },
    });

    const cloudSources = useMemo(
        () => [
            ...cloudIntegrations
                .filter((integration) => integration.is_user_source !== true)
                .map(cloudIntegrationSource),
            ...userAtsSources.map(cloudIntegrationSource),
        ],
        [cloudIntegrations, userAtsSources]
    );
    const allSources = useMemo(
        () => [...(data?.sources ?? []), ...cloudSources],
        [data?.sources, cloudSources]
    );
    const sourceViewCounts = useMemo(
        () => SOURCE_VIEW_OPTIONS.reduce<Record<SourceView, number>>((counts, option) => {
            counts[option.key] = allSources.filter((source) => sourceMatchesView(source, option.key)).length;
            return counts;
        }, {
            all: 0,
            ats: 0,
            seed: 0,
            api: 0,
            paused: 0,
            needs_attention: 0,
        }),
        [allSources]
    );
    const sources = useMemo(
        () => allSources.filter((source) => (
            sourceMatchesView(source, sourceView) && sourceMatchesSearch(source, sourceSearch)
        )),
        [allSources, sourceSearch, sourceView]
    );
    const sourceActivityCounts = useMemo(
        () => SOURCE_ACTIVITY_FILTER_OPTIONS.reduce<Record<SourceActivityFilter, number>>((counts, option) => {
            counts[option.key] = sourceHistory.filter((event) => historyEventMatchesFilter(event, option.key)).length;
            return counts;
        }, {
            all: 0,
            added: 0,
            updated: 0,
            deleted: 0,
            synced: 0,
            recoverable: 0,
        }),
        [sourceHistory]
    );
    const filteredSourceHistory = useMemo(
        () => sourceHistory.filter((event) => historyEventMatchesFilter(event, sourceActivityFilter)),
        [sourceActivityFilter, sourceHistory]
    );
    const totalCount = (data?.total_count ?? (data?.sources ?? []).length) + cloudSources.length;
    const emptyMessage = sourceEmptyMessage(sourceSearch, sourceView);

    function buildUserSourcePayload(): AtsSourceCreateRequest | null {
        const provider = newSourceProvider.trim() || undefined;
        const identifier = newSourceIdentifier.trim() || undefined;
        const sourceUrl = newSourceUrl.trim();
        if (sourceUrl && !isSupportedAtsUrl(sourceUrl)) {
            toast.error('Use a Greenhouse, Lever, or Ashby board URL.');
            return null;
        }
        const payload: AtsSourceCreateRequest = {
            display_name: newSourceName.trim() || undefined,
            source_url: sourceUrl || undefined,
            provider,
            identifier,
            providers: provider && !identifier ? [provider] : undefined,
        };
        if (!payload.display_name && !payload.source_url && !payload.identifier) {
            toast.error('Add a source name, careers URL, or board identifier.');
            return null;
        }
        return payload;
    }

    function clearDiscoveryCandidates() {
        setDiscoveryCandidates([]);
        setDiscoveryPayload(null);
        setDiscoveryError(null);
        setHasCheckedDiscovery(false);
        latestDiscoveryKeyRef.current = '';
    }

    function updateNewSourceName(value: string) {
        setNewSourceName(value);
        clearDiscoveryCandidates();
    }

    function updateNewSourceUrl(value: string) {
        setNewSourceUrl(value);
        clearDiscoveryCandidates();
    }

    function updateNewSourceProvider(value: string) {
        setNewSourceProvider(value);
        clearDiscoveryCandidates();
    }

    function updateNewSourceIdentifier(value: string) {
        setNewSourceIdentifier(value);
        clearDiscoveryCandidates();
    }

    function submitUserSource(event: FormSubmitEvent) {
        event.preventDefault();
        const payload = buildUserSourcePayload();
        if (!payload) return;
        createUserSourceMutation.mutate(payload);
    }

    function checkUserSource() {
        const payload = buildUserSourcePayload();
        if (!payload) return;
        setDiscoveryError(null);
        setHasCheckedDiscovery(false);
        latestDiscoveryKeyRef.current = sourcePayloadKey(payload);
        discoverUserSourceMutation.mutate(payload);
    }

    function addDiscoveredSource(candidate: AtsSourceDiscoveryCandidate) {
        createUserSourceMutation.mutate({
            display_name: discoveryPayload?.display_name || candidate.display_name,
            source_url: candidate.source_url || discoveryPayload?.source_url || undefined,
            provider: candidate.provider,
            identifier: candidate.identifier,
        });
    }

    function readdHistorySource(event: AtsSourceHistoryEvent) {
        if (!event.readd_payload) return;
        createUserSourceMutation.mutate(event.readd_payload);
    }

    function editUserSource(source: FetchSource) {
        const sourceId = userSourceId(source);
        if (!sourceId) return;
        setEditingSourceId(sourceId);
        setEditSourceName(source.display_name);
        setEditSourceUrl(String(source.options?.source_url || source.seed_url || ''));
        setEditSourceProvider(String(source.options?.ats_provider || source.site_type || ''));
        setEditSourceIdentifier(String(source.options?.ats_identifier || ''));
        setEditSyncInterval(String(atsInterval(source) ?? 120));
        setEditOriginalSource({
            sourceUrl: String(source.options?.source_url || source.seed_url || ''),
            provider: String(source.options?.ats_provider || source.site_type || ''),
            identifier: String(source.options?.ats_identifier || ''),
        });
    }

    function submitUserSourceEdit(sourceId: string) {
        const displayName = editSourceName.trim();
        const sourceUrl = editSourceUrl.trim();
        const provider = editSourceProvider.trim() || undefined;
        const identifier = editSourceIdentifier.trim() || undefined;
        const interval = Number(editSyncInterval);
        if (!displayName) {
            toast.error('Source name cannot be blank.');
            return;
        }
        if (sourceUrl && !isSupportedAtsUrl(sourceUrl)) {
            toast.error('Use a Greenhouse, Lever, or Ashby board URL.');
            return;
        }
        if (!Number.isInteger(interval) || interval < 5 || interval > 1440) {
            toast.error('Sync interval must be between 5 and 1440 minutes.');
            return;
        }
        const payload: AtsSourceUpdateRequest = {
            display_name: displayName,
            sync_interval_minutes: interval,
        };
        const boardChanged = !editOriginalSource
            || sourceUrl !== editOriginalSource.sourceUrl
            || (provider || '') !== editOriginalSource.provider
            || (identifier || '') !== editOriginalSource.identifier;
        if (boardChanged) {
            payload.source_url = sourceUrl || undefined;
            payload.provider = provider;
            payload.identifier = identifier;
            payload.providers = provider && !identifier ? [provider] : undefined;
        }
        updateUserSourceMutation.mutate({
            sourceId,
            payload,
        });
    }

    function deleteUserSource(source: FetchSource) {
        const sourceId = userSourceId(source);
        if (!sourceId) return;
        setSourcePendingDelete(source);
    }

    function confirmDeleteUserSource() {
        if (!sourcePendingDelete) return;
        const sourceId = userSourceId(sourcePendingDelete);
        if (!sourceId) return;
        deleteUserSourceMutation.mutate(sourceId);
    }

    const sourceLoadErrors = sourceLoadMessages({
        isSourcesError,
        sourcesError,
        isCloudError,
        cloudError,
        isUserSourcesError,
        userSourcesError,
        isHistoryError,
        historyError,
    });

    function retrySourceLoads() {
        void refetchSources();
        void refetchCloudIntegrations();
        void refetchUserSources();
        void refetchSourceHistory();
    }

    return (
        <section className="border-t border-rule pt-6">
            <SourcePanelHeader
                visibleCount={sources.length}
                totalCount={totalCount}
                isAddingSource={isAddingSource}
                showHistory={showHistory}
                sourceSearch={sourceSearch}
                sourceView={sourceView}
                sourceViewCounts={sourceViewCounts}
                catalogLabel={sourceCatalogLabel(data?.api_based_fetching, cloudSources.length)}
                onToggleAddingSource={() => setIsAddingSource((value) => !value)}
                onToggleHistory={() => setShowHistory((value) => !value)}
                onSourceSearchChange={setSourceSearch}
                onSourceViewChange={setSourceView}
            />

            {sourceLoadErrors.length > 0 ? (
                <SourceLoadErrorBanner messages={sourceLoadErrors} onRetry={retrySourceLoads} />
            ) : null}

            {isAddingSource ? (
                <AddSourceForm
                    newSourceName={newSourceName}
                    newSourceUrl={newSourceUrl}
                    newSourceProvider={newSourceProvider}
                    newSourceIdentifier={newSourceIdentifier}
                    discoveryError={discoveryError}
                    hasCheckedDiscovery={hasCheckedDiscovery}
                    discoveryCandidates={discoveryCandidates}
                    isChecking={discoverUserSourceMutation.isPending}
                    isAdding={createUserSourceMutation.isPending}
                    onSubmit={submitUserSource}
                    onCheck={checkUserSource}
                    onNameChange={updateNewSourceName}
                    onUrlChange={updateNewSourceUrl}
                    onProviderChange={updateNewSourceProvider}
                    onIdentifierChange={updateNewSourceIdentifier}
                    onAddDiscoveredSource={addDiscoveredSource}
                />
            ) : null}

            {showHistory ? (
                <SourceActivityPanel
                    sourceActivityFilter={sourceActivityFilter}
                    sourceActivityCounts={sourceActivityCounts}
                    sourceHistory={sourceHistory}
                    filteredSourceHistory={filteredSourceHistory}
                    isAdding={createUserSourceMutation.isPending}
                    onFilterChange={setSourceActivityFilter}
                    onHide={() => setShowHistory(false)}
                    onReaddSource={readdHistorySource}
                />
            ) : null}

            <SourcesContent
                isLoading={isLoading || isLoadingCloud || isLoadingUserSources}
                sources={sources}
                emptyMessage={emptyMessage}
                fetchingSourceSiteType={fetchSourceMutation.isPending ? fetchSourceMutation.variables ?? null : null}
                syncingSourceId={syncUserSourceMutation.isPending ? syncUserSourceMutation.variables ?? null : null}
                updatingSourceId={updateUserSourceMutation.isPending ? updateUserSourceMutation.variables?.sourceId ?? null : null}
                deletingSourceId={deleteUserSourceMutation.isPending ? deleteUserSourceMutation.variables ?? null : null}
                editingSourceId={editingSourceId}
                editSourceName={editSourceName}
                editSourceUrl={editSourceUrl}
                editSourceProvider={editSourceProvider}
                editSourceIdentifier={editSourceIdentifier}
                editSyncInterval={editSyncInterval}
                onFetchSource={(siteType) => fetchSourceMutation.mutate(siteType)}
                onSyncAtsSource={(sourceId) => syncUserSourceMutation.mutate(sourceId)}
                onEditAtsSource={editUserSource}
                onToggleAtsSource={(sourceId, status) => updateUserSourceMutation.mutate({ sourceId, payload: { status } })}
                onDeleteAtsSource={deleteUserSource}
                onSubmitEdit={submitUserSourceEdit}
                onCancelEdit={() => {
                    setEditingSourceId(null);
                    setEditOriginalSource(null);
                }}
                onEditSourceNameChange={setEditSourceName}
                onEditSourceUrlChange={setEditSourceUrl}
                onEditSourceProviderChange={setEditSourceProvider}
                onEditSourceIdentifierChange={setEditSourceIdentifier}
                onEditSyncIntervalChange={setEditSyncInterval}
            />

            {sourcePendingDelete ? (
                <SourceDeleteDialog
                    source={sourcePendingDelete}
                    isDeleting={deleteUserSourceMutation.isPending}
                    onCancel={() => setSourcePendingDelete(null)}
                    onConfirm={confirmDeleteUserSource}
                />
            ) : null}
        </section>
    );
}
