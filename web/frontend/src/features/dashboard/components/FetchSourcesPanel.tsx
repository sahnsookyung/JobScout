import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useMemo, useState } from 'react';
import type { FormEvent, ReactNode } from 'react';
import { ExternalLink, Globe2, MapPin, PauseCircle, Plus, RefreshCw, Search, Server, Trash2, Zap } from 'lucide-react';
import { toast } from 'sonner';

import { pipelineApi } from '@/services/pipelineApi';
import type { AtsSourceCreateRequest, CloudIntegration, FetchSource } from '@/types/api';

const OPERATIONAL_OPTION_KEYS = new Set([
    'status',
    'validation_status',
    'sync_interval_minutes',
    'last_error',
]);

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

function userSourceId(source: FetchSource): string | null {
    const sourceId = source.options?.user_source_id;
    return typeof sourceId === 'string' && sourceId ? sourceId : null;
}

function cloudIntegrationSource(integration: CloudIntegration): FetchSource {
    const providerLabel = toTitleCase(integration.provider);
    const status = integration.status || 'unknown';
    const isUserSource = integration.is_user_source === true;
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
            last_error: integration.last_error,
            user_source_id: isUserSource ? integration.id : undefined,
            is_user_source: isUserSource || undefined,
            owner_user_id: integration.owner_user_id || undefined,
            source_url: integration.source_url || undefined,
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

function SourceCard({
    source,
    index,
    onFetchSource,
    isFetchingSource,
    onSyncAtsSource,
    onToggleAtsSource,
    onDeleteAtsSource,
    isSyncingAtsSource,
    isUpdatingAtsSource,
    isDeletingAtsSource,
}: Readonly<{
    source: FetchSource;
    index: number;
    onFetchSource: (source: string) => void;
    isFetchingSource: boolean;
    onSyncAtsSource: (sourceId: string) => void;
    onToggleAtsSource: (sourceId: string, status: string) => void;
    onDeleteAtsSource: (source: FetchSource) => void;
    isSyncingAtsSource: boolean;
    isUpdatingAtsSource: boolean;
    isDeletingAtsSource: boolean;
}>) {
    const healthText = healthLabel(source);
    const externalText = externalSeedLabel(source);
    const statusText = atsStatus(source);
    const intervalMinutes = atsInterval(source);
    const canFetch = canFetchExternalSeed(source);
    const managedSourceId = userSourceId(source);
    const isDisabled = statusText === 'disabled';
    const isMutatingAtsSource = isSyncingAtsSource || isUpdatingAtsSource || isDeletingAtsSource;
    const content = (
        <>
            <div className="flex items-start justify-between gap-3">
                <div className="min-w-0">
                    <div className="flex min-w-0 flex-wrap items-center gap-2">
                        <div className="truncate text-[14px] font-medium text-ink">
                            {source.display_name}
                        </div>
                        <span className="inline-flex min-h-6 items-center border border-rule bg-surface-sunk px-2 py-0.5 text-[11px] leading-none text-ink-soft">
                            {modeLabel(source)}
                        </span>
                    </div>
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
                        className="mt-0.5 inline-flex h-7 w-7 flex-shrink-0 items-center justify-center border border-transparent text-ink-muted transition-colors hover:border-rule hover:text-accent"
                    >
                        <ExternalLink className="h-4 w-4" aria-hidden="true" />
                    </a>
                ) : (
                    <Globe2 className="mt-0.5 h-4 w-4 flex-shrink-0 text-ink-muted transition-colors group-hover:text-accent" aria-hidden="true" />
                )}
            </div>
            {source.description ? (
                <p className="mt-2 line-clamp-2 text-[12px] leading-5 text-ink-muted">
                    {source.description}
                </p>
            ) : null}
            <div className="mt-3 flex flex-wrap items-center gap-1.5">
                <span className={metaChipClasses()}>
                    <MapPin className="h-3 w-3" aria-hidden="true" />
                    {sourceScope(source)}
                </span>
                <span className={metaChipClasses('tabular-nums')}>{source.results_wanted} jobs</span>
                {optionCount(source) > 0 ? (
                    <span className={metaChipClasses('tabular-nums')}>{optionCount(source)} filters</span>
                ) : null}
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
                {statusText ? (
                    <span className={metaChipClasses('capitalize tabular-nums')}>
                        {statusText}
                    </span>
                ) : null}
                {intervalMinutes ? (
                    <span className={metaChipClasses('tabular-nums')}>
                        {intervalMinutes}m sync
                    </span>
                ) : null}
            </div>
            {source.tags.length > 0 ? (
                <div className="mt-3 flex flex-wrap gap-1">
                    {source.tags.slice(0, 4).map((tag) => (
                        <span
                            key={tag}
                            className="border border-rule bg-surface-sunk px-1.5 py-0.5 text-[11px] text-ink-soft"
                        >
                            {tag}
                        </span>
                    ))}
                </div>
            ) : null}
            {canFetch ? (
                <div className="mt-3 flex justify-end">
                    <button
                        type="button"
                        onClick={() => onFetchSource(source.site_type)}
                        disabled={isFetchingSource}
                        className="inline-flex min-h-8 items-center gap-1.5 border border-accent px-2.5 py-1 text-[12px] font-medium text-accent transition-colors hover:bg-accent-soft disabled:cursor-not-allowed disabled:border-rule disabled:text-ink-soft"
                    >
                        <RefreshCw
                            className={`h-3.5 w-3.5 ${isFetchingSource ? 'animate-spin' : ''}`}
                            aria-hidden="true"
                        />
                        Fetch
                    </button>
                </div>
            ) : null}
            {managedSourceId ? (
                <div className="mt-3 flex flex-wrap justify-end gap-2 border-t border-rule pt-3">
                    <button
                        type="button"
                        onClick={() => onSyncAtsSource(managedSourceId)}
                        disabled={isDisabled || isSyncingAtsSource}
                        className="inline-flex min-h-8 items-center gap-1.5 border border-accent px-2.5 py-1 text-[12px] font-medium text-accent transition-colors hover:bg-accent-soft disabled:cursor-not-allowed disabled:border-rule disabled:text-ink-soft"
                    >
                        <RefreshCw
                            className={`h-3.5 w-3.5 ${isSyncingAtsSource ? 'animate-spin' : ''}`}
                            aria-hidden="true"
                        />
                        Sync
                    </button>
                    <button
                        type="button"
                        onClick={() => onToggleAtsSource(managedSourceId, isDisabled ? 'active' : 'disabled')}
                        disabled={isMutatingAtsSource}
                        className="inline-flex min-h-8 items-center gap-1.5 border border-rule px-2.5 py-1 text-[12px] font-medium text-ink-soft transition-colors hover:border-accent hover:text-accent disabled:cursor-not-allowed disabled:text-ink-soft"
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
                        onClick={() => onDeleteAtsSource(source)}
                        disabled={isMutatingAtsSource}
                        className="inline-flex min-h-8 items-center gap-1.5 border border-warn/50 px-2.5 py-1 text-[12px] font-medium text-warn transition-colors hover:bg-warn-soft disabled:cursor-not-allowed disabled:border-rule disabled:text-ink-soft"
                    >
                        <Trash2 className="h-3.5 w-3.5" aria-hidden="true" />
                        Delete
                    </button>
                </div>
            ) : null}
        </>
    );
    const className = 'group min-h-36 border border-rule bg-surface px-4 py-3 transition-colors hover:border-rule-strong';

    return (
        <div key={`${source.site_type}-${index}`} className={className}>
            {content}
        </div>
    );
}

export function FetchSourcesPanel() {
    const [sourceSearch, setSourceSearch] = useState('');
    const [isAddingSource, setIsAddingSource] = useState(false);
    const [newSourceName, setNewSourceName] = useState('');
    const [newSourceUrl, setNewSourceUrl] = useState('');
    const [newSourceProvider, setNewSourceProvider] = useState('');
    const [newSourceIdentifier, setNewSourceIdentifier] = useState('');
    const queryClient = useQueryClient();
    const { data, isLoading } = useQuery({
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
    const { data: cloudIntegrations = [], isLoading: isLoadingCloud } = useQuery({
        queryKey: ['cloud', 'integrations', 'source-panel'],
        queryFn: async () => {
            const response = await pipelineApi.getCloudIntegrations();
            return response.status === 200 && Array.isArray(response.data) ? response.data : [];
        },
        staleTime: 5 * 60 * 1000,
    });
    const { data: userAtsSources = [], isLoading: isLoadingUserSources } = useQuery({
        queryKey: ['cloud', 'integrations', 'user-sources'],
        queryFn: async () => {
            const response = await pipelineApi.getUserAtsSources();
            return response.status === 200 && Array.isArray(response.data) ? response.data : [];
        },
        staleTime: 60 * 1000,
    });
    const createUserSourceMutation = useMutation({
        mutationFn: async (payload: AtsSourceCreateRequest) => {
            const response = await pipelineApi.createUserAtsSource(payload);
            return response.data;
        },
        onSuccess: (source) => {
            toast.success(`${source.display_name} added`);
            setIsAddingSource(false);
            setNewSourceName('');
            setNewSourceUrl('');
            setNewSourceProvider('');
            setNewSourceIdentifier('');
            void queryClient.invalidateQueries({ queryKey: ['cloud', 'integrations', 'source-panel'] });
            void queryClient.invalidateQueries({ queryKey: ['cloud', 'integrations', 'user-sources'] });
        },
        onError: (error) => {
            toast.error(`ATS source add failed: ${apiErrorMessage(error)}`);
        },
    });
    const updateUserSourceMutation = useMutation({
        mutationFn: async ({ sourceId, status }: { sourceId: string; status: string }) => {
            const response = await pipelineApi.updateUserAtsSource(sourceId, { status });
            return response.data;
        },
        onSuccess: (source) => {
            toast.success(`${source.display_name} ${source.status === 'disabled' ? 'disabled' : 'enabled'}`);
            void queryClient.invalidateQueries({ queryKey: ['cloud', 'integrations', 'source-panel'] });
            void queryClient.invalidateQueries({ queryKey: ['cloud', 'integrations', 'user-sources'] });
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
            void queryClient.invalidateQueries({ queryKey: ['cloud', 'integrations', 'source-panel'] });
            void queryClient.invalidateQueries({ queryKey: ['cloud', 'integrations', 'user-sources'] });
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
    const sources = useMemo(
        () => allSources.filter((source) => sourceMatchesSearch(source, sourceSearch)),
        [allSources, sourceSearch]
    );
    const totalCount = (data?.total_count ?? (data?.sources ?? []).length) + cloudSources.length;
    const emptyMessage = sourceSearch.trim()
        ? 'No sources match that search.'
        : 'No fetch sources configured.';

    function submitUserSource(event: FormEvent<HTMLFormElement>) {
        event.preventDefault();
        const provider = newSourceProvider.trim() || undefined;
        const identifier = newSourceIdentifier.trim() || undefined;
        const payload: AtsSourceCreateRequest = {
            display_name: newSourceName.trim() || undefined,
            source_url: newSourceUrl.trim() || undefined,
            provider,
            identifier,
            providers: provider && !identifier ? [provider] : undefined,
        };
        if (!payload.display_name && !payload.source_url && !(payload.provider && payload.identifier)) {
            toast.error('Add a source name, careers URL, or provider board identifier.');
            return;
        }
        createUserSourceMutation.mutate(payload);
    }

    function deleteUserSource(source: FetchSource) {
        const sourceId = userSourceId(source);
        if (!sourceId) return;
        if (!window.confirm(`Delete ${source.display_name}?`)) return;
        deleteUserSourceMutation.mutate(sourceId);
    }

    let sourcesContent: ReactNode;
    if (isLoading || isLoadingCloud || isLoadingUserSources) {
        sourcesContent = (
            <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
                {[0, 1, 2].map((item) => (
                    <div key={item} className="h-24 animate-pulse border border-rule bg-surface-sunk" />
                ))}
            </div>
        );
    } else if (sources.length === 0) {
        sourcesContent = (
            <div className="border border-dashed border-rule bg-surface px-4 py-5 text-[13px] text-ink-muted">
                {emptyMessage}
            </div>
        );
    } else {
        sourcesContent = (
            <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
                {sources.map((source, index) => (
                    <SourceCard
                        key={`${source.site_type}-${index}`}
                        source={source}
                        index={index}
                        onFetchSource={(siteType) => fetchSourceMutation.mutate(siteType)}
                        isFetchingSource={
                            fetchSourceMutation.isPending
                            && fetchSourceMutation.variables === source.site_type
                        }
                        onSyncAtsSource={(sourceId) => syncUserSourceMutation.mutate(sourceId)}
                        onToggleAtsSource={(sourceId, status) => updateUserSourceMutation.mutate({ sourceId, status })}
                        onDeleteAtsSource={deleteUserSource}
                        isSyncingAtsSource={
                            syncUserSourceMutation.isPending
                            && syncUserSourceMutation.variables === userSourceId(source)
                        }
                        isUpdatingAtsSource={
                            updateUserSourceMutation.isPending
                            && updateUserSourceMutation.variables?.sourceId === userSourceId(source)
                        }
                        isDeletingAtsSource={
                            deleteUserSourceMutation.isPending
                            && deleteUserSourceMutation.variables === userSourceId(source)
                        }
                    />
                ))}
            </div>
        );
    }

    return (
        <section className="border-t border-rule pt-6">
            <div className="mb-4 flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
                <div>
                    <p className="caption">Sources</p>
                    <h3 className="mt-1 text-[15px] font-medium text-ink">
                        Fetch queue
                        {data ? (
                            <span className="ml-2 text-[12px] font-normal text-ink-soft">
                                {sources.length}/{totalCount}
                            </span>
                        ) : null}
                    </h3>
                </div>
                <div className="flex flex-col gap-2 sm:flex-row sm:items-center">
                    <button
                        type="button"
                        onClick={() => setIsAddingSource((value) => !value)}
                        className="inline-flex h-9 items-center justify-center gap-1.5 border border-accent px-3 text-[13px] font-medium text-accent transition-colors hover:bg-accent-soft"
                    >
                        <Plus className="h-3.5 w-3.5" aria-hidden="true" />
                        Add source
                    </button>
                    <label className="relative block">
                        <Search className="pointer-events-none absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-ink-soft" aria-hidden="true" />
                        <input
                            aria-label="Search sources"
                            value={sourceSearch}
                            onChange={(event) => setSourceSearch(event.target.value)}
                            placeholder="Search sources"
                            className="h-9 w-full border border-rule bg-surface pl-8 pr-3 text-[13px] text-ink outline-none transition-colors placeholder:text-ink-soft focus:border-accent sm:w-48"
                        />
                    </label>
                    <div className="inline-flex items-center gap-2 self-start border border-rule bg-surface px-2.5 py-1.5 text-[12px] text-ink-soft">
                        <Server className="h-3.5 w-3.5 text-accent" aria-hidden="true" />
                        <span>{sourceCatalogLabel(data?.api_based_fetching, cloudSources.length)}</span>
                    </div>
                </div>
            </div>

            {isAddingSource ? (
                <form
                    onSubmit={submitUserSource}
                    className="mb-4 grid gap-3 border border-rule bg-surface px-4 py-3 md:grid-cols-[minmax(0,1fr)_minmax(0,1.4fr)_9rem_minmax(0,1fr)_auto]"
                >
                    <label className="grid gap-1 text-[12px] text-ink-soft">
                        Name
                        <input
                            value={newSourceName}
                            onChange={(event) => setNewSourceName(event.target.value)}
                            placeholder="Company or board"
                            className="h-9 border border-rule bg-surface-raised px-3 text-[13px] text-ink outline-none placeholder:text-ink-soft focus:border-accent"
                        />
                    </label>
                    <label className="grid gap-1 text-[12px] text-ink-soft">
                        Careers URL
                        <input
                            value={newSourceUrl}
                            onChange={(event) => setNewSourceUrl(event.target.value)}
                            placeholder="https://boards.greenhouse.io/acme"
                            className="h-9 border border-rule bg-surface-raised px-3 text-[13px] text-ink outline-none placeholder:text-ink-soft focus:border-accent"
                        />
                    </label>
                    <label className="grid gap-1 text-[12px] text-ink-soft">
                        Provider
                        <select
                            value={newSourceProvider}
                            onChange={(event) => setNewSourceProvider(event.target.value)}
                            className="h-9 border border-rule bg-surface-raised px-3 text-[13px] text-ink outline-none focus:border-accent"
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
                            value={newSourceIdentifier}
                            onChange={(event) => setNewSourceIdentifier(event.target.value)}
                            placeholder="acme"
                            className="h-9 border border-rule bg-surface-raised px-3 text-[13px] text-ink outline-none placeholder:text-ink-soft focus:border-accent"
                        />
                    </label>
                    <div className="flex items-end gap-2">
                        <button
                            type="submit"
                            disabled={createUserSourceMutation.isPending}
                            className="inline-flex h-9 items-center justify-center border border-accent px-3 text-[13px] font-medium text-accent transition-colors hover:bg-accent-soft disabled:cursor-not-allowed disabled:border-rule disabled:text-ink-soft"
                        >
                            {createUserSourceMutation.isPending ? 'Adding' : 'Add'}
                        </button>
                    </div>
                </form>
            ) : null}

            {sourcesContent}
        </section>
    );
}
