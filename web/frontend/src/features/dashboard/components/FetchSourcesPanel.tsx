import { useQuery } from '@tanstack/react-query';
import { useMemo, useState } from 'react';
import type { ReactNode } from 'react';
import { Globe2, MapPin, Search, Server } from 'lucide-react';

import { pipelineApi } from '@/services/pipelineApi';
import type { FetchSource } from '@/types/api';

function sourceScope(source: FetchSource): string {
    const parts = [source.location, source.country].filter(Boolean);
    return parts.length > 0 ? parts.join(', ') : 'Global';
}

function sourceQuery(source: FetchSource): string {
    return source.search_term?.trim() || 'Seed feed';
}

function optionCount(source: FetchSource): number {
    return Object.values(source.options || {}).reduce((count, value) => {
        if (Array.isArray(value)) return count + value.length;
        return value === undefined || value === null || value === '' ? count : count + 1;
    }, 0);
}

function healthLabel(source: FetchSource): string {
    if (!source.api_health) return 'API status off';
    if (source.api_health.available) return 'API online';
    if (source.api_health.status === 'not_configured') return 'API not configured';
    if (source.api_health.status === 'timeout') return 'API timeout';
    return 'API offline';
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

function SourceCard({
    source,
    index,
}: Readonly<{
    source: FetchSource;
    index: number;
}>) {
    const content = (
        <>
            <div className="flex items-start justify-between gap-3">
                <div className="min-w-0">
                    <div className="truncate text-[14px] font-medium text-ink">
                        {source.display_name}
                    </div>
                    <div className="mt-1 flex items-center gap-1.5 text-[12px] text-ink-muted">
                        <Search className="h-3 w-3" aria-hidden="true" />
                        <span className="truncate">{sourceQuery(source)}</span>
                    </div>
                </div>
                <Globe2 className="mt-0.5 h-4 w-4 flex-shrink-0 text-ink-muted transition-colors group-hover:text-accent" aria-hidden="true" />
            </div>
            {source.description ? (
                <p className="mt-2 line-clamp-2 text-[12px] leading-5 text-ink-muted">
                    {source.description}
                </p>
            ) : null}
            <div className="mt-3 flex flex-wrap items-center gap-x-3 gap-y-1 text-[12px] text-ink-soft">
                <span className="inline-flex items-center gap-1">
                    <MapPin className="h-3 w-3" aria-hidden="true" />
                    {sourceScope(source)}
                </span>
                <span className="num">{source.results_wanted} jobs</span>
                {optionCount(source) > 0 ? (
                    <span className="num">{optionCount(source)} filters</span>
                ) : null}
                <span className="num">{healthLabel(source)}</span>
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
        </>
    );
    const className = 'group min-h-32 border border-rule bg-surface px-4 py-3 transition-colors hover:border-rule-strong';

    if (!source.seed_url) {
        return (
            <div key={`${source.site_type}-${index}`} className={className}>
                {content}
            </div>
        );
    }

    return (
        <a
            key={`${source.site_type}-${index}`}
            href={source.seed_url}
            className={className}
            target="_blank"
            rel="noreferrer"
        >
            {content}
        </a>
    );
}

export function FetchSourcesPanel() {
    const [sourceSearch, setSourceSearch] = useState('');
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

    const allSources = data?.sources ?? [];
    const sources = useMemo(
        () => allSources.filter((source) => sourceMatchesSearch(source, sourceSearch)),
        [allSources, sourceSearch]
    );
    const totalCount = data?.total_count ?? allSources.length;
    const emptyMessage = sourceSearch.trim()
        ? 'No sources match that search.'
        : 'No fetch sources configured.';
    let sourcesContent: ReactNode;
    if (isLoading) {
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
                    <SourceCard key={`${source.site_type}-${index}`} source={source} index={index} />
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
                        <span>{data?.api_based_fetching ? 'JobSpy API' : 'Local config'}</span>
                    </div>
                </div>
            </div>

            {sourcesContent}
        </section>
    );
}
