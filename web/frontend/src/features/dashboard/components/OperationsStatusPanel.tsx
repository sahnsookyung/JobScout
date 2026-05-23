import { useQuery } from '@tanstack/react-query';

import { cloudOperationsApi } from '@/services/cloudOperationsApi';

function valueAsText(value: unknown): string {
    if (value === null || value === undefined) return 'Unknown';
    if (typeof value === 'boolean') return value ? 'Yes' : 'No';
    if (typeof value === 'string' || typeof value === 'number') return String(value);
    return JSON.stringify(value);
}

export function OperationsStatusPanel() {
    const statusQuery = useQuery({
        queryKey: ['cloud-operations-status'],
        queryFn: async () => (await cloudOperationsApi.getStatus()).data,
        retry: 1,
    });

    if (statusQuery.isLoading) {
        return <p className="text-[13px] text-ink-soft">Loading diagnostics...</p>;
    }

    if (statusQuery.isError || !statusQuery.data) {
        return (
            <p className="border border-warn/40 bg-warn-soft px-3 py-2 text-[13px] text-ink" role="alert">
                Diagnostics are unavailable for this account.
            </p>
        );
    }

    const status = statusQuery.data;
    const quotaBackend = status.quotas && typeof status.quotas === 'object'
        ? status.quotas['backend']
        : undefined;
    const notificationDryRun = status.notifications && typeof status.notifications === 'object'
        ? status.notifications['dry_run']
        : undefined;
    const warnings = status.warnings ?? [];

    return (
        <div className="space-y-4">
            <div className="grid gap-3 sm:grid-cols-2">
                <div className="border border-rule bg-surface px-3 py-3">
                    <p className="caption">Tenant</p>
                    <p className="mt-1 truncate text-[13px] text-ink">{status.tenant_id}</p>
                </div>
                <div className="border border-rule bg-surface px-3 py-3">
                    <p className="caption">Quota Backend</p>
                    <p className="mt-1 text-[13px] text-ink">{valueAsText(quotaBackend)}</p>
                </div>
                <div className="border border-rule bg-surface px-3 py-3">
                    <p className="caption">Notifications Dry Run</p>
                    <p className="mt-1 text-[13px] text-ink">{valueAsText(notificationDryRun)}</p>
                </div>
                <div className="border border-rule bg-surface px-3 py-3">
                    <p className="caption">Generated</p>
                    <p className="mt-1 text-[13px] text-ink">{new Date(status.generated_at).toLocaleString()}</p>
                </div>
            </div>

            {warnings.length > 0 ? (
                <div className="space-y-2">
                    {warnings.map((warning) => (
                        <div key={warning.code} className="border border-warn/40 bg-warn-soft px-3 py-2">
                            <p className="text-[13px] font-medium text-ink">{warning.code}</p>
                            <p className="mt-1 text-[13px] text-ink-soft">{warning.message}</p>
                        </div>
                    ))}
                </div>
            ) : (
                <p className="border border-rule bg-surface px-3 py-2 text-[13px] text-ink-soft">
                    No tenant-visible warnings.
                </p>
            )}
        </div>
    );
}
