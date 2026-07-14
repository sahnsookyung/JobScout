import { QueryClient, hashKey, type QueryKey } from '@tanstack/react-query';

let privateQueryScope = 'anonymous';

export function setPrivateQueryScope(ownerId: string | null, tenantId: string | null): void {
    privateQueryScope = `${ownerId ?? 'anonymous'}:${tenantId ?? 'none'}`;
}

export const queryClient = new QueryClient({
    defaultOptions: {
        queries: {
            refetchOnWindowFocus: false,
            retry: 1,
            queryKeyHashFn: (queryKey: QueryKey) => hashKey([privateQueryScope, queryKey]),
        },
    },
});

export function clearPrivateQueryCache(): void {
    queryClient.clear();
}
