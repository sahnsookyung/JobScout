import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { cloudOperationsApi } from '@/services/cloudOperationsApi';
import { OperationsStatusPanel } from '../OperationsStatusPanel';

vi.mock('@/services/cloudOperationsApi', () => ({
    cloudOperationsApi: {
        getStatus: vi.fn(),
    },
}));

const mockGetStatus = vi.mocked(cloudOperationsApi.getStatus);

function renderPanel() {
    const queryClient = new QueryClient({
        defaultOptions: {
            queries: {
                retry: false,
            },
        },
    });

    return render(
        <QueryClientProvider client={queryClient}>
            <OperationsStatusPanel />
        </QueryClientProvider>
    );
}

describe('OperationsStatusPanel', () => {
    beforeEach(() => {
        mockGetStatus.mockReset();
    });

    it('renders quota and notification diagnostics', async () => {
        mockGetStatus.mockResolvedValue({
            data: {
                generated_at: '2026-05-23T06:00:00Z',
                tenant_id: 'tenant-123',
                quotas: { backend: 'redis' },
                notifications: { dry_run: true },
                warnings: [],
            },
        } as never);

        renderPanel();

        expect(screen.getByText('Loading diagnostics...')).toBeInTheDocument();
        expect(await screen.findByText('tenant-123')).toBeInTheDocument();
        expect(screen.getByText('redis')).toBeInTheDocument();
        expect(screen.getByText('Yes')).toBeInTheDocument();
        expect(screen.getByText('No tenant-visible warnings.')).toBeInTheDocument();
    });

    it('renders warnings and non-scalar diagnostic values safely', async () => {
        mockGetStatus.mockResolvedValue({
            data: {
                generated_at: '2026-05-23T06:00:00Z',
                tenant_id: 'tenant-456',
                quotas: { backend: { mode: 'redis' } },
                notifications: { dry_run: false },
                warnings: [
                    {
                        code: 'redis_eviction_policy',
                        message: 'Redis eviction policy should be noeviction.',
                    },
                ],
            },
        } as never);

        renderPanel();

        expect(await screen.findByText('tenant-456')).toBeInTheDocument();
        expect(screen.getByText('{"mode":"redis"}')).toBeInTheDocument();
        expect(screen.getByText('No')).toBeInTheDocument();
        expect(screen.getByText('redis_eviction_policy')).toBeInTheDocument();
        expect(screen.getByText('Redis eviction policy should be noeviction.')).toBeInTheDocument();
    });
});
