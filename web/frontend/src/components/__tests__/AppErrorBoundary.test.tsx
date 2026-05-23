import { render, screen } from '@testing-library/react';
import type { ReactElement } from 'react';
import { vi } from 'vitest';

import { AppErrorBoundary } from '../AppErrorBoundary';

function BrokenChild(): ReactElement {
    throw new Error('broken render');
}

describe('AppErrorBoundary', () => {
    it('renders children while the app is healthy', () => {
        render(
            <AppErrorBoundary>
                <span>Dashboard ready</span>
            </AppErrorBoundary>
        );

        expect(screen.getByText('Dashboard ready')).toBeInTheDocument();
    });

    it('logs development errors and renders the recovery panel', () => {
        const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

        render(
            <AppErrorBoundary>
                <BrokenChild />
            </AppErrorBoundary>
        );

        expect(screen.getByText('JobScout needs a refresh')).toBeInTheDocument();
        expect(screen.getByRole('button', { name: 'Refresh' })).toBeInTheDocument();
        expect(consoleSpy).toHaveBeenCalledWith(
            '[AppErrorBoundary]',
            expect.any(Error),
            expect.any(String)
        );

        consoleSpy.mockRestore();
    });
});
