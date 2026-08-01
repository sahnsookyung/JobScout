import { fireEvent, render, screen } from '@testing-library/react';
import type { ReactNode } from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import App from '../App';

vi.mock('@/components/AppErrorBoundary', () => ({
    AppErrorBoundary: ({ children }: { children: ReactNode }) => <>{children}</>,
}));

vi.mock('@/components/ui/Toast', () => ({
    ToastProvider: () => <div data-testid="toast-provider" />,
}));

vi.mock('@/features/auth/AuthGate', () => ({
    AuthGate: ({ children }: { children: ReactNode }) => <>{children}</>,
}));

vi.mock('@/features/auth/TurnstileGate', () => ({
    TurnstileGate: () => <div data-testid="turnstile-gate" />,
}));

vi.mock('@/features/auth/useAuth', () => ({
    useAuth: () => ({ user: { is_platform_admin: true } }),
}));

vi.mock('@/features/config/components/PolicyPanel', () => ({
    PolicyPanel: () => <div>Policy panel</div>,
}));

vi.mock('@/features/dashboard/components/DashboardControls', () => ({
    DashboardControls: () => <div>Dashboard controls</div>,
}));

vi.mock('@/features/dashboard/components/DashboardHeader', () => ({
    DashboardHeader: () => <header>Dashboard header</header>,
}));

vi.mock('@/features/dashboard/components/JobManagementPanel', () => ({
    JobManagementPanel: () => <div>Deferred job management</div>,
}));

vi.mock('@/features/matches/components/MatchList', () => ({
    MatchList: ({ onMatchSelect }: { onMatchSelect: (matchId: string) => void }) => (
        <button type="button" onClick={() => onMatchSelect('match-1')}>
            Open match
        </button>
    ),
}));

vi.mock('@/features/matches/components/MatchDetailsModal', () => ({
    MatchDetailsModal: ({
        matchId,
        onClose,
    }: {
        matchId: string | null;
        onClose: () => void;
    }) => (
        <div>
            <span>Deferred match details {matchId}</span>
            <button type="button" onClick={onClose}>Close details</button>
        </div>
    ),
}));

vi.mock('@/features/notifications/components/EmailVerificationPage', () => ({
    EmailVerificationPage: () => <div>Deferred email verification</div>,
}));

describe('App lazy boundaries', () => {
    beforeEach(() => {
        globalThis.history.replaceState({}, '', '/');
    });

    it('loads match details and admin management only when requested', async () => {
        render(<App />);

        expect(screen.getByRole('tabpanel', { name: 'Jobs' })).toBeInTheDocument();
        expect(screen.queryByText('Deferred match details match-1')).not.toBeInTheDocument();
        expect(screen.queryByText('Deferred job management')).not.toBeInTheDocument();

        fireEvent.click(screen.getByRole('button', { name: 'Open match' }));
        expect(await screen.findByText('Deferred match details match-1')).toBeInTheDocument();
        fireEvent.click(screen.getByRole('button', { name: 'Close details' }));
        expect(screen.queryByText('Deferred match details match-1')).not.toBeInTheDocument();

        fireEvent.click(screen.getByRole('tab', { name: 'Job Management' }));
        expect(await screen.findByText('Deferred job management')).toBeInTheDocument();
        expect(screen.getByRole('tabpanel', { name: 'Job Management' })).toBeInTheDocument();
    });

    it('loads the email verification route without mounting the dashboard', async () => {
        globalThis.history.replaceState({}, '', '/verify-email');

        render(<App />);

        expect(await screen.findByText('Deferred email verification')).toBeInTheDocument();
        expect(screen.queryByText('Dashboard header')).not.toBeInTheDocument();
    });
});
