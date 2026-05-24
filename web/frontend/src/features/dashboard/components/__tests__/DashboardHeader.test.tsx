import { fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { vi } from 'vitest';

import { DashboardHeader } from '../DashboardHeader';
import { useAuth } from '@/features/auth/useAuth';

vi.mock('@/features/auth/useAuth', () => ({
    useAuth: vi.fn(() => ({
        user: {
            name: 'Ada Lovelace',
            email: 'ada@example.com',
            picture: 'https://example.com/ada.png',
        },
        logout: vi.fn(),
    })),
}));

vi.mock('@/features/notifications/components/NotificationSettingsPanel', () => ({
    NotificationSettingsPanel: () => <div>Notification settings content</div>,
}));

vi.mock('@/features/preferences/components/CandidatePreferencesPanel', () => ({
    CandidatePreferencesPanel: () => <div>Candidate preferences content</div>,
}));

vi.mock('../OperationsStatusPanel', () => ({
    OperationsStatusPanel: () => <div>Operations status content</div>,
}));

const mockUseAuth = vi.mocked(useAuth);

function makeAuthState(overrides: Partial<ReturnType<typeof useAuth>> = {}) {
    return {
        user: {
            name: 'Ada Lovelace',
            email: 'ada@example.com',
            picture: 'https://example.com/ada.png',
        },
        token: 'token-123',
        isReady: true,
        restoreError: null,
        login: vi.fn(),
        logout: vi.fn(),
        retrySession: vi.fn(),
        ...overrides,
    };
}

describe('DashboardHeader', () => {
    beforeEach(() => {
        mockUseAuth.mockReturnValue(makeAuthState());
    });

    it('opens notification settings from the top bar', async () => {
        render(<DashboardHeader />);

        await userEvent.click(screen.getByRole('button', { name: /open notification settings/i }));

        expect(screen.getByRole('dialog', { name: /notification preferences/i })).toBeInTheDocument();
        expect(screen.getByText('Notification settings content')).toBeInTheDocument();
    });

    it('closes notification settings from the backdrop control', async () => {
        render(<DashboardHeader />);

        await userEvent.click(screen.getByRole('button', { name: /open notification settings/i }));
        await userEvent.click(screen.getAllByRole('button', { name: /close notification settings/i })[0]);

        expect(
            screen.queryByRole('dialog', { name: /notification preferences/i }),
        ).not.toBeInTheDocument();
    });

    it('shows the profile panel from the top bar', async () => {
        render(<DashboardHeader />);

        await userEvent.click(screen.getByRole('button', { name: /open profile menu/i }));

        const panel = screen.getByLabelText('Profile panel');

        expect(panel).toBeInTheDocument();
        expect(within(panel).getByText('Ada Lovelace')).toBeInTheDocument();
        expect(within(panel).getByText('ada@example.com')).toBeInTheDocument();
        expect(within(panel).getByRole('button', { name: /sign out/i })).toBeInTheDocument();
    });

    it('returns focus to the notification trigger after closing the modal', async () => {
        render(<DashboardHeader />);

        const notificationButton = screen.getByRole('button', { name: /open notification settings/i });
        await userEvent.click(notificationButton);
        await userEvent.click(screen.getAllByRole('button', { name: /close notification settings/i })[0]);

        expect(notificationButton).toHaveFocus();
    });

    it('renders the notification button before the profile button', () => {
        render(<DashboardHeader />);

        const notificationButton = screen.getByRole('button', { name: /open notification settings/i });
        const profileButton = screen.getByRole('button', { name: /open profile menu/i });

        expect(notificationButton.compareDocumentPosition(profileButton) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    });

    it('opens candidate preferences from the top bar and closes on escape', async () => {
        render(<DashboardHeader />);

        await userEvent.click(screen.getByRole('button', { name: /preferences/i }));
        expect(screen.getByRole('dialog', { name: /candidate preferences/i })).toBeInTheDocument();
        expect(screen.getByText('Candidate preferences content')).toBeInTheDocument();

        fireEvent.keyDown(document, { key: 'Escape' });

        await waitFor(() => {
            expect(
                screen.queryByRole('dialog', { name: /candidate preferences/i }),
            ).not.toBeInTheDocument();
        });
    });

    it('opens tenant diagnostics from the profile panel', async () => {
        render(<DashboardHeader />);

        await userEvent.click(screen.getByRole('button', { name: /open profile menu/i }));
        await userEvent.click(screen.getByRole('button', { name: /diagnostics/i }));

        expect(screen.getByRole('dialog', { name: /tenant diagnostics/i })).toBeInTheDocument();
        expect(screen.getByText('Operations status content')).toBeInTheDocument();
        expect(screen.queryByLabelText('Profile panel')).not.toBeInTheDocument();

        await userEvent.click(screen.getAllByRole('button', { name: /close diagnostics/i })[0]);
        expect(screen.queryByRole('dialog', { name: /tenant diagnostics/i })).not.toBeInTheDocument();
    });

    it('signs out from the profile panel', async () => {
        const logout = vi.fn();
        mockUseAuth.mockReturnValue(makeAuthState({ logout }));

        render(<DashboardHeader />);

        await userEvent.click(screen.getByRole('button', { name: /open profile menu/i }));
        await userEvent.click(screen.getByRole('button', { name: /sign out/i }));

        expect(logout).toHaveBeenCalledTimes(1);
    });

    it('closes the profile panel on outside click', async () => {
        render(<DashboardHeader />);

        await userEvent.click(screen.getByRole('button', { name: /open profile menu/i }));
        expect(screen.getByLabelText('Profile panel')).toBeInTheDocument();

        fireEvent.mouseDown(document.body);

        await waitFor(() => {
            expect(screen.queryByLabelText('Profile panel')).not.toBeInTheDocument();
        });
    });

    it('closes the profile panel on escape', async () => {
        render(<DashboardHeader />);

        await userEvent.click(screen.getByRole('button', { name: /open profile menu/i }));
        fireEvent.keyDown(document, { key: 'Escape' });

        await waitFor(() => {
            expect(screen.queryByLabelText('Profile panel')).not.toBeInTheDocument();
        });
    });

    it('renders the local session fallback when no user is signed in', async () => {
        mockUseAuth.mockReturnValue(makeAuthState({ user: null }));

        render(<DashboardHeader />);
        await userEvent.click(screen.getByRole('button', { name: /open profile menu/i }));

        const panel = screen.getByLabelText('Profile panel');
        expect(within(panel).getByText('Workshop')).toBeInTheDocument();
        expect(within(panel).getByText('Local session')).toBeInTheDocument();
        expect(within(panel).getByText(/google sign-in is off in this session/i)).toBeInTheDocument();
    });
});
