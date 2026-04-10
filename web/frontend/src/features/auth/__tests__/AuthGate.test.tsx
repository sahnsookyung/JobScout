import { fireEvent, render, screen } from '@testing-library/react';
import { AuthGate } from '../AuthGate';

vi.mock('../useAuth', () => ({
    useAuth: vi.fn(() => ({
        user: null,
        token: null,
        isReady: true,
        restoreError: null,
        login: vi.fn(),
        logout: vi.fn(),
        retrySession: vi.fn(),
    })),
}));

vi.mock('../GoogleLoginScreen', () => ({
    GoogleLoginScreen: () => <div data-testid="google-login-screen" />,
}));

import { useAuth } from '../useAuth';

describe('AuthGate', () => {
    afterEach(() => {
        vi.unstubAllEnvs();
    });

    describe('OSS mode — VITE_GOOGLE_CLIENT_ID not set', () => {
        beforeEach(() => {
            vi.stubEnv('VITE_GOOGLE_CLIENT_ID', '');
        });

        it('renders children directly without requiring login', () => {
            render(
                <AuthGate>
                    <div data-testid="child-content">Dashboard</div>
                </AuthGate>
            );
            expect(screen.getByTestId('child-content')).toBeInTheDocument();
        });

        it('does not render GoogleLoginScreen', () => {
            render(
                <AuthGate>
                    <div>Content</div>
                </AuthGate>
            );
            expect(screen.queryByTestId('google-login-screen')).not.toBeInTheDocument();
        });

        it('renders multiple children', () => {
            render(
                <AuthGate>
                    <span data-testid="a">A</span>
                    <span data-testid="b">B</span>
                </AuthGate>
            );
            expect(screen.getByTestId('a')).toBeInTheDocument();
            expect(screen.getByTestId('b')).toBeInTheDocument();
        });
    });

    describe('SaaS mode — VITE_GOOGLE_CLIENT_ID set', () => {
        beforeEach(() => {
            vi.stubEnv('VITE_GOOGLE_CLIENT_ID', 'test-google-client-id-123');
        });

        it('shows GoogleLoginScreen when user is null', () => {
            vi.mocked(useAuth).mockReturnValue({
                user: null,
                token: null,
                isReady: true,
                restoreError: null,
                login: vi.fn(),
                logout: vi.fn(),
                retrySession: vi.fn(),
            });
            render(
                <AuthGate>
                    <div data-testid="child-content">App</div>
                </AuthGate>
            );
            expect(screen.getByTestId('google-login-screen')).toBeInTheDocument();
            expect(screen.queryByTestId('child-content')).not.toBeInTheDocument();
        });

        it('renders children when user is authenticated', () => {
            vi.mocked(useAuth).mockReturnValue({
                user: { email: 'user@example.com', name: 'User' },
                token: 'jwt-token-xyz',
                isReady: true,
                restoreError: null,
                login: vi.fn(),
                logout: vi.fn(),
                retrySession: vi.fn(),
            });
            render(
                <AuthGate>
                    <div data-testid="child-content">App</div>
                </AuthGate>
            );
            expect(screen.getByTestId('child-content')).toBeInTheDocument();
            expect(screen.queryByTestId('google-login-screen')).not.toBeInTheDocument();
        });

        it('hides children when user has only token but no user object', () => {
            vi.mocked(useAuth).mockReturnValue({
                user: null,
                token: 'dangling-token',
                isReady: true,
                restoreError: null,
                login: vi.fn(),
                logout: vi.fn(),
                retrySession: vi.fn(),
            });
            render(
                <AuthGate>
                    <div data-testid="child-content">App</div>
                </AuthGate>
            );
            expect(screen.queryByTestId('child-content')).not.toBeInTheDocument();
        });

        it('waits for stored-session validation before rendering children', () => {
            vi.mocked(useAuth).mockReturnValue({
                user: { email: 'user@example.com', name: 'User' },
                token: 'jwt-token-xyz',
                isReady: false,
                restoreError: null,
                login: vi.fn(),
                logout: vi.fn(),
                retrySession: vi.fn(),
            });
            render(
                <AuthGate>
                    <div data-testid="child-content">App</div>
                </AuthGate>
            );
            expect(screen.getByRole('status')).toHaveTextContent(
                'Restoring your session...'
            );
            expect(screen.queryByTestId('child-content')).not.toBeInTheDocument();
        });

        it('shows recovery actions when stored-session restore fails', () => {
            const retrySession = vi.fn();
            const logout = vi.fn();
            vi.mocked(useAuth).mockReturnValue({
                user: { email: 'user@example.com', name: 'User' },
                token: 'jwt-token-xyz',
                isReady: true,
                restoreError: 'We could not restore your session. Please try again or sign out.',
                login: vi.fn(),
                logout,
                retrySession,
            });
            render(
                <AuthGate>
                    <div data-testid="child-content">App</div>
                </AuthGate>
            );

            expect(screen.getByRole('alert')).toHaveTextContent(
                'We could not restore your session. Please try again or sign out.'
            );
            expect(screen.getByRole('button', { name: 'Try again' })).toBeInTheDocument();
            expect(screen.getByRole('button', { name: 'Sign out' })).toBeInTheDocument();
            expect(screen.queryByTestId('child-content')).not.toBeInTheDocument();
            expect(screen.queryByTestId('google-login-screen')).not.toBeInTheDocument();
        });

        it('wires retry and logout actions for session restore failures', () => {
            const retrySession = vi.fn();
            const logout = vi.fn();
            vi.mocked(useAuth).mockReturnValue({
                user: { email: 'user@example.com', name: 'User' },
                token: 'jwt-token-xyz',
                isReady: true,
                restoreError: 'We could not restore your session. Please try again or sign out.',
                login: vi.fn(),
                logout,
                retrySession,
            });
            render(
                <AuthGate>
                    <div data-testid="child-content">App</div>
                </AuthGate>
            );

            fireEvent.click(screen.getByRole('button', { name: 'Try again' }));
            fireEvent.click(screen.getByRole('button', { name: 'Sign out' }));

            expect(retrySession).toHaveBeenCalledTimes(1);
            expect(logout).toHaveBeenCalledTimes(1);
        });
    });
});
