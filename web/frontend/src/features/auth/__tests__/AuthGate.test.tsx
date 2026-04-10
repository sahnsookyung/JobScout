import { render, screen } from '@testing-library/react';
import { AuthGate } from '../AuthGate';

vi.mock('../useAuth', () => ({
    useAuth: vi.fn(() => ({
        user: null,
        token: null,
        isReady: true,
        login: vi.fn(),
        logout: vi.fn(),
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
                login: vi.fn(),
                logout: vi.fn(),
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
                login: vi.fn(),
                logout: vi.fn(),
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
                login: vi.fn(),
                logout: vi.fn(),
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
                login: vi.fn(),
                logout: vi.fn(),
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
    });
});
