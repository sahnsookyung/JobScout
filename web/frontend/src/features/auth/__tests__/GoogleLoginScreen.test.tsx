import { render, screen, act } from '@testing-library/react';
import { GoogleLoginScreen } from '../GoogleLoginScreen';
import { useAuth } from '../useAuth';

vi.mock('../useAuth', () => ({
    useAuth: vi.fn(() => ({
        login: vi.fn(),
        user: null,
        token: null,
        logout: vi.fn(),
    })),
}));

describe('GoogleLoginScreen', () => {
    beforeEach(() => {
        vi.useFakeTimers();
        vi.stubEnv('VITE_GOOGLE_CLIENT_ID', 'test-client-id-abc');
        document.getElementById('google-gsi')?.remove();
        delete (window as any).google;
    });

    afterEach(() => {
        vi.runOnlyPendingTimers();
        vi.useRealTimers();
        vi.unstubAllEnvs();
        document.getElementById('google-gsi')?.remove();
        delete (window as any).google;
    });

    describe('static rendering', () => {
        it('renders the JobScout heading', () => {
            render(<GoogleLoginScreen />);
            expect(screen.getByText('JobScout')).toBeInTheDocument();
        });

        it('renders the sign-in sub-text', () => {
            render(<GoogleLoginScreen />);
            expect(screen.getByText('Sign in to continue')).toBeInTheDocument();
        });

        it('appends the Google GSI script to document.head', () => {
            render(<GoogleLoginScreen />);
            const script = document.getElementById('google-gsi') as HTMLScriptElement | null;
            expect(script).not.toBeNull();
            expect(script?.src).toContain('accounts.google.com');
        });

        it('does not add a duplicate GSI script if one already exists', () => {
            render(<GoogleLoginScreen />);
            render(<GoogleLoginScreen />);
            const scripts = document.querySelectorAll('#google-gsi');
            expect(scripts.length).toBe(1);
        });
    });

    describe('Google SDK polling', () => {
        it('calls google.accounts.id.initialize when window.google becomes available', () => {
            const mockInitialize = vi.fn();
            const mockRenderButton = vi.fn();
            (window as any).google = {
                accounts: { id: { initialize: mockInitialize, renderButton: mockRenderButton } },
            };

            render(<GoogleLoginScreen />);
            act(() => { vi.advanceTimersByTime(200); });

            expect(mockInitialize).toHaveBeenCalledWith(
                expect.objectContaining({ client_id: 'test-client-id-abc' })
            );
        });

        it('calls google.accounts.id.renderButton with the button ref', () => {
            const mockInitialize = vi.fn();
            const mockRenderButton = vi.fn();
            (window as any).google = {
                accounts: { id: { initialize: mockInitialize, renderButton: mockRenderButton } },
            };

            render(<GoogleLoginScreen />);
            act(() => { vi.advanceTimersByTime(200); });

            expect(mockRenderButton).toHaveBeenCalledWith(
                expect.any(HTMLElement),
                expect.objectContaining({ theme: 'outline', size: 'large' })
            );
        });

        it('does not initialize when window.google is not yet available', () => {
            const mockInitialize = vi.fn();
            render(<GoogleLoginScreen />);
            act(() => { vi.advanceTimersByTime(50); });
            expect(mockInitialize).not.toHaveBeenCalled();
        });

        it('stops polling after google SDK becomes available', () => {
            const mockInitialize = vi.fn();
            const mockRenderButton = vi.fn();
            (window as any).google = {
                accounts: { id: { initialize: mockInitialize, renderButton: mockRenderButton } },
            };

            render(<GoogleLoginScreen />);
            act(() => { vi.advanceTimersByTime(300); });

            // initialize should only be called once even after multiple ticks
            expect(mockInitialize).toHaveBeenCalledTimes(1);
        });

        it('clears the polling interval on unmount', () => {
            const clearSpy = vi.spyOn(globalThis, 'clearInterval');
            const { unmount } = render(<GoogleLoginScreen />);
            unmount();
            expect(clearSpy).toHaveBeenCalled();
        });
    });

    describe('login callback', () => {
        it('calls login() with parsed JWT payload user and token', () => {
            const mockLogin = vi.fn();
            vi.mocked(useAuth).mockReturnValue({
                login: mockLogin,
                user: null,
                token: null,
                logout: vi.fn(),
            });

            let capturedCallback: ((resp: { credential: string }) => void) | undefined;
            (window as any).google = {
                accounts: {
                    id: {
                        initialize: vi.fn((config: any) => { capturedCallback = config.callback; }),
                        renderButton: vi.fn(),
                    },
                },
            };

            render(<GoogleLoginScreen />);
            act(() => { vi.advanceTimersByTime(200); });

            // Create a minimal JWT: header.payload.signature
            const payload = { email: 'user@test.com', name: 'Test User', picture: 'https://img/p.jpg' };
            const encodedPayload = btoa(JSON.stringify(payload));
            const fakeJwt = `eyJhbGciOiJSUzI1NiJ9.${encodedPayload}.sig`;

            capturedCallback?.({ credential: fakeJwt });

            expect(mockLogin).toHaveBeenCalledWith(
                expect.objectContaining({ email: 'user@test.com', name: 'Test User' }),
                fakeJwt
            );
        });

        it('includes picture in user when JWT contains it', () => {
            const mockLogin = vi.fn();
            vi.mocked(useAuth).mockReturnValue({
                login: mockLogin,
                user: null,
                token: null,
                logout: vi.fn(),
            });

            let capturedCallback: ((resp: { credential: string }) => void) | undefined;
            (window as any).google = {
                accounts: {
                    id: {
                        initialize: vi.fn((config: any) => { capturedCallback = config.callback; }),
                        renderButton: vi.fn(),
                    },
                },
            };

            render(<GoogleLoginScreen />);
            act(() => { vi.advanceTimersByTime(200); });

            const payload = { email: 'pic@test.com', name: 'Pic User', picture: 'https://cdn/photo.jpg' };
            const fakeJwt = `header.${btoa(JSON.stringify(payload))}.sig`;
            capturedCallback?.({ credential: fakeJwt });

            expect(mockLogin).toHaveBeenCalledWith(
                expect.objectContaining({ picture: 'https://cdn/photo.jpg' }),
                fakeJwt
            );
        });

        it('falls back to email for name when JWT name is missing', () => {
            const mockLogin = vi.fn();
            vi.mocked(useAuth).mockReturnValue({
                login: mockLogin,
                user: null,
                token: null,
                logout: vi.fn(),
            });

            let capturedCallback: ((resp: { credential: string }) => void) | undefined;
            (window as any).google = {
                accounts: {
                    id: {
                        initialize: vi.fn((config: any) => { capturedCallback = config.callback; }),
                        renderButton: vi.fn(),
                    },
                },
            };

            render(<GoogleLoginScreen />);
            act(() => { vi.advanceTimersByTime(200); });

            // No 'name' field in payload
            const payload = { email: 'noname@test.com' };
            const fakeJwt = `header.${btoa(JSON.stringify(payload))}.sig`;
            capturedCallback?.({ credential: fakeJwt });

            expect(mockLogin).toHaveBeenCalledWith(
                expect.objectContaining({ email: 'noname@test.com', name: 'noname@test.com' }),
                fakeJwt
            );
        });

        it('handles malformed JWT gracefully without throwing', () => {
            const mockLogin = vi.fn();
            vi.mocked(useAuth).mockReturnValue({
                login: mockLogin,
                user: null,
                token: null,
                logout: vi.fn(),
            });

            let capturedCallback: ((resp: { credential: string }) => void) | undefined;
            (window as any).google = {
                accounts: {
                    id: {
                        initialize: vi.fn((config: any) => { capturedCallback = config.callback; }),
                        renderButton: vi.fn(),
                    },
                },
            };

            render(<GoogleLoginScreen />);
            act(() => { vi.advanceTimersByTime(200); });

            // Should not throw even with invalid JWT
            expect(() => {
                capturedCallback?.({ credential: 'not.a.valid.jwt' });
            }).not.toThrow();
        });
    });
});
