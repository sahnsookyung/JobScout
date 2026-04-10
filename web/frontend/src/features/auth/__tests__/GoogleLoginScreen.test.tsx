import { render, screen, act } from '@testing-library/react';
import { GoogleLoginScreen } from '../GoogleLoginScreen';
import { useAuth } from '../useAuth';
import { cloudAuthApi } from '@/services/cloudAuthApi';

vi.mock('../useAuth', () => ({
    useAuth: vi.fn(() => ({
        login: vi.fn(),
        user: null,
        token: null,
        logout: vi.fn(),
    })),
}));

vi.mock('@/services/cloudAuthApi', () => ({
    cloudAuthApi: {
        exchangeGoogleCredential: vi.fn(),
    },
}));

describe('GoogleLoginScreen', () => {
    beforeEach(() => {
        vi.useFakeTimers();
        vi.stubEnv('VITE_GOOGLE_CLIENT_ID', 'test-client-id-abc');
        document.getElementById('google-gsi')?.remove();
        delete (globalThis as any).google;
    });

    afterEach(() => {
        vi.runOnlyPendingTimers();
        vi.useRealTimers();
        vi.unstubAllEnvs();
        document.getElementById('google-gsi')?.remove();
        delete (globalThis as any).google;
    });

    describe('static rendering', () => {
        it('renders the JobScout heading', () => {
            render(<GoogleLoginScreen />);
            expect(screen.getByText('JobScout')).toBeInTheDocument();
        });

        it('renders the sign-in sub-text', () => {
            render(<GoogleLoginScreen />);
            expect(
                screen.getByText('Continue with Google to create an account or sign in')
            ).toBeInTheDocument();
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
            (globalThis as any).google = {
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
            (globalThis as any).google = {
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
            (globalThis as any).google = {
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
        /** Mount the screen with a captured Google callback and a fresh mockLogin. */
        function setupLoginCallback() {
            const mockLogin = vi.fn();
            vi.mocked(cloudAuthApi.exchangeGoogleCredential).mockResolvedValue({
                data: {
                    access_token: 'app-token-123',
                    token_type: 'Bearer',
                    user: {
                        id: 'user-1',
                        email: 'user@test.com',
                        name: 'Test User',
                        picture: 'https://img/p.jpg',
                        provider: 'google',
                        token_kind: 'google_id_token',
                    },
                },
            } as never);
            vi.mocked(useAuth).mockReturnValue({
                login: mockLogin,
                user: null,
                token: null,
                logout: vi.fn(),
            });
            let capturedCallback: ((resp: { credential: string }) => void) | undefined;
            (globalThis as any).google = {
                accounts: {
                    id: {
                        initialize: vi.fn((config: any) => { capturedCallback = config.callback; }),
                        renderButton: vi.fn(),
                    },
                },
            };
            render(<GoogleLoginScreen />);
            act(() => { vi.advanceTimersByTime(200); });
            return {
                mockLogin,
                fire: async (jwt: string) => {
                    await Promise.resolve(capturedCallback?.({ credential: jwt }));
                },
            };
        }

        it('exchanges the Google credential and stores the app token', async () => {
            const { mockLogin, fire } = setupLoginCallback();
            const payload = { email: 'user@test.com', name: 'Test User', picture: 'https://img/p.jpg' };
            const fakeJwt = `eyJhbGciOiJSUzI1NiJ9.${btoa(JSON.stringify(payload))}.sig`;
            await act(async () => {
                await fire(fakeJwt);
            });

            expect(cloudAuthApi.exchangeGoogleCredential).toHaveBeenCalledWith(fakeJwt);
            expect(mockLogin).toHaveBeenCalledWith(
                expect.objectContaining({ email: 'user@test.com', name: 'Test User' }),
                'app-token-123'
            );
        });

        it('includes the returned picture in the stored user', async () => {
            const { mockLogin, fire } = setupLoginCallback();
            const payload = { email: 'pic@test.com', name: 'Pic User', picture: 'https://cdn/photo.jpg' };
            const fakeJwt = `header.${btoa(JSON.stringify(payload))}.sig`;
            await act(async () => {
                await fire(fakeJwt);
            });

            expect(mockLogin).toHaveBeenCalledWith(
                expect.objectContaining({ picture: 'https://img/p.jpg' }),
                'app-token-123'
            );
        });

        it('uses the backend-returned identity rather than parsing the JWT locally', async () => {
            const { mockLogin, fire } = setupLoginCallback();
            const payload = { email: 'noname@test.com' };
            const fakeJwt = `header.${btoa(JSON.stringify(payload))}.sig`;
            await act(async () => {
                await fire(fakeJwt);
            });

            expect(mockLogin).toHaveBeenCalledWith(
                expect.objectContaining({ email: 'user@test.com', name: 'Test User' }),
                'app-token-123'
            );
        });

        it('shows an error when the exchange fails', async () => {
            const { fire, mockLogin } = setupLoginCallback();
            vi.mocked(cloudAuthApi.exchangeGoogleCredential).mockRejectedValueOnce(
                new Error('exchange failed')
            );

            await act(async () => {
                await fire('header.payload.sig');
            });

            expect(screen.getByRole('alert')).toHaveTextContent(
                'Sign-in failed. Please try again.'
            );
            expect(mockLogin).not.toHaveBeenCalled();
        });
    });
});
