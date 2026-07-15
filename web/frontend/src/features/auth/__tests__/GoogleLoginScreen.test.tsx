import { render, screen, act } from '@testing-library/react';
import { GoogleLoginScreen } from '../GoogleLoginScreen';
import { useAuth } from '../useAuth';
import { cloudAuthApi } from '@/services/cloudAuthApi';

const NOW_SECONDS = 1_800_000_000;

vi.mock('../useAuth', () => ({
    useAuth: vi.fn(() => ({
        login: vi.fn(),
        user: null,
        token: null,
        isReady: true,
        restoreError: null,
        logout: vi.fn(),
        retrySession: vi.fn(),
    })),
}));

vi.mock('@/services/cloudAuthApi', () => ({
    cloudAuthApi: {
        createGoogleLoginNonce: vi.fn(),
        exchangeGoogleCredential: vi.fn(),
    },
}));

function createDeferred<T>() {
    let resolve!: (value: T) => void;
    let reject!: (reason?: unknown) => void;
    const promise = new Promise<T>((res, rej) => {
        resolve = res;
        reject = rej;
    });
    return { promise, resolve, reject };
}

describe('GoogleLoginScreen', () => {
    beforeEach(() => {
        vi.useFakeTimers();
        vi.setSystemTime(NOW_SECONDS * 1000);
        vi.clearAllMocks();
        vi.stubEnv('VITE_GOOGLE_CLIENT_ID', 'test-client-id-abc');
        vi.mocked(cloudAuthApi.createGoogleLoginNonce).mockResolvedValue({
            data: { nonce: 'login-nonce-123', expires_at: NOW_SECONDS + 300 },
        } as never);
        document.getElementById('google-gsi')?.remove();
        delete (globalThis as any).google;
    });

    afterEach(() => {
        vi.clearAllTimers();
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
                screen.getByText(/Non-admin accounts and their uploaded data are deleted four hours/i)
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
        it('calls google.accounts.id.initialize when window.google becomes available', async () => {
            const mockInitialize = vi.fn();
            const mockRenderButton = vi.fn();
            (globalThis as any).google = {
                accounts: { id: { initialize: mockInitialize, renderButton: mockRenderButton } },
            };

            render(<GoogleLoginScreen />);
            await act(async () => {
                vi.advanceTimersByTime(200);
                await Promise.resolve();
            });

            expect(mockInitialize).toHaveBeenCalledWith(
                expect.objectContaining({
                    client_id: 'test-client-id-abc',
                    nonce: 'login-nonce-123',
                })
            );
        });

        it('calls google.accounts.id.renderButton with the button ref', async () => {
            const mockInitialize = vi.fn();
            const mockRenderButton = vi.fn();
            (globalThis as any).google = {
                accounts: { id: { initialize: mockInitialize, renderButton: mockRenderButton } },
            };

            render(<GoogleLoginScreen />);
            await act(async () => {
                vi.advanceTimersByTime(200);
                await Promise.resolve();
            });

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

        it('stops polling after google SDK becomes available', async () => {
            const mockInitialize = vi.fn();
            const mockRenderButton = vi.fn();
            (globalThis as any).google = {
                accounts: { id: { initialize: mockInitialize, renderButton: mockRenderButton } },
            };

            render(<GoogleLoginScreen />);
            await act(async () => {
                vi.advanceTimersByTime(300);
                await Promise.resolve();
            });

            // initialize should only be called once even after multiple ticks
            expect(mockInitialize).toHaveBeenCalledTimes(1);
        });

        it('renews the Google nonce before it expires', async () => {
            const mockInitialize = vi.fn();
            const mockRenderButton = vi.fn();
            vi.mocked(cloudAuthApi.createGoogleLoginNonce)
                .mockResolvedValueOnce({
                    data: { nonce: 'login-nonce-123', expires_at: NOW_SECONDS + 300 },
                } as never)
                .mockResolvedValueOnce({
                    data: { nonce: 'login-nonce-456', expires_at: NOW_SECONDS + 600 },
                } as never);
            (globalThis as any).google = {
                accounts: { id: { initialize: mockInitialize, renderButton: mockRenderButton } },
            };

            render(<GoogleLoginScreen />);
            await act(async () => {
                vi.advanceTimersByTime(200);
                await Promise.resolve();
            });
            await act(async () => {
                vi.advanceTimersByTime(239_999);
                await Promise.resolve();
            });
            expect(cloudAuthApi.createGoogleLoginNonce).toHaveBeenCalledTimes(1);
            await act(async () => {
                vi.advanceTimersByTime(1);
                await Promise.resolve();
            });

            expect(cloudAuthApi.createGoogleLoginNonce).toHaveBeenCalledTimes(2);
            expect(mockInitialize).toHaveBeenLastCalledWith(
                expect.objectContaining({ nonce: 'login-nonce-456' })
            );
            expect(mockRenderButton).toHaveBeenCalledTimes(2);
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
        async function setupLoginCallback(
            exchangeUserOverrides: Partial<{
                id: string;
                email: string;
                name: string;
                picture?: string;
                provider: string;
                token_kind: string;
            }> = {}
        ) {
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
                        ...exchangeUserOverrides,
                    },
                },
            } as never);
            vi.mocked(useAuth).mockReturnValue({
                login: mockLogin,
                user: null,
                token: null,
                isReady: true,
                restoreError: null,
                logout: vi.fn(),
                retrySession: vi.fn(),
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
            const { unmount } = render(<GoogleLoginScreen />);
            await act(async () => {
                vi.advanceTimersByTime(200);
                await Promise.resolve();
            });
            return {
                mockLogin,
                unmount,
                fire: async (jwt: string) => {
                    await Promise.resolve(capturedCallback?.({ credential: jwt }));
                },
            };
        }

        it('exchanges the Google credential and stores the app token', async () => {
            const { mockLogin, fire } = await setupLoginCallback();
            const payload = { email: 'user@test.com', name: 'Test User', picture: 'https://img/p.jpg' };
            const fakeJwt = `eyJhbGciOiJSUzI1NiJ9.${btoa(JSON.stringify(payload))}.sig`;
            await act(async () => {
                await fire(fakeJwt);
            });

            expect(cloudAuthApi.exchangeGoogleCredential).toHaveBeenCalledWith(
                fakeJwt,
                'login-nonce-123'
            );
            expect(mockLogin).toHaveBeenCalledWith(
                expect.objectContaining({ email: 'user@test.com', name: 'Test User' }),
                'app-token-123'
            );
        });

        it('includes the returned picture in the stored user', async () => {
            const { mockLogin, fire } = await setupLoginCallback();
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

        it('passes through an undefined picture when the backend omits it', async () => {
            const { mockLogin, fire } = await setupLoginCallback({ picture: undefined });

            await act(async () => {
                await fire('header.payload.sig');
            });

            expect(mockLogin).toHaveBeenCalledWith(
                expect.objectContaining({ picture: undefined }),
                'app-token-123'
            );
        });

        it('uses the backend-returned identity rather than parsing the JWT locally', async () => {
            const { mockLogin, fire } = await setupLoginCallback();
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
            const { fire, mockLogin } = await setupLoginCallback();
            vi.mocked(cloudAuthApi.exchangeGoogleCredential).mockRejectedValueOnce(
                new Error('exchange failed')
            );

            await act(async () => {
                await fire('header.payload.sig');
            });

            expect(screen.getByRole('alert')).toHaveTextContent(
                'Sign-in didn’t go through. Please try once more.'
            );
            expect(mockLogin).not.toHaveBeenCalled();
        });

        it('replaces a consumed nonce after an exchange failure', async () => {
            const { fire } = await setupLoginCallback();
            vi.mocked(cloudAuthApi.exchangeGoogleCredential).mockRejectedValueOnce(
                new Error('exchange failed')
            );

            await act(async () => {
                await fire('header.payload.sig');
                await Promise.resolve();
            });

            expect(cloudAuthApi.createGoogleLoginNonce).toHaveBeenCalledTimes(2);
        });

        it('shows a pending message while the credential exchange is in flight', async () => {
            const exchange = createDeferred<{
                data: {
                    access_token: string;
                    token_type: string;
                    user: {
                        id: string;
                        email: string;
                        name: string;
                        picture?: string;
                        provider: string;
                        token_kind: string;
                    };
                };
            }>();
            vi.mocked(cloudAuthApi.exchangeGoogleCredential).mockReturnValueOnce(
                exchange.promise as never
            );

            const { fire } = await setupLoginCallback();

            await act(async () => {
                void fire('header.payload.sig');
                await Promise.resolve();
            });

            expect(screen.getByText('Finishing sign-in')).toBeInTheDocument();

            await act(async () => {
                exchange.resolve({
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
                });
                await Promise.resolve();
            });

            expect(screen.queryByText('Finishing sign-in')).not.toBeInTheDocument();
        });

        it('ignores stale exchange responses when a newer sign-in attempt finishes first', async () => {
            const firstExchange = createDeferred<{
                data: {
                    access_token: string;
                    token_type: string;
                    user: {
                        id: string;
                        email: string;
                        name: string;
                        picture?: string;
                        provider: string;
                        token_kind: string;
                    };
                };
            }>();
            const secondExchange = createDeferred<{
                data: {
                    access_token: string;
                    token_type: string;
                    user: {
                        id: string;
                        email: string;
                        name: string;
                        picture?: string;
                        provider: string;
                        token_kind: string;
                    };
                };
            }>();

            const { fire, mockLogin } = await setupLoginCallback();
            vi.mocked(cloudAuthApi.exchangeGoogleCredential)
                .mockReturnValueOnce(firstExchange.promise as never)
                .mockReturnValueOnce(secondExchange.promise as never);

            await act(async () => {
                void fire('first.jwt.token');
                await Promise.resolve();
            });
            await act(async () => {
                void fire('second.jwt.token');
                await Promise.resolve();
            });

            await act(async () => {
                secondExchange.resolve({
                    data: {
                        access_token: 'newer-app-token',
                        token_type: 'Bearer',
                        user: {
                            id: 'user-2',
                            email: 'newer@test.com',
                            name: 'Newer User',
                            provider: 'google',
                            token_kind: 'google_id_token',
                        },
                    },
                });
                await Promise.resolve();
            });

            await act(async () => {
                firstExchange.resolve({
                    data: {
                        access_token: 'stale-app-token',
                        token_type: 'Bearer',
                        user: {
                            id: 'user-1',
                            email: 'stale@test.com',
                            name: 'Stale User',
                            provider: 'google',
                            token_kind: 'google_id_token',
                        },
                    },
                });
                await Promise.resolve();
            });

            expect(mockLogin).toHaveBeenCalledTimes(1);
            expect(mockLogin).toHaveBeenCalledWith(
                expect.objectContaining({ email: 'newer@test.com', name: 'Newer User' }),
                'newer-app-token'
            );
            expect(screen.queryByText('Finishing sign-in...')).not.toBeInTheDocument();
        });

        it('ignores exchange completions after the screen unmounts', async () => {
            const exchange = createDeferred<{
                data: {
                    access_token: string;
                    token_type: string;
                    user: {
                        id: string;
                        email: string;
                        name: string;
                        picture?: string;
                        provider: string;
                        token_kind: string;
                    };
                };
            }>();
            const { fire, mockLogin, unmount } = await setupLoginCallback();
            vi.mocked(cloudAuthApi.exchangeGoogleCredential).mockReturnValueOnce(
                exchange.promise as never
            );

            await act(async () => {
                void fire('header.payload.sig');
                await Promise.resolve();
            });

            unmount();

            await act(async () => {
                exchange.resolve({
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
                });
                await Promise.resolve();
            });

            expect(mockLogin).not.toHaveBeenCalled();
        });

        it('ignores Google callbacks that arrive after unmount', async () => {
            const { fire, mockLogin, unmount } = await setupLoginCallback();

            unmount();

            await act(async () => {
                await fire('post-unmount.jwt.token');
            });

            expect(cloudAuthApi.exchangeGoogleCredential).not.toHaveBeenCalled();
            expect(mockLogin).not.toHaveBeenCalled();
        });
    });
});
