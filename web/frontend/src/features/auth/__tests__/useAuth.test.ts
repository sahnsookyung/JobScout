import { act, renderHook, waitFor } from '@testing-library/react';

vi.mock('@/services/cloudAuthApi', () => ({
    cloudAuthApi: {
        getCurrentUser: vi.fn(),
        refreshSession: vi.fn(),
    },
}));

import { cloudAuthApi } from '@/services/cloudAuthApi';
import { useAuth, __resetAuthForTests } from '../useAuth';

const STORAGE_KEY = 'jobscout_auth';

function buildCloudUser(overrides: Partial<{
    id: string;
    email: string;
    name: string;
    picture?: string;
}> = {}) {
    return {
        id: overrides.id ?? 'user-1',
        email: overrides.email ?? 'user@example.com',
        name: overrides.name ?? 'Example User',
        picture: overrides.picture,
        provider: 'google',
        token_kind: 'app_jwt',
    };
}

async function flushAuthEffects(): Promise<void> {
    await act(async () => {
        await Promise.resolve();
    });
}

function createDeferred<T>() {
    let resolve!: (value: T) => void;
    let reject!: (reason?: unknown) => void;
    const promise = new Promise<T>((res, rej) => {
        resolve = res;
        reject = rej;
    });
    return { promise, resolve, reject };
}

const storageMock = (() => {
    let store: Record<string, string> = {};
    return {
        getItem: (key: string) => store[key] ?? null,
        setItem: (key: string, value: string) => {
            store[key] = String(value);
        },
        removeItem: (key: string) => {
            delete store[key];
        },
        clear: () => {
            store = {};
        },
        get length() {
            return Object.keys(store).length;
        },
        key: (i: number) => Object.keys(store)[i] ?? null,
    };
})();

beforeAll(() => {
    vi.stubGlobal('localStorage', storageMock);
});

afterAll(() => {
    vi.unstubAllGlobals();
});

describe('useAuth', () => {
    beforeEach(() => {
        storageMock.clear();
        __resetAuthForTests();
        vi.clearAllMocks();
    });

    describe('initialization', () => {
        it('returns null user and token when storage is empty', () => {
            const { result } = renderHook(() => useAuth());
            expect(result.current.user).toBeNull();
            expect(result.current.token).toBeNull();
            expect(result.current.isReady).toBe(true);
        });

        it('loads stored user and token from localStorage on mount', async () => {
            vi.mocked(cloudAuthApi.getCurrentUser).mockResolvedValue({
                data: buildCloudUser({
                    email: 'alice@example.com',
                    name: 'Alice',
                }),
            } as never);
            localStorage.setItem(
                STORAGE_KEY,
                JSON.stringify({
                    user: { email: 'alice@example.com', name: 'Alice' },
                    token: 'stored-token-123',
                    expires_at: Date.now() + 120_000,
                })
            );

            const { result } = renderHook(() => useAuth());
            await flushAuthEffects();

            expect(result.current.user?.email).toBe('alice@example.com');
            expect(result.current.token).toBe('stored-token-123');
            expect(result.current.isReady).toBe(true);
        });

        it('loads stored user with picture field', async () => {
            vi.mocked(cloudAuthApi.getCurrentUser).mockResolvedValue({
                data: buildCloudUser({
                    email: 'bob@example.com',
                    name: 'Bob',
                    picture: 'https://example.com/pic.jpg',
                }),
            } as never);
            localStorage.setItem(
                STORAGE_KEY,
                JSON.stringify({
                    user: {
                        email: 'bob@example.com',
                        name: 'Bob',
                        picture: 'https://example.com/pic.jpg',
                    },
                    token: 'tok-bob',
                    expires_at: Date.now() + 120_000,
                })
            );

            const { result } = renderHook(() => useAuth());
            await flushAuthEffects();

            expect(result.current.user?.picture).toBe('https://example.com/pic.jpg');
            expect(result.current.isReady).toBe(true);
        });

        it('returns null when localStorage contains invalid JSON', () => {
            localStorage.setItem(STORAGE_KEY, '{not: valid json{{');

            const { result } = renderHook(() => useAuth());

            expect(result.current.user).toBeNull();
            expect(result.current.token).toBeNull();
            expect(result.current.isReady).toBe(true);
        });

        it('clears expired stored auth on initialization', () => {
            localStorage.setItem(
                STORAGE_KEY,
                JSON.stringify({
                    user: { email: 'expired@example.com', name: 'Expired User' },
                    token: 'expired-token',
                    expires_at: Date.now() - 1000,
                })
            );

            const { result } = renderHook(() => useAuth());

            expect(result.current.user).toBeNull();
            expect(result.current.token).toBeNull();
            expect(localStorage.getItem(STORAGE_KEY)).toBeNull();
            expect(result.current.isReady).toBe(true);
        });
    });

    describe('login', () => {
        it('sets user state after login', () => {
            const { result } = renderHook(() => useAuth());

            act(() => {
                result.current.login(
                    { email: 'test@example.com', name: 'Test User' },
                    'jwt-token'
                );
            });

            expect(result.current.user?.email).toBe('test@example.com');
            expect(result.current.user?.name).toBe('Test User');
        });

        it('sets token state after login', () => {
            const { result } = renderHook(() => useAuth());

            act(() => {
                result.current.login({ email: 'test@example.com', name: 'Test' }, 'my-jwt');
            });

            expect(result.current.token).toBe('my-jwt');
        });

        it('persists auth data to localStorage', () => {
            const { result } = renderHook(() => useAuth());

            act(() => {
                result.current.login({ email: 'x@y.com', name: 'X' }, 'tok-x');
            });

            const stored = JSON.parse(localStorage.getItem(STORAGE_KEY)!);
            expect(stored.user.email).toBe('x@y.com');
            expect(stored.token).toBe('tok-x');
            expect(typeof stored.expires_at).toBe('number');
        });

        it('accepts optional picture field', () => {
            const { result } = renderHook(() => useAuth());

            act(() => {
                result.current.login(
                    {
                        email: 'a@b.com',
                        name: 'A',
                        picture: 'http://img.example.com/pic',
                    },
                    'tok'
                );
            });

            expect(result.current.user?.picture).toBe('http://img.example.com/pic');
            const stored = JSON.parse(localStorage.getItem(STORAGE_KEY)!);
            expect(stored.user.picture).toBe('http://img.example.com/pic');
        });

        it('overwrites previous login data', () => {
            const { result } = renderHook(() => useAuth());

            act(() => {
                result.current.login({ email: 'first@example.com', name: 'First' }, 'tok-1');
            });
            act(() => {
                result.current.login({ email: 'second@example.com', name: 'Second' }, 'tok-2');
            });

            expect(result.current.user?.email).toBe('second@example.com');
            expect(result.current.token).toBe('tok-2');
        });

        it('synchronizes login state across multiple hook instances', () => {
            const first = renderHook(() => useAuth());
            const second = renderHook(() => useAuth());

            act(() => {
                first.result.current.login(
                    { email: 'sync@example.com', name: 'Sync User' },
                    'shared-token'
                );
            });

            expect(second.result.current.user?.email).toBe('sync@example.com');
            expect(second.result.current.token).toBe('shared-token');
        });
    });

    describe('logout', () => {
        it('clears user state', () => {
            const { result } = renderHook(() => useAuth());

            act(() => {
                result.current.login({ email: 'a@b.com', name: 'A' }, 'tok');
            });
            act(() => {
                result.current.logout();
            });

            expect(result.current.user).toBeNull();
        });

        it('clears token state', () => {
            const { result } = renderHook(() => useAuth());

            act(() => {
                result.current.login({ email: 'a@b.com', name: 'A' }, 'tok');
            });
            act(() => {
                result.current.logout();
            });

            expect(result.current.token).toBeNull();
        });

        it('removes entry from localStorage', () => {
            const { result } = renderHook(() => useAuth());

            act(() => {
                result.current.login({ email: 'a@b.com', name: 'A' }, 'tok');
            });
            act(() => {
                result.current.logout();
            });

            expect(localStorage.getItem(STORAGE_KEY)).toBeNull();
        });

        it('is a no-op when already logged out', () => {
            const { result } = renderHook(() => useAuth());

            act(() => {
                result.current.logout();
            });

            expect(result.current.user).toBeNull();
            expect(result.current.token).toBeNull();
        });

        it('synchronizes logout across multiple hook instances', () => {
            const first = renderHook(() => useAuth());
            const second = renderHook(() => useAuth());

            act(() => {
                first.result.current.login(
                    { email: 'sync@example.com', name: 'Sync User' },
                    'shared-token'
                );
            });
            act(() => {
                second.result.current.logout();
            });

            expect(first.result.current.user).toBeNull();
            expect(first.result.current.token).toBeNull();
        });
    });

    describe('callback stability', () => {
        it('login reference is stable across re-renders', () => {
            const { result, rerender } = renderHook(() => useAuth());
            const first = result.current.login;
            rerender();
            expect(result.current.login).toBe(first);
        });

        it('logout reference is stable across re-renders', () => {
            const { result, rerender } = renderHook(() => useAuth());
            const first = result.current.logout;
            rerender();
            expect(result.current.logout).toBe(first);
        });
    });

    describe('session lifecycle', () => {
        beforeEach(() => {
            vi.useFakeTimers();
        });

        afterEach(() => {
            vi.clearAllTimers();
            vi.useRealTimers();
        });

        it('bootstraps the current user from a stored valid token', async () => {
            localStorage.setItem(
                STORAGE_KEY,
                JSON.stringify({
                    user: { email: 'stale@example.com', name: 'Stale User' },
                    token: 'valid-app-token',
                    expires_at: Date.now() + 120_000,
                })
            );
            vi.mocked(cloudAuthApi.getCurrentUser).mockResolvedValue({
                data: {
                    id: 'user-1',
                    email: 'fresh@example.com',
                    name: 'Fresh User',
                    provider: 'google',
                    token_kind: 'app_jwt',
                },
            } as never);

            const { result } = renderHook(() => useAuth());

            expect(result.current.isReady).toBe(false);
            await flushAuthEffects();

            expect(cloudAuthApi.getCurrentUser).toHaveBeenCalled();
            expect(result.current.user?.name).toBe('Fresh User');
            expect(result.current.isReady).toBe(true);
        });

        it('keeps the stored session pending when bootstrap hits a transient error', async () => {
            localStorage.setItem(
                STORAGE_KEY,
                JSON.stringify({
                    user: { email: 'stale@example.com', name: 'Stale User' },
                    token: 'valid-app-token',
                    expires_at: Date.now() + 120_000,
                })
            );
            vi.mocked(cloudAuthApi.getCurrentUser).mockRejectedValue({
                response: { status: 503 },
            } as never);

            const { result } = renderHook(() => useAuth());

            expect(result.current.isReady).toBe(false);
            await flushAuthEffects();

            expect(result.current.user?.email).toBe('stale@example.com');
            expect(result.current.token).toBe('valid-app-token');
            expect(result.current.isReady).toBe(false);
        });

        it('refreshes the token before expiry', async () => {
            localStorage.setItem(
                STORAGE_KEY,
                JSON.stringify({
                    user: { email: 'refresh@example.com', name: 'Refresh User' },
                    token: 'old-app-token',
                    expires_at: Date.now() + 61_000,
                })
            );
            vi.mocked(cloudAuthApi.getCurrentUser).mockResolvedValue({
                data: {
                    id: 'user-1',
                    email: 'refresh@example.com',
                    name: 'Refresh User',
                    provider: 'google',
                    token_kind: 'app_jwt',
                },
            } as never);
            vi.mocked(cloudAuthApi.refreshSession).mockResolvedValue({
                data: {
                    access_token: 'new-app-token',
                    token_type: 'Bearer',
                    user: {
                        id: 'user-1',
                        email: 'refresh@example.com',
                        name: 'Refresh User',
                        provider: 'google',
                        token_kind: 'app_jwt',
                    },
                },
            } as never);

            const { result } = renderHook(() => useAuth());

            await flushAuthEffects();

            expect(result.current.user?.email).toBe('refresh@example.com');
            expect(result.current.isReady).toBe(true);

            await act(async () => {
                await vi.advanceTimersByTimeAsync(2_000);
            });

            expect(cloudAuthApi.refreshSession).toHaveBeenCalled();
            expect(result.current.token).toBe('new-app-token');
        });

        it('does not log out when refresh hits a transient error', async () => {
            localStorage.setItem(
                STORAGE_KEY,
                JSON.stringify({
                    user: { email: 'refresh@example.com', name: 'Refresh User' },
                    token: 'old-app-token',
                    expires_at: Date.now() + 61_000,
                })
            );
            vi.mocked(cloudAuthApi.getCurrentUser).mockResolvedValue({
                data: {
                    id: 'user-1',
                    email: 'refresh@example.com',
                    name: 'Refresh User',
                    provider: 'google',
                    token_kind: 'app_jwt',
                },
            } as never);
            vi.mocked(cloudAuthApi.refreshSession).mockRejectedValue(
                { response: { status: 503 } } as never
            );

            const { result } = renderHook(() => useAuth());

            await flushAuthEffects();

            expect(result.current.user?.email).toBe('refresh@example.com');

            await act(async () => {
                await vi.advanceTimersByTimeAsync(2_000);
            });

            expect(result.current.user?.email).toBe('refresh@example.com');
            expect(result.current.token).toBe('old-app-token');
        });

        it('logs out when refresh is unauthorized', async () => {
            localStorage.setItem(
                STORAGE_KEY,
                JSON.stringify({
                    user: { email: 'refresh@example.com', name: 'Refresh User' },
                    token: 'old-app-token',
                    expires_at: Date.now() + 61_000,
                })
            );
            vi.mocked(cloudAuthApi.getCurrentUser).mockResolvedValue({
                data: {
                    id: 'user-1',
                    email: 'refresh@example.com',
                    name: 'Refresh User',
                    provider: 'google',
                    token_kind: 'app_jwt',
                },
            } as never);
            vi.mocked(cloudAuthApi.refreshSession).mockRejectedValue({
                response: { status: 401 },
            } as never);

            const { result } = renderHook(() => useAuth());

            await flushAuthEffects();

            await act(async () => {
                await vi.advanceTimersByTimeAsync(2_000);
            });

            expect(result.current.user).toBeNull();
            expect(result.current.token).toBeNull();
            expect(result.current.isReady).toBe(true);
        });

        it('does not let a stale bootstrap response restore a logged-out session', async () => {
            const bootstrap = createDeferred<{ data: ReturnType<typeof buildCloudUser> }>();
            localStorage.setItem(
                STORAGE_KEY,
                JSON.stringify({
                    user: { email: 'stale@example.com', name: 'Stale User' },
                    token: 'valid-app-token',
                    expires_at: Date.now() + 120_000,
                })
            );
            vi.mocked(cloudAuthApi.getCurrentUser).mockReturnValue(
                bootstrap.promise as never
            );

            const { result } = renderHook(() => useAuth());

            act(() => {
                result.current.logout();
            });

            await act(async () => {
                bootstrap.resolve({
                    data: buildCloudUser({
                        email: 'old@example.com',
                        name: 'Old User',
                    }),
                });
                await Promise.resolve();
            });

            expect(result.current.user).toBeNull();
            expect(result.current.token).toBeNull();
        });

        it('does not let a stale refresh response overwrite a newer login', async () => {
            const refresh = createDeferred<{
                data: {
                    access_token: string;
                    token_type: string;
                    user: ReturnType<typeof buildCloudUser>;
                };
            }>();
            localStorage.setItem(
                STORAGE_KEY,
                JSON.stringify({
                    user: { email: 'refresh@example.com', name: 'Refresh User' },
                    token: 'old-app-token',
                    expires_at: Date.now() + 61_000,
                })
            );
            vi.mocked(cloudAuthApi.getCurrentUser).mockResolvedValue({
                data: buildCloudUser({
                    email: 'refresh@example.com',
                    name: 'Refresh User',
                }),
            } as never);
            vi.mocked(cloudAuthApi.refreshSession).mockReturnValue(refresh.promise as never);

            const { result } = renderHook(() => useAuth());

            await flushAuthEffects();
            await act(async () => {
                await vi.advanceTimersByTimeAsync(2_000);
            });

            act(() => {
                result.current.login(
                    { email: 'new@example.com', name: 'New User' },
                    'brand-new-token'
                );
            });

            await act(async () => {
                refresh.resolve({
                    data: {
                        access_token: 'stale-refresh-token',
                        token_type: 'Bearer',
                        user: buildCloudUser({
                            email: 'old@example.com',
                            name: 'Old User',
                        }),
                    },
                });
                await Promise.resolve();
            });

            expect(result.current.user?.email).toBe('new@example.com');
            expect(result.current.token).toBe('brand-new-token');
        });
    });
});
