import { act, renderHook } from '@testing-library/react';
import { AxiosHeaders, type AxiosResponse, type InternalAxiosRequestConfig } from 'axios';

vi.mock('@/services/cloudAuthApi', () => ({
    cloudAuthApi: {
        getCurrentUser: vi.fn(),
        refreshSession: vi.fn(),
    },
}));

import { cloudAuthApi } from '@/services/cloudAuthApi';
import type { CloudAuthExchangeResponse, CloudUser } from '@/types/api';
import { useAuth, __resetAuthForTests } from '../useAuth';

const STORAGE_KEY = 'jobscout_auth';

function axiosResponse<T>(data: T): AxiosResponse<T> {
    const config: InternalAxiosRequestConfig = { headers: new AxiosHeaders() };
    return { data, status: 200, statusText: 'OK', headers: {}, config };
}

function buildCloudUser(overrides: Partial<{
    id: string;
    email: string;
    name: string;
    picture?: string;
}> = {}): CloudUser {
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

function makeJwt(payload: Record<string, unknown>): string {
    return `header.${btoa(JSON.stringify(payload))}.sig`;
}

function makeBase64UrlJwt(payload: Record<string, unknown>): string {
    const encoded = Buffer.from(JSON.stringify(payload), 'utf8').toString('base64url');
    return `header.${encoded}.sig`;
}

type RefreshSessionResponse = AxiosResponse<CloudAuthExchangeResponse>;

const REFRESH_USER = {
    email: 'refresh@example.com',
    name: 'Refresh User',
} as const;
const STALE_BOOTSTRAP_USER = {
    email: 'stale@example.com',
    name: 'Stale User',
} as const;
const OLD_APP_TOKEN = 'old-app-token';
const VALID_APP_TOKEN = 'valid-app-token';

function storeAuthSession(overrides: Partial<{
    user: { email: string; name: string; picture?: string };
    token: string;
    expires_at: number;
}> = {}): void {
    localStorage.setItem(
        STORAGE_KEY,
        JSON.stringify({
            user:
                overrides.user ?? {
                    email: 'stored@example.com',
                    name: 'Stored User',
                },
            token: overrides.token ?? 'stored-token',
            expires_at: overrides.expires_at ?? Date.now() + 120_000,
        })
    );
}

function mockCurrentUser(overrides: Partial<{
    id: string;
    email: string;
    name: string;
    picture?: string;
}> = {}): void {
    vi.mocked(cloudAuthApi.getCurrentUser).mockResolvedValue(axiosResponse(buildCloudUser(overrides)));
}

function refreshResponse(
    token: string,
    user: Partial<{ id: string; email: string; name: string; picture?: string }> = REFRESH_USER,
): RefreshSessionResponse {
    return axiosResponse({
        access_token: token,
        token_type: 'Bearer',
        user: buildCloudUser(user),
    });
}

function seedRefreshableSession(
    overrides: Partial<{
        token: string;
        expires_at: number;
    }> = {}
): void {
    storeAuthSession({
        user: { ...REFRESH_USER },
        token: overrides.token ?? OLD_APP_TOKEN,
        expires_at: overrides.expires_at ?? Date.now() + 61_000,
    });
    mockCurrentUser(REFRESH_USER);
}

async function advanceRefreshWindow(): Promise<void> {
    await act(async () => {
        await vi.advanceTimersByTimeAsync(2_000);
    });
}

async function renderAuthWithRefreshFailure(error: unknown) {
    seedRefreshableSession();
    vi.mocked(cloudAuthApi.refreshSession).mockRejectedValue(error);

    const hook = renderHook(() => useAuth());

    await flushAuthEffects();
    await advanceRefreshWindow();

    return hook;
}

function createRefreshDeferred() {
    return createDeferred<RefreshSessionResponse>();
}

async function startPendingRefresh() {
    const refresh = createRefreshDeferred();
    seedRefreshableSession();
    vi.mocked(cloudAuthApi.refreshSession).mockReturnValue(refresh.promise);

    const hook = renderHook(() => useAuth());

    await flushAuthEffects();
    await advanceRefreshWindow();

    act(() => {
        hook.result.current.login(
            { email: 'new@example.com', name: 'New User' },
            'brand-new-token'
        );
    });

    return { refresh, ...hook };
}

function expectLatestLoginToWin(result: { current: ReturnType<typeof useAuth> }) {
    expect(result.current.user?.email).toBe('new@example.com');
    expect(result.current.token).toBe('brand-new-token');
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
        vi.stubGlobal('localStorage', storageMock);
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
            mockCurrentUser({ email: 'alice@example.com', name: 'Alice' });
            storeAuthSession({
                user: { email: 'alice@example.com', name: 'Alice' },
                token: 'stored-token-123',
            });

            const { result } = renderHook(() => useAuth());
            await flushAuthEffects();

            expect(result.current.user?.email).toBe('alice@example.com');
            expect(result.current.token).toBe('stored-token-123');
            expect(result.current.isReady).toBe(true);
        });

        it('loads stored user with picture field', async () => {
            mockCurrentUser({
                email: 'bob@example.com',
                name: 'Bob',
                picture: 'https://example.com/pic.jpg',
            });
            storeAuthSession({
                user: {
                    email: 'bob@example.com',
                    name: 'Bob',
                    picture: 'https://example.com/pic.jpg',
                },
                token: 'tok-bob',
            });

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
            storeAuthSession({
                user: { email: 'expired@example.com', name: 'Expired User' },
                token: 'expired-token',
                expires_at: Date.now() - 1000,
            });

            const { result } = renderHook(() => useAuth());

            expect(result.current.user).toBeNull();
            expect(result.current.token).toBeNull();
            expect(localStorage.getItem(STORAGE_KEY)).toBeNull();
            expect(result.current.isReady).toBe(true);
        });

        it('derives expiry from a JWT exp string when expires_at is missing', async () => {
            const exp = Math.floor(Date.now() / 1000) + 120;
            mockCurrentUser({ email: 'jwt@example.com', name: 'JWT User' });
            localStorage.setItem(
                STORAGE_KEY,
                JSON.stringify({
                    user: { email: 'jwt@example.com', name: 'JWT User' },
                    token: makeJwt({ exp: String(exp) }),
                })
            );

            const { result } = renderHook(() => useAuth());
            await flushAuthEffects();

            const stored = JSON.parse(localStorage.getItem(STORAGE_KEY)!);
            expect(result.current.token).toBeTruthy();
            expect(stored.expires_at).toBe(exp * 1000);
        });

        it('derives expiry from a base64url-encoded JWT payload', async () => {
            const exp = Math.floor(Date.now() / 1000) + 240;
            mockCurrentUser({ email: 'jwt-url@example.com', name: 'JWT Url User' });
            localStorage.setItem(
                STORAGE_KEY,
                JSON.stringify({
                    user: { email: 'jwt-url@example.com', name: 'JWT Url User' },
                    token: makeBase64UrlJwt({ exp, tag: '???' }),
                })
            );

            const { result } = renderHook(() => useAuth());
            await flushAuthEffects();

            const stored = JSON.parse(localStorage.getItem(STORAGE_KEY)!);
            expect(result.current.token).toBeTruthy();
            expect(stored.expires_at).toBe(exp * 1000);
        });

        it('derives expiry from a JWT exp number when expires_at is missing', async () => {
            const exp = Math.floor(Date.now() / 1000) + 180;
            mockCurrentUser({ email: 'jwt-number@example.com', name: 'JWT Number User' });
            localStorage.setItem(
                STORAGE_KEY,
                JSON.stringify({
                    user: { email: 'jwt-number@example.com', name: 'JWT Number User' },
                    token: makeJwt({ exp }),
                })
            );

            const { result } = renderHook(() => useAuth());
            await flushAuthEffects();

            const stored = JSON.parse(localStorage.getItem(STORAGE_KEY)!);
            expect(result.current.token).toBeTruthy();
            expect(stored.expires_at).toBe(exp * 1000);
        });

        it('falls back to a conservative expiry when a JWT exp claim is malformed', async () => {
            vi.setSystemTime(new Date('2026-04-10T10:00:00.000Z'));
            mockCurrentUser({ email: 'jwt-bad@example.com', name: 'JWT Bad User' });
            localStorage.setItem(
                STORAGE_KEY,
                JSON.stringify({
                    user: { email: 'jwt-bad@example.com', name: 'JWT Bad User' },
                    token: makeJwt({ exp: 'soon' }),
                })
            );

            const { result } = renderHook(() => useAuth());
            await flushAuthEffects();

            const stored = JSON.parse(localStorage.getItem(STORAGE_KEY)!);
            expect(result.current.token).toBeTruthy();
            expect(stored.expires_at).toBe(Date.now() + 55 * 60 * 1000);
        });

        it('returns empty auth when the stored session is missing a user', () => {
            localStorage.setItem(
                STORAGE_KEY,
                JSON.stringify({
                    token: 'stored-token',
                    expires_at: Date.now() + 120_000,
                })
            );

            const { result } = renderHook(() => useAuth());

            expect(result.current.user).toBeNull();
            expect(result.current.token).toBeNull();
        });

        it('returns empty auth when the stored session is missing a token', () => {
            localStorage.setItem(
                STORAGE_KEY,
                JSON.stringify({
                    user: { email: 'missing@example.com', name: 'Missing Token' },
                    expires_at: Date.now() + 120_000,
                })
            );

            const { result } = renderHook(() => useAuth());

            expect(result.current.user).toBeNull();
            expect(result.current.token).toBeNull();
        });

        it('returns empty auth when localStorage is unavailable', () => {
            vi.stubGlobal('localStorage', undefined);

            const { result } = renderHook(() => useAuth());

            expect(result.current.user).toBeNull();
            expect(result.current.token).toBeNull();
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

        it('falls back to a conservative expiry for opaque login tokens', () => {
            vi.useFakeTimers();
            vi.setSystemTime(new Date('2026-04-10T10:00:00.000Z'));
            const { result } = renderHook(() => useAuth());

            act(() => {
                result.current.login(
                    { email: 'opaque@example.com', name: 'Opaque User' },
                    'opaque-token'
                );
            });

            const stored = JSON.parse(localStorage.getItem(STORAGE_KEY)!);
            expect(stored.expires_at).toBe(Date.now() + 55 * 60 * 1000);
            vi.useRealTimers();
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

        it('clears auth state even when localStorage becomes unavailable before logout', () => {
            const { result } = renderHook(() => useAuth());

            act(() => {
                result.current.login({ email: 'a@b.com', name: 'A' }, 'tok');
            });

            vi.stubGlobal('localStorage', undefined);

            act(() => {
                result.current.logout();
            });

            expect(result.current.user).toBeNull();
            expect(result.current.token).toBeNull();
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
            storeAuthSession({
                user: { email: 'stale@example.com', name: 'Stale User' },
                token: 'valid-app-token',
            });
            mockCurrentUser({ email: 'fresh@example.com', name: 'Fresh User' });

            const { result } = renderHook(() => useAuth());

            expect(result.current.isReady).toBe(false);
            await flushAuthEffects();

            expect(cloudAuthApi.getCurrentUser).toHaveBeenCalled();
            expect(result.current.user?.name).toBe('Fresh User');
            expect(result.current.isReady).toBe(true);
        });

        it('keeps the stored session pending when bootstrap hits a transient error', async () => {
            storeAuthSession({
                user: { ...STALE_BOOTSTRAP_USER },
                token: VALID_APP_TOKEN,
            });
            vi.mocked(cloudAuthApi.getCurrentUser).mockRejectedValue({
                response: { status: 503 },
            });

            const { result } = renderHook(() => useAuth());

            expect(result.current.isReady).toBe(false);
            await flushAuthEffects();

            expect(result.current.user?.email).toBe(STALE_BOOTSTRAP_USER.email);
            expect(result.current.token).toBe(VALID_APP_TOKEN);
            expect(result.current.isReady).toBe(false);
        });

        it('marks a freshly logged-in session as pending when bootstrap later hits a transient error', async () => {
            vi.mocked(cloudAuthApi.getCurrentUser).mockRejectedValue({
                response: { status: 503 },
            });

            const initial = renderHook(() => useAuth());

            act(() => {
                initial.result.current.login(
                    { email: 'fresh@example.com', name: 'Fresh User' },
                    'fresh-token'
                );
            });

            const mirrored = renderHook(() => useAuth());
            await flushAuthEffects();

            expect(cloudAuthApi.getCurrentUser).toHaveBeenCalledTimes(1);
            expect(initial.result.current.user?.email).toBe('fresh@example.com');
            expect(mirrored.result.current.user?.email).toBe('fresh@example.com');
            expect(initial.result.current.isReady).toBe(false);
            expect(mirrored.result.current.isReady).toBe(false);
        });

        it('retries bootstrap after a transient error and restores readiness on success', async () => {
            storeAuthSession({
                user: { email: 'retry@example.com', name: 'Retry User' },
                token: VALID_APP_TOKEN,
            });
            vi.mocked(cloudAuthApi.getCurrentUser)
                .mockRejectedValueOnce({ response: { status: 503 } })
                .mockResolvedValueOnce(axiosResponse(
                    buildCloudUser({
                        email: 'retry@example.com',
                        name: 'Retry User',
                    }),
                ));

            const { result } = renderHook(() => useAuth());

            await flushAuthEffects();
            expect(result.current.isReady).toBe(false);

            await act(async () => {
                await vi.advanceTimersByTimeAsync(15_000);
            });

            expect(cloudAuthApi.getCurrentUser).toHaveBeenCalledTimes(2);
            expect(result.current.isReady).toBe(true);
            expect(result.current.user?.email).toBe('retry@example.com');
        });

        it('surfaces a recovery state after repeated bootstrap failures', async () => {
            storeAuthSession({
                user: { email: 'retry@example.com', name: 'Retry User' },
                token: VALID_APP_TOKEN,
            });
            vi.mocked(cloudAuthApi.getCurrentUser).mockRejectedValue({
                response: { status: 503 },
            });

            const { result } = renderHook(() => useAuth());

            await flushAuthEffects();
            expect(result.current.isReady).toBe(false);
            expect(result.current.restoreError).toBeNull();

            await act(async () => {
                await vi.advanceTimersByTimeAsync(15_000);
            });

            expect(cloudAuthApi.getCurrentUser).toHaveBeenCalledTimes(2);
            expect(result.current.isReady).toBe(true);
            expect(result.current.restoreError).toBe(
                'We could not restore your session. Please try again or sign out.'
            );
        });

        it('cancels a scheduled bootstrap retry when the session changes', async () => {
            storeAuthSession({
                user: { email: 'retry@example.com', name: 'Retry User' },
                token: VALID_APP_TOKEN,
            });
            vi.mocked(cloudAuthApi.getCurrentUser).mockRejectedValue({
                response: { status: 503 },
            });

            const { result } = renderHook(() => useAuth());

            await flushAuthEffects();
            act(() => {
                result.current.login(
                    { email: 'fresh@example.com', name: 'Fresh User' },
                    'fresh-token'
                );
            });

            await act(async () => {
                await vi.advanceTimersByTimeAsync(15_000);
            });

            expect(cloudAuthApi.getCurrentUser).toHaveBeenCalledTimes(1);
            expect(result.current.user?.email).toBe('fresh@example.com');
        });

        it('lets users retry session restore manually after repeated failures', async () => {
            storeAuthSession({
                user: { email: 'retry@example.com', name: 'Retry User' },
                token: VALID_APP_TOKEN,
            });
            vi.mocked(cloudAuthApi.getCurrentUser)
                .mockRejectedValueOnce({ response: { status: 503 } })
                .mockRejectedValueOnce({ response: { status: 503 } })
                .mockResolvedValueOnce(axiosResponse(
                    buildCloudUser({
                        email: 'retry@example.com',
                        name: 'Retry User',
                    }),
                ));

            const { result } = renderHook(() => useAuth());

            await flushAuthEffects();
            await act(async () => {
                await vi.advanceTimersByTimeAsync(15_000);
            });

            expect(result.current.restoreError).toBeTruthy();

            act(() => {
                result.current.retrySession();
            });
            await flushAuthEffects();

            expect(cloudAuthApi.getCurrentUser).toHaveBeenCalledTimes(3);
            expect(result.current.isReady).toBe(true);
            expect(result.current.restoreError).toBeNull();
            expect(result.current.user?.email).toBe('retry@example.com');
        });

        it('treats manual retry as a new session boundary for stale refresh responses', async () => {
            const refresh = createRefreshDeferred();
            seedRefreshableSession();
            vi.mocked(cloudAuthApi.refreshSession).mockReturnValue(refresh.promise);

            const { result } = renderHook(() => useAuth());

            await flushAuthEffects();
            await advanceRefreshWindow();

            vi.mocked(cloudAuthApi.getCurrentUser).mockResolvedValueOnce(
                axiosResponse(buildCloudUser(REFRESH_USER)),
            );

            act(() => {
                result.current.retrySession();
            });
            await flushAuthEffects();

            await act(async () => {
                refresh.resolve(refreshResponse('stale-refresh-token', {
                    email: 'old@example.com',
                    name: 'Old User',
                }));
                await Promise.resolve();
            });

            expect(result.current.user?.email).toBe(REFRESH_USER.email);
            expect(result.current.token).toBe(OLD_APP_TOKEN);
            expect(result.current.restoreError).toBeNull();
        });

        it('clears the stored session when bootstrap is unauthorized', async () => {
            storeAuthSession({
                user: { ...STALE_BOOTSTRAP_USER },
                token: VALID_APP_TOKEN,
            });
            vi.mocked(cloudAuthApi.getCurrentUser).mockRejectedValue({
                response: { status: 403 },
            });

            const { result } = renderHook(() => useAuth());
            await flushAuthEffects();

            expect(result.current.user).toBeNull();
            expect(result.current.token).toBeNull();
            expect(localStorage.getItem(STORAGE_KEY)).toBeNull();
        });

        it('refreshes the token before expiry', async () => {
            seedRefreshableSession();
            vi.mocked(cloudAuthApi.refreshSession).mockResolvedValue(refreshResponse('new-app-token'));

            const { result } = renderHook(() => useAuth());

            await flushAuthEffects();

            expect(result.current.user?.email).toBe(REFRESH_USER.email);
            expect(result.current.isReady).toBe(true);

            await advanceRefreshWindow();

            expect(cloudAuthApi.refreshSession).toHaveBeenCalled();
            expect(result.current.token).toBe('new-app-token');
        });

        it.each([
            ['hits a transient error', { response: { status: 503 } }],
            ['fails without a response object', new Error('offline')],
            ['fails with a non-numeric status', { response: { status: 'temporarily_unavailable' } }],
        ])('does not log out when refresh %s', async (_label, error) => {
            const { result } = await renderAuthWithRefreshFailure(error);

            expect(result.current.user?.email).toBe(REFRESH_USER.email);
            expect(result.current.token).toBe(OLD_APP_TOKEN);
        });

        it('logs out when refresh is unauthorized', async () => {
            seedRefreshableSession();
            vi.mocked(cloudAuthApi.refreshSession).mockRejectedValue({
                response: { status: 401 },
            });

            const { result } = renderHook(() => useAuth());

            await flushAuthEffects();

            await advanceRefreshWindow();

            expect(result.current.user).toBeNull();
            expect(result.current.token).toBeNull();
            expect(result.current.isReady).toBe(true);
        });

        it('does not let a stale bootstrap response restore a logged-out session', async () => {
            const bootstrap = createDeferred<AxiosResponse<CloudUser>>();
            storeAuthSession({
                user: { ...STALE_BOOTSTRAP_USER },
                token: VALID_APP_TOKEN,
            });
            vi.mocked(cloudAuthApi.getCurrentUser).mockReturnValue(
                bootstrap.promise
            );

            const { result } = renderHook(() => useAuth());

            act(() => {
                result.current.logout();
            });

            await act(async () => {
                bootstrap.resolve(axiosResponse(
                    buildCloudUser({
                        email: 'old@example.com',
                        name: 'Old User',
                    }),
                ));
                await Promise.resolve();
            });

            expect(result.current.user).toBeNull();
            expect(result.current.token).toBeNull();
        });

        it('does not let a stale bootstrap error clear a newer login', async () => {
            const bootstrap = createDeferred<AxiosResponse<CloudUser>>();
            storeAuthSession({
                user: { ...STALE_BOOTSTRAP_USER },
                token: VALID_APP_TOKEN,
            });
            vi.mocked(cloudAuthApi.getCurrentUser).mockReturnValue(
                bootstrap.promise
            );

            const { result } = renderHook(() => useAuth());

            act(() => {
                result.current.login(
                    { email: 'fresh@example.com', name: 'Fresh User' },
                    'fresh-token'
                );
            });

            await act(async () => {
                bootstrap.reject({ response: { status: 503 } });
                await Promise.resolve();
            });

            expect(result.current.user?.email).toBe('fresh@example.com');
            expect(result.current.token).toBe('fresh-token');
        });

        it('deduplicates concurrent bootstrap requests across hook instances', async () => {
            const bootstrap = createDeferred<AxiosResponse<CloudUser>>();
            storeAuthSession({
                user: { ...STALE_BOOTSTRAP_USER },
                token: VALID_APP_TOKEN,
            });
            vi.mocked(cloudAuthApi.getCurrentUser).mockReturnValue(
                bootstrap.promise
            );

            renderHook(() => useAuth());
            await flushAuthEffects();
            renderHook(() => useAuth());
            await flushAuthEffects();

            expect(cloudAuthApi.getCurrentUser).toHaveBeenCalledTimes(1);

            await act(async () => {
                bootstrap.resolve(axiosResponse(
                    buildCloudUser({
                        email: 'stale@example.com',
                        name: 'Stale User',
                    }),
                ));
                await Promise.resolve();
            });
        });

        it('does not let a stale refresh response overwrite a newer login', async () => {
            const { refresh, result } = await startPendingRefresh();

            await act(async () => {
                refresh.resolve(refreshResponse('stale-refresh-token', {
                    email: 'old@example.com',
                    name: 'Old User',
                }));
                await Promise.resolve();
            });

            expectLatestLoginToWin(result);
        });

        it('does not let a stale refresh rejection clear a newer login', async () => {
            const { refresh, result } = await startPendingRefresh();

            await act(async () => {
                refresh.reject({ response: { status: 503 } });
                await Promise.resolve();
            });

            expectLatestLoginToWin(result);
        });

        it('ignores a second refresh trigger while one is already in flight', async () => {
            const refresh = createRefreshDeferred();
            seedRefreshableSession();
            vi.mocked(cloudAuthApi.refreshSession).mockReturnValue(refresh.promise);

            renderHook(() => useAuth());
            await flushAuthEffects();

            await advanceRefreshWindow();

            renderHook(() => useAuth());
            await flushAuthEffects();
            await act(async () => {
                await vi.advanceTimersByTimeAsync(0);
            });

            expect(cloudAuthApi.refreshSession).toHaveBeenCalledTimes(1);

            await act(async () => {
                refresh.resolve(refreshResponse('new-app-token'));
                await Promise.resolve();
            });
        });
    });
});
