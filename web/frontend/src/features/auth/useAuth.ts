import { useCallback, useEffect, useSyncExternalStore } from 'react';

import { cloudAuthApi } from '@/services/cloudAuthApi';

export interface AuthUser {
    email: string;
    name: string;
    picture?: string;
}

interface AuthState {
    user: AuthUser | null;
    token: string | null;
    expires_at?: number | null;
    is_ready: boolean;
}

const STORAGE_KEY = 'jobscout_auth';
const TOKEN_REFRESH_SKEW_MS = 60_000;
const FALLBACK_TOKEN_TTL_MS = 55 * 60 * 1000;
const BOOTSTRAP_RETRY_DELAY_MS = 15_000;
const REFRESH_RETRY_DELAY_MS = 15_000;
const EMPTY_AUTH_STATE: AuthState = {
    user: null,
    token: null,
    expires_at: null,
    is_ready: true,
};

let authState: AuthState | null = null;
let refreshTimer: ReturnType<typeof setTimeout> | null = null;
let bootstrapRetryTimer: ReturnType<typeof setTimeout> | null = null;
let refreshInFlight: Promise<void> | null = null;
let bootstrapInFlight: Promise<void> | null = null;
let hasBootstrappedStoredSession = false;
let authVersion = 0;
const listeners = new Set<() => void>();

function decodeTokenExpiry(token: string): number {
    try {
        const [, payload] = token.split('.');
        if (!payload) {
            return Date.now() + FALLBACK_TOKEN_TTL_MS;
        }

        const claims = JSON.parse(atob(payload)) as { exp?: unknown };
        if (typeof claims.exp === 'number') {
            return claims.exp * 1000;
        }
        if (typeof claims.exp === 'string' && /^\d+$/.test(claims.exp)) {
            return Number(claims.exp) * 1000;
        }
    } catch {
        // Fall back to a conservative default when the token is opaque.
    }

    return Date.now() + FALLBACK_TOKEN_TTL_MS;
}

function createAuthState(
    user: AuthUser,
    token: string,
    expiresAt: number | null = null,
    isReady = true
): AuthState {
    return {
        user,
        token,
        expires_at: expiresAt ?? decodeTokenExpiry(token),
        is_ready: isReady,
    };
}

function emitChange(): void {
    listeners.forEach((listener) => listener());
}

function clearRefreshTimer(): void {
    if (refreshTimer) {
        clearTimeout(refreshTimer);
        refreshTimer = null;
    }
}

function clearBootstrapRetryTimer(): void {
    if (bootstrapRetryTimer) {
        clearTimeout(bootstrapRetryTimer);
        bootstrapRetryTimer = null;
    }
}

function getAuthState(): AuthState {
    authState ??= loadStoredAuth();
    return authState;
}

function persistAuthState(next: AuthState): void {
    if (typeof localStorage === 'undefined') {
        return;
    }
    if (!next.user || !next.token) {
        localStorage.removeItem(STORAGE_KEY);
        return;
    }

    localStorage.setItem(
        STORAGE_KEY,
        JSON.stringify({
            user: next.user,
            token: next.token,
            expires_at: next.expires_at,
        })
    );
}

function applyAuthState(next: AuthState): void {
    clearBootstrapRetryTimer();
    authState = next;
    authVersion += 1;
    persistAuthState(next);
    scheduleTokenRefresh();
    emitChange();
}

function updateAuthReadiness(isReady: boolean): void {
    const currentAuth = getAuthState();
    if (currentAuth.is_ready === isReady) {
        return;
    }
    authState = { ...currentAuth, is_ready: isReady };
    persistAuthState(authState);
    emitChange();
}

function clearAuthState(): void {
    clearRefreshTimer();
    clearBootstrapRetryTimer();
    authState = EMPTY_AUTH_STATE;
    authVersion += 1;
    if (typeof localStorage !== 'undefined') {
        localStorage.removeItem(STORAGE_KEY);
    }
    emitChange();
}

function subscribe(listener: () => void): () => void {
    listeners.add(listener);
    return () => listeners.delete(listener);
}

function getSnapshot(): AuthState {
    return getAuthState();
}

function loadStoredAuth(): AuthState {
    try {
        if (typeof localStorage === 'undefined') {
            return EMPTY_AUTH_STATE;
        }
        const raw = localStorage.getItem(STORAGE_KEY);
        if (!raw) return EMPTY_AUTH_STATE;
        const parsed = JSON.parse(raw) as AuthState;
        if (!parsed.token || !parsed.user) {
            return EMPTY_AUTH_STATE;
        }
        if (parsed.expires_at && parsed.expires_at <= Date.now()) {
            localStorage.removeItem(STORAGE_KEY);
            return EMPTY_AUTH_STATE;
        }
        return {
            user: parsed.user,
            token: parsed.token,
            expires_at:
                typeof parsed.expires_at === 'number'
                    ? parsed.expires_at
                    : decodeTokenExpiry(parsed.token),
            is_ready: false,
        };
    } catch {
        return EMPTY_AUTH_STATE;
    }
}

function getErrorStatus(error: unknown): number | null {
    if (typeof error !== 'object' || error === null || !('response' in error)) {
        return null;
    }

    const response = (error as { response?: { status?: unknown } }).response;
    return typeof response?.status === 'number' ? response.status : null;
}

function isAuthFailure(error: unknown): boolean {
    const status = getErrorStatus(error);
    return status === 401 || status === 403;
}

function isCurrentSession(expectedVersion: number, expectedToken: string): boolean {
    const currentAuth = getAuthState();
    return authVersion === expectedVersion && currentAuth.token === expectedToken;
}

function scheduleBootstrapRetry(expectedVersion: number, expectedToken: string): void {
    clearBootstrapRetryTimer();
    bootstrapRetryTimer = setTimeout(() => {
        if (!isCurrentSession(expectedVersion, expectedToken)) {
            return;
        }
        hasBootstrappedStoredSession = false;
        void bootstrapStoredSession();
    }, BOOTSTRAP_RETRY_DELAY_MS);
}

async function refreshAuthSession(): Promise<void> {
    const currentAuth = getAuthState();
    if (!currentAuth.token) {
        return;
    }
    if (refreshInFlight) {
        return refreshInFlight;
    }

    const expectedVersion = authVersion;
    const expectedToken = currentAuth.token;
    refreshInFlight = (async () => {
        try {
            const response = await cloudAuthApi.refreshSession();
            if (!isCurrentSession(expectedVersion, expectedToken)) {
                return;
            }
            applyAuthState(
                createAuthState(
                    {
                        email: response.data.user.email,
                        name: response.data.user.name,
                        picture: response.data.user.picture ?? undefined,
                    },
                    response.data.access_token
                )
            );
        } catch (error) {
            if (!isCurrentSession(expectedVersion, expectedToken)) {
                return;
            }

            if (isAuthFailure(error) || !currentAuth.expires_at || currentAuth.expires_at <= Date.now()) {
                clearAuthState();
                return;
            }

            scheduleTokenRefresh(
                Math.min(
                    REFRESH_RETRY_DELAY_MS,
                    Math.max(1_000, currentAuth.expires_at - Date.now() - 1_000)
                )
            );
        } finally {
            refreshInFlight = null;
        }
    })();

    return refreshInFlight;
}

function scheduleTokenRefresh(delayOverrideMs?: number): void {
    clearRefreshTimer();
    const currentAuth = getAuthState();
    if (!currentAuth.token || !currentAuth.expires_at) {
        return;
    }

    const refreshDelay =
        delayOverrideMs ??
        Math.max(0, currentAuth.expires_at - Date.now() - TOKEN_REFRESH_SKEW_MS);
    refreshTimer = setTimeout(() => {
        void refreshAuthSession();
    }, refreshDelay);
}

async function bootstrapStoredSession(): Promise<void> {
    const currentAuth = getAuthState();
    if (!currentAuth.token || hasBootstrappedStoredSession) {
        return;
    }
    if (bootstrapInFlight) {
        return bootstrapInFlight;
    }

    const expectedVersion = authVersion;
    const expectedToken = currentAuth.token;
    hasBootstrappedStoredSession = true;
    bootstrapInFlight = (async () => {
        try {
            const response = await cloudAuthApi.getCurrentUser();
            if (!isCurrentSession(expectedVersion, expectedToken)) {
                return;
            }
            applyAuthState(
                createAuthState(
                    {
                        email: response.data.email,
                        name: response.data.name,
                        picture: response.data.picture ?? undefined,
                    },
                    expectedToken,
                    currentAuth.expires_at ?? null
                )
            );
        } catch (error) {
            if (!isCurrentSession(expectedVersion, expectedToken)) {
                return;
            }

            if (isAuthFailure(error) || !currentAuth.expires_at || currentAuth.expires_at <= Date.now()) {
                clearAuthState();
                return;
            }

            updateAuthReadiness(false);
            scheduleBootstrapRetry(expectedVersion, expectedToken);
        } finally {
            bootstrapInFlight = null;
        }
    })();

    return bootstrapInFlight;
}

export function useAuth() {
    const auth = useSyncExternalStore(subscribe, getSnapshot, getSnapshot);

    const login = useCallback((user: AuthUser, token: string) => {
        hasBootstrappedStoredSession = false;
        applyAuthState(createAuthState(user, token));
    }, []);

    const logout = useCallback(() => {
        hasBootstrappedStoredSession = false;
        clearAuthState();
    }, []);

    useEffect(() => {
        scheduleTokenRefresh();
        void bootstrapStoredSession();
    }, []);

    return {
        user: auth.user,
        token: auth.token,
        isReady: auth.is_ready,
        login,
        logout,
    };
}

export function __resetAuthForTests(): void {
    clearRefreshTimer();
    clearBootstrapRetryTimer();
    refreshInFlight = null;
    bootstrapInFlight = null;
    hasBootstrappedStoredSession = false;
    authVersion = 0;
    authState = null;
}
