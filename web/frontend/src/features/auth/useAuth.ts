import { useCallback, useEffect, useSyncExternalStore } from 'react';

import { readRequestedTenantId, setVerifiedTenantId } from '@/services/api';
import { cloudAuthApi } from '@/services/cloudAuthApi';
import type { CloudTenant } from '@/types/api';

export interface AuthUser {
    email: string;
    name: string;
    picture?: string;
}

interface AuthState {
    user: AuthUser | null;
    token: string | null;
    tenants: CloudTenant[];
    selected_tenant_id: string | null;
    expires_at?: number | null;
    is_ready: boolean;
    restore_error: string | null;
}

interface UseAuthResult {
    user: AuthUser | null;
    token: string | null;
    tenants?: CloudTenant[];
    selectedTenantId?: string | null;
    isReady: boolean;
    restoreError: string | null;
    login: (user: AuthUser, tokenOrTenants: string | CloudTenant[]) => void;
    logout: () => void;
    retrySession: () => void;
}

const STORAGE_KEY = 'jobscout_auth';
const TOKEN_REFRESH_SKEW_MS = 60_000;
const FALLBACK_TOKEN_TTL_MS = 55 * 60 * 1000;
const BOOTSTRAP_RETRY_DELAY_MS = 15_000;
const REFRESH_RETRY_DELAY_MS = 15_000;
const MAX_BOOTSTRAP_RETRIES = 2;
const RESTORE_SESSION_ERROR =
    'We could not restore your session. Please try again or sign out.';
const EMPTY_AUTH_STATE: AuthState = {
    user: null,
    token: null,
    tenants: [],
    selected_tenant_id: null,
    expires_at: null,
    is_ready: true,
    restore_error: null,
};

let authState: AuthState | null = null;
let refreshTimer: ReturnType<typeof setTimeout> | null = null;
let bootstrapRetryTimer: ReturnType<typeof setTimeout> | null = null;
let refreshInFlight: Promise<void> | null = null;
let bootstrapInFlight: Promise<void> | null = null;
let cookieBootstrapInFlight: Promise<void> | null = null;
let hasBootstrappedStoredSession = false;
let bootstrapRetryCount = 0;
let authVersion = 0;
const listeners = new Set<() => void>();

function hostedAuthRequired(): boolean {
    return import.meta.env.PROD
        || String(import.meta.env.VITE_AUTH_REQUIRED ?? '').toLowerCase() === 'true';
}

function decodeBase64Url(value: string): string {
    const normalized = value.replace(/-/g, '+').replace(/_/g, '/');
    const padded = normalized.padEnd(Math.ceil(normalized.length / 4) * 4, '=');
    return atob(padded);
}

function decodeTokenExpiry(token: string): number {
    try {
        const [, payload] = token.split('.');
        if (!payload) {
            return Date.now() + FALLBACK_TOKEN_TTL_MS;
        }

        const claims = JSON.parse(decodeBase64Url(payload)) as { exp?: unknown };
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

function selectVerifiedTenant(tenants: CloudTenant[]): string | null {
    const requestedTenantId = readRequestedTenantId();
    const selected =
        tenants.find((tenant) => tenant.id === requestedTenantId)
        ?? tenants.find((tenant) => tenant.is_default)
        ?? tenants[0]
        ?? null;
    const selectedTenantId = selected?.id ?? null;
    setVerifiedTenantId(selectedTenantId);
    return selectedTenantId;
}

function createAuthState(
    user: AuthUser,
    token: string | null,
    expiresAt: number | null = null,
    isReady = true,
    tenants: CloudTenant[] = []
): AuthState {
    return {
        user,
        token,
        tenants,
        selected_tenant_id: tenants.length > 0 ? selectVerifiedTenant(tenants) : null,
        expires_at: token ? expiresAt ?? decodeTokenExpiry(token) : expiresAt,
        is_ready: isReady,
        restore_error: null,
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
    if (import.meta.env.PROD || typeof localStorage === 'undefined') {
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
    bootstrapRetryCount = 0;
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
    bootstrapRetryCount = 0;
    authState = { ...EMPTY_AUTH_STATE };
    setVerifiedTenantId(null);
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
        if (import.meta.env.PROD || typeof localStorage === 'undefined') {
            return { ...EMPTY_AUTH_STATE, is_ready: !hostedAuthRequired() };
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
            tenants: [],
            selected_tenant_id: null,
            expires_at:
                typeof parsed.expires_at === 'number'
                    ? parsed.expires_at
                    : decodeTokenExpiry(parsed.token),
            is_ready: false,
            restore_error: null,
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

function setRestoreErrorState(): void {
    const currentAuth = getAuthState();
    authState = {
        ...currentAuth,
        is_ready: true,
        restore_error: RESTORE_SESSION_ERROR,
    };
    emitChange();
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
            const accessToken = response.data.access_token;
            if (!accessToken) {
                return;
            }
            applyAuthState(
                createAuthState(
                    {
                        email: response.data.user.email,
                        name: response.data.user.name,
                        picture: response.data.user.picture ?? undefined,
                    },
                    accessToken
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
            bootstrapRetryCount += 1;
            if (bootstrapRetryCount >= MAX_BOOTSTRAP_RETRIES) {
                setRestoreErrorState();
                return;
            }
            scheduleBootstrapRetry(expectedVersion, expectedToken);
        } finally {
            bootstrapInFlight = null;
        }
    })();

    return bootstrapInFlight;
}

async function bootstrapCookieSession(): Promise<void> {
    if (!hostedAuthRequired() || getAuthState().token || cookieBootstrapInFlight) {
        return cookieBootstrapInFlight ?? Promise.resolve();
    }
    cookieBootstrapInFlight = (async () => {
        try {
            const [userResponse, tenantsResponse] = await Promise.all([
                cloudAuthApi.getCurrentUser(),
                cloudAuthApi.listTenants(),
            ]);
            applyAuthState(
                createAuthState(
                    {
                        email: userResponse.data.email,
                        name: userResponse.data.name,
                        picture: userResponse.data.picture ?? undefined,
                    },
                    null,
                    userResponse.data.session_expires_at ?? null,
                    true,
                    tenantsResponse.data
                )
            );
        } catch {
            clearAuthState();
        } finally {
            cookieBootstrapInFlight = null;
        }
    })();
    return cookieBootstrapInFlight;
}

export function useAuth(): UseAuthResult {
    const auth = useSyncExternalStore(subscribe, getSnapshot, getSnapshot);

    const login = useCallback((user: AuthUser, tokenOrTenants: string | CloudTenant[]) => {
        hasBootstrappedStoredSession = false;
        if (Array.isArray(tokenOrTenants)) {
            applyAuthState(createAuthState(user, null, null, true, tokenOrTenants));
            return;
        }
        applyAuthState(createAuthState(user, tokenOrTenants));
    }, []);

    const logout = useCallback(() => {
        hasBootstrappedStoredSession = false;
        const maybeLogout = cloudAuthApi.logout?.();
        clearAuthState();
        void maybeLogout?.catch(() => undefined);
    }, []);

    const retrySession = useCallback(() => {
        const currentAuth = getAuthState();
        if (!currentAuth.token) {
            void bootstrapCookieSession();
            return;
        }
        clearBootstrapRetryTimer();
        bootstrapRetryCount = 0;
        hasBootstrappedStoredSession = false;
        authVersion += 1;
        authState = {
            ...currentAuth,
            is_ready: false,
            restore_error: null,
        };
        emitChange();
        void bootstrapStoredSession();
    }, []);

    useEffect(() => {
        scheduleTokenRefresh();
        void bootstrapStoredSession();
        void bootstrapCookieSession();
    }, []);

    return {
        user: auth.user,
        token: auth.token,
        tenants: auth.tenants,
        selectedTenantId: auth.selected_tenant_id,
        isReady: auth.is_ready,
        restoreError: auth.restore_error,
        login,
        logout,
        retrySession,
    };
}

export function __resetAuthForTests(): void {
    clearRefreshTimer();
    clearBootstrapRetryTimer();
    refreshInFlight = null;
    bootstrapInFlight = null;
    cookieBootstrapInFlight = null;
    hasBootstrappedStoredSession = false;
    bootstrapRetryCount = 0;
    authVersion = 0;
    authState = null;
}
