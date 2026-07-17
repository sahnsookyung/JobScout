const TURNSTILE_TOKEN_KEY = 'jobscout_turnstile_token';
const TURNSTILE_TOKEN_ISSUED_AT_KEY = 'jobscout_turnstile_token_issued_at';
const TURNSTILE_VERIFIED_KEY = 'jobscout_turnstile_verified';
const TURNSTILE_TOKEN_MAX_AGE_MS = 4 * 60 * 1000;

export const TURNSTILE_RESET_EVENT = 'jobscout:turnstile-reset';

function sessionStorageOrNull(): Storage | null {
    try {
        return typeof globalThis.window === 'undefined' ? null : globalThis.sessionStorage;
    } catch {
        return null;
    }
}

function removeToken(storage: Storage): void {
    try {
        storage.removeItem(TURNSTILE_TOKEN_KEY);
        storage.removeItem(TURNSTILE_TOKEN_ISSUED_AT_KEY);
    } catch {
        // Storage can be blocked in privacy-restricted browser contexts.
    }
}

export function storeTurnstileVerification(token: string, now = Date.now()): void {
    const storage = sessionStorageOrNull();
    const normalizedToken = token.trim();
    if (!storage || !normalizedToken) {
        clearTurnstileVerification();
        return;
    }
    try {
        storage.setItem(TURNSTILE_TOKEN_KEY, normalizedToken);
        storage.setItem(TURNSTILE_TOKEN_ISSUED_AT_KEY, String(now));
        storage.setItem(TURNSTILE_VERIFIED_KEY, '1');
    } catch {
        clearTurnstileVerification();
    }
}

export function hasTurnstileVerification(): boolean {
    const storage = sessionStorageOrNull();
    try {
        return storage?.getItem(TURNSTILE_VERIFIED_KEY) === '1';
    } catch {
        return false;
    }
}

export function readFreshTurnstileToken(now = Date.now()): string | null {
    const storage = sessionStorageOrNull();
    if (!storage) return null;

    let token = '';
    let issuedAt = Number.NaN;
    try {
        token = storage.getItem(TURNSTILE_TOKEN_KEY)?.trim() ?? '';
        issuedAt = Number(storage.getItem(TURNSTILE_TOKEN_ISSUED_AT_KEY));
    } catch {
        return null;
    }
    const age = now - issuedAt;
    if (
        !token
        || !Number.isFinite(issuedAt)
        || issuedAt <= 0
        || age < 0
        || age > TURNSTILE_TOKEN_MAX_AGE_MS
    ) {
        removeToken(storage);
        return null;
    }
    return token;
}

export function clearTurnstileVerification(): void {
    const storage = sessionStorageOrNull();
    if (!storage) return;
    removeToken(storage);
    try {
        storage.removeItem(TURNSTILE_VERIFIED_KEY);
    } catch {
        // Storage can be blocked in privacy-restricted browser contexts.
    }
}

export function requestTurnstileReset(): void {
    clearTurnstileVerification();
    if (typeof globalThis.window !== 'undefined') {
        globalThis.window.dispatchEvent(new globalThis.window.Event(TURNSTILE_RESET_EVENT));
    }
}
