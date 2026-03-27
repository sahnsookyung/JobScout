import { useState, useCallback } from 'react';

export interface AuthUser {
    email: string;
    name: string;
    picture?: string;
}

interface AuthState {
    user: AuthUser | null;
    token: string | null;
}

const STORAGE_KEY = 'jobscout_auth';

function loadStoredAuth(): AuthState {
    try {
        const raw = localStorage.getItem(STORAGE_KEY);
        if (!raw) return { user: null, token: null };
        return JSON.parse(raw) as AuthState;
    } catch {
        return { user: null, token: null };
    }
}

export function useAuth() {
    const [auth, setAuth] = useState<AuthState>(loadStoredAuth);

    const login = useCallback((user: AuthUser, token: string) => {
        const next = { user, token };
        localStorage.setItem(STORAGE_KEY, JSON.stringify(next));
        setAuth(next);
    }, []);

    const logout = useCallback(() => {
        localStorage.removeItem(STORAGE_KEY);
        setAuth({ user: null, token: null });
    }, []);

    return { user: auth.user, token: auth.token, login, logout };
}
