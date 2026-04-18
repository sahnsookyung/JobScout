import { useCallback, useEffect, useState } from 'react';

export type Theme = 'light' | 'dark';

const STORAGE_KEY = 'jobscout-theme';

function readInitialTheme(): Theme {
    if (typeof document === 'undefined') return 'light';
    const attr = document.documentElement.getAttribute('data-theme');
    if (attr === 'dark' || attr === 'light') return attr;
    try {
        const saved = localStorage.getItem(STORAGE_KEY);
        if (saved === 'dark' || saved === 'light') return saved;
    } catch {
        /* ignore */
    }
    return globalThis.matchMedia?.('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
}

export function useTheme() {
    const [theme, setThemeState] = useState<Theme>(readInitialTheme);

    useEffect(() => {
        document.documentElement.setAttribute('data-theme', theme);
        try {
            localStorage.setItem(STORAGE_KEY, theme);
        } catch {
            /* ignore */
        }
    }, [theme]);

    // Follow the OS if the user never made an explicit choice
    useEffect(() => {
        const mq = globalThis.matchMedia?.('(prefers-color-scheme: dark)');
        if (!mq) return;
        const handler = (event: MediaQueryListEvent) => {
            try {
                if (localStorage.getItem(STORAGE_KEY)) return;
            } catch {
                /* ignore */
            }
            setThemeState(event.matches ? 'dark' : 'light');
        };
        mq.addEventListener('change', handler);
        return () => mq.removeEventListener('change', handler);
    }, []);

    const setTheme = useCallback((next: Theme) => {
        setThemeState(next);
    }, []);

    const toggle = useCallback(() => {
        setThemeState((current) => (current === 'dark' ? 'light' : 'dark'));
    }, []);

    return { theme, setTheme, toggle };
}
