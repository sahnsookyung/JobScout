import { useCallback, useEffect, useRef, useState } from 'react';

export type Theme = 'light' | 'dark';

const STORAGE_KEY = 'jobscout-theme';
const THEME_COLORS: Record<Theme, string> = {
    light: '#F2EEE5',
    dark: '#15130F',
};

function readStoredTheme(): Theme | null {
    try {
        const saved = localStorage.getItem(STORAGE_KEY);
        return saved === 'dark' || saved === 'light' ? saved : null;
    } catch {
        return null;
    }
}

function readInitialTheme(): Theme {
    if (typeof document === 'undefined') return 'light';
    const savedTheme = readStoredTheme();
    if (savedTheme) return savedTheme;
    const attr = document.documentElement.dataset.theme;
    if (attr === 'dark' || attr === 'light') return attr;
    return globalThis.matchMedia?.('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
}

function applyTheme(theme: Theme) {
    if (typeof document === 'undefined') return;

    document.documentElement.dataset.theme = theme;

    const themeColorMeta =
        document.head.querySelector<HTMLMetaElement>('meta[name="theme-color"][data-theme-color]') ??
        document.head.querySelector<HTMLMetaElement>('meta[name="theme-color"]');

    if (themeColorMeta) {
        themeColorMeta.content = THEME_COLORS[theme];
    }
}

export function useTheme() {
    const [theme, setTheme] = useState<Theme>(readInitialTheme);
    const hasExplicitPreferenceRef = useRef(readStoredTheme() !== null);

    useEffect(() => {
        applyTheme(theme);
        if (!hasExplicitPreferenceRef.current) {
            return;
        }
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
            if (hasExplicitPreferenceRef.current) {
                return;
            }
            setTheme(event.matches ? 'dark' : 'light');
        };
        mq.addEventListener('change', handler);
        return () => mq.removeEventListener('change', handler);
    }, []);

    const setExplicitTheme = useCallback((next: Theme) => {
        hasExplicitPreferenceRef.current = true;
        setTheme(next);
    }, []);

    const toggle = useCallback(() => {
        hasExplicitPreferenceRef.current = true;
        setTheme((current) => (current === 'dark' ? 'light' : 'dark'));
    }, []);

    return { theme, setTheme: setExplicitTheme, toggle };
}
