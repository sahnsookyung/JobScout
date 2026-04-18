import { act, renderHook } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { useTheme } from '../useTheme';

function mockMatchMedia(matches = false) {
    const listeners = new Set<(event: MediaQueryListEvent) => void>();
    const mediaQuery = {
        matches,
        media: '(prefers-color-scheme: dark)',
        addEventListener: vi.fn((_: string, listener: (event: MediaQueryListEvent) => void) => {
            listeners.add(listener);
        }),
        removeEventListener: vi.fn((_: string, listener: (event: MediaQueryListEvent) => void) => {
            listeners.delete(listener);
        }),
        dispatch(nextMatches: boolean) {
            mediaQuery.matches = nextMatches;
            listeners.forEach((listener) => listener({ matches: nextMatches } as MediaQueryListEvent));
        },
    };

    vi.stubGlobal('matchMedia', vi.fn(() => mediaQuery));
    return mediaQuery;
}

describe('useTheme', () => {
    beforeEach(() => {
        const store: Record<string, string> = {};
        vi.stubGlobal('localStorage', {
            getItem: (key: string) => store[key] ?? null,
            setItem: (key: string, value: string) => {
                store[key] = value;
            },
            removeItem: (key: string) => {
                delete store[key];
            },
        });
        delete document.documentElement.dataset.theme;
    });

    it('uses the current document theme and persists updates', () => {
        document.documentElement.dataset.theme = 'dark';
        mockMatchMedia(false);

        const { result } = renderHook(() => useTheme());

        expect(result.current.theme).toBe('dark');

        act(() => {
            result.current.toggle();
        });

        expect(document.documentElement.dataset.theme).toBe('light');
        expect(localStorage.getItem('jobscout-theme')).toBe('light');
    });

    it('follows system preference changes until the user saves a theme', () => {
        const mediaQuery = mockMatchMedia(false);

        const { result } = renderHook(() => useTheme());

        act(() => {
            mediaQuery.dispatch(true);
        });

        expect(result.current.theme).toBe('dark');

        act(() => {
            result.current.setTheme('light');
        });

        act(() => {
            mediaQuery.dispatch(true);
        });

        expect(result.current.theme).toBe('light');
    });
});
