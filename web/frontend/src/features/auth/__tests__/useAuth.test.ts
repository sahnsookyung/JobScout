import { renderHook, act } from '@testing-library/react';
import { useAuth } from '../useAuth';

const STORAGE_KEY = 'jobscout_auth';

// jsdom localStorage doesn't always expose .clear(); mock it directly.
const storageMock = (() => {
    let store: Record<string, string> = {};
    return {
        getItem: (key: string) => store[key] ?? null,
        setItem: (key: string, value: string) => { store[key] = String(value); },
        removeItem: (key: string) => { delete store[key]; },
        clear: () => { store = {}; },
        get length() { return Object.keys(store).length; },
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
    });

    describe('initialization', () => {
        it('returns null user and token when storage is empty', () => {
            const { result } = renderHook(() => useAuth());
            expect(result.current.user).toBeNull();
            expect(result.current.token).toBeNull();
        });

        it('loads stored user and token from localStorage on mount', () => {
            const stored = {
                user: { email: 'alice@example.com', name: 'Alice' },
                token: 'stored-token-123',
            };
            localStorage.setItem(STORAGE_KEY, JSON.stringify(stored));
            const { result } = renderHook(() => useAuth());
            expect(result.current.user?.email).toBe('alice@example.com');
            expect(result.current.token).toBe('stored-token-123');
        });

        it('loads stored user with picture field', () => {
            const stored = {
                user: { email: 'bob@example.com', name: 'Bob', picture: 'https://example.com/pic.jpg' },
                token: 'tok-bob',
            };
            localStorage.setItem(STORAGE_KEY, JSON.stringify(stored));
            const { result } = renderHook(() => useAuth());
            expect(result.current.user?.picture).toBe('https://example.com/pic.jpg');
        });

        it('returns null when localStorage contains invalid JSON', () => {
            localStorage.setItem(STORAGE_KEY, '{not: valid json{{');
            const { result } = renderHook(() => useAuth());
            expect(result.current.user).toBeNull();
            expect(result.current.token).toBeNull();
        });

        it('returns null when localStorage item is missing user field', () => {
            localStorage.setItem(STORAGE_KEY, JSON.stringify({ token: 'orphan-token' }));
            const { result } = renderHook(() => useAuth());
            // No user, so user should be undefined/null
            expect(result.current.user).toBeUndefined();
        });
    });

    describe('login', () => {
        it('sets user state after login', () => {
            const { result } = renderHook(() => useAuth());
            act(() => {
                result.current.login({ email: 'test@example.com', name: 'Test User' }, 'jwt-token');
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
        });

        it('accepts optional picture field', () => {
            const { result } = renderHook(() => useAuth());
            act(() => {
                result.current.login(
                    { email: 'a@b.com', name: 'A', picture: 'http://img.example.com/pic' },
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
            // Should not throw
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
});
