/**
 * Vitest test setup file
 * Configures Testing Library matchers and utilities
 */

import '@testing-library/jest-dom';
import { cleanup } from '@testing-library/react';
import { afterEach, vi } from 'vitest';

// Cleanup after each test
afterEach(() => {
    cleanup();
});

// Mock window.matchMedia for components that use it
// Note: Using Object.defineProperty because jsdom doesn't implement matchMedia
// This is the recommended Jest/Vitest pattern for missing browser APIs
Object.defineProperty(window, 'matchMedia', {
    writable: true,
    value: vi.fn().mockImplementation(query => ({
        matches: false,
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
    })),
});

// Mock EventSource for SSE tests (usePipelineEvents hook)
class MockEventSource {
    static readonly CONNECTING = 0;
    static readonly OPEN = 1;
    static readonly CLOSED = 2;
    readonly CONNECTING = 0;
    readonly OPEN = 1;
    readonly CLOSED = 2;

    onopen: (() => void) | null = null;
    onmessage: ((event: MessageEvent) => void) | null = null;
    onerror: (() => void) | null = null;

    constructor(
        public url: string,
        public withCredentials: boolean = false
    ) {
        // Mock implementation - actual behavior controlled in tests
    }

    close(): void {
        // Mock implementation
    }
}

// Set up global EventSource mock
vi.stubGlobal('EventSource', MockEventSource);

// Mock IndexedDB for indexedDB.ts tests
const mockIndexedDB = {
    open: vi.fn().mockImplementation(() => {
        const request = {
            onsuccess: null as ((event: any) => void) | null,
            onerror: null as ((event: any) => void) | null,
            result: {
                createObjectStore: vi.fn().mockReturnValue({
                    add: vi.fn(),
                    put: vi.fn(),
                    get: vi.fn(),
                    getAll: vi.fn(),
                    delete: vi.fn(),
                    clear: vi.fn(),
                }),
                objectStoreNames: {
                    contains: vi.fn(),
                },
                deleteObjectStore: vi.fn(),
            },
        };
        setTimeout(() => request.onsuccess?.({ target: request }), 0);
        return request;
    }),
};

vi.stubGlobal('indexedDB', mockIndexedDB);

// Mock IDBKeyVal for indexedDB.ts
vi.mock('idb-keyval', () => ({
    set: vi.fn().mockResolvedValue(undefined),
    get: vi.fn().mockResolvedValue(null),
    del: vi.fn().mockResolvedValue(undefined),
    clear: vi.fn().mockResolvedValue(undefined),
    keys: vi.fn().mockResolvedValue([]),
    entries: vi.fn().mockResolvedValue([]),
    getMany: vi.fn().mockResolvedValue([]),
    setMany: vi.fn().mockResolvedValue(undefined),
    delMany: vi.fn().mockResolvedValue(undefined),
    createStore: vi.fn(),
}));

// Polyfill File.arrayBuffer() for jsdom (required for hash computation tests)
if (!File.prototype.arrayBuffer) {
    File.prototype.arrayBuffer = async function() {
        const buffer = await new Response(this).arrayBuffer();
        return buffer;
    };
}

// Polyfill Blob.arrayBuffer() for jsdom
if (!Blob.prototype.arrayBuffer) {
    Blob.prototype.arrayBuffer = async function() {
        const buffer = await new Response(this).arrayBuffer();
        return buffer;
    };
}
