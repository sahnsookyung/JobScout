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
