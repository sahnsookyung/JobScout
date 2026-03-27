/**
 * Test utilities for api.test.ts
 * Provides helpers to reduce duplication in API client tests
 */

import axios, { AxiosError, AxiosResponse } from 'axios';

/**
 * Get mock axios handlers for testing interceptors
 */
export function getMockHandlers() {
    const mockAxios = axios as any;
    return {
        requestHandler: mockAxios.__mockRequestHandlers[0],
        responseHandler: mockAxios.__mockResponseHandlers[0],
    };
}

/**
 * Create a mock AxiosError for testing
 */
export function createMockError(options: {
    message: string;
    status?: number;
    data?: Record<string, unknown>;
}): AxiosError {
    return {
        message: options.message,
        response: options.status
            ? {
                  status: options.status,
                  data: options.data ?? {},
                  statusText: 'Error',
                  headers: {},
                  config: {},
              } as AxiosResponse
            : undefined,
        config: {},
        isAxiosError: true,
        name: 'AxiosError',
    } as unknown as AxiosError;
}

/**
 * Test error interceptor with automatic console spy management
 */
export function testErrorInterceptor(
    mockError: AxiosError,
    expectedMessage: string,
    expected?: {
        code?: string;
        detail?: string;
        fieldsLength?: number;
    }
): Promise<void> {
    const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    try {
        const { responseHandler } = getMockHandlers();
        const result = responseHandler.rejected(mockError);
        if (result && typeof result.then === 'function') {
            return result.catch((error: Error & { code?: string; detail?: string; fields?: unknown[] }) => {
                expect(error.message).toBe(expectedMessage);
                if (expected?.code !== undefined) {
                    expect(error.code).toBe(expected.code);
                }
                if (expected?.detail !== undefined) {
                    expect(error.detail).toBe(expected.detail);
                }
                if (expected?.fieldsLength !== undefined) {
                    expect(error.fields).toHaveLength(expected.fieldsLength);
                }
            });
        }
        return Promise.resolve();
    } finally {
        consoleSpy.mockRestore();
    }
}

/**
 * Create a mock axios instance for configuration tests
 */
export function createMockAxiosInstance(config: {
    baseURL: string;
    timeout: number;
    headers: Record<string, string>;
}) {
    return {
        defaults: config,
        interceptors: {
            request: {
                use: vi.fn(),
                eject: vi.fn(),
            },
            response: {
                use: vi.fn(),
                eject: vi.fn(),
            },
        },
        create: vi.fn(),
    };
}
