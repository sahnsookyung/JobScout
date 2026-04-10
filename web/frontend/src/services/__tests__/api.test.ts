/**
 * Tests for API client
 * Covers: api.ts
 */

import { getMockHandlers, createMockError, testErrorInterceptor } from './api.test.utils';

// Mock axios with interceptors support - must be before apiClient import
vi.mock('axios', () => {
    const mockRequestHandlers: any[] = [];
    const mockResponseHandlers: any[] = [];

    const create = vi.fn().mockImplementation((config) => {
        const instance = {
            defaults: config,
            interceptors: {
                request: {
                    use: vi.fn((fulfilled, rejected) => {
                        mockRequestHandlers.push({ fulfilled, rejected });
                        return mockRequestHandlers.length - 1;
                    }),
                    eject: vi.fn(),
                    get handlers() {
                        return mockRequestHandlers;
                    },
                },
                response: {
                    use: vi.fn((fulfilled, rejected) => {
                        mockResponseHandlers.push({ fulfilled, rejected });
                        return mockResponseHandlers.length - 1;
                    }),
                    eject: vi.fn(),
                    get handlers() {
                        return mockResponseHandlers;
                    },
                },
            },
        };
        return instance;
    });

    const defaultInstance = create({
        baseURL: '/api',
        timeout: 30000,
        headers: {
            'Content-Type': 'application/json',
        },
    });

    defaultInstance.create = create;

    Object.defineProperty(defaultInstance, '__mockRequestHandlers', {
        get: () => mockRequestHandlers,
    });
    Object.defineProperty(defaultInstance, '__mockResponseHandlers', {
        get: () => mockResponseHandlers,
    });

    return {
        default: defaultInstance,
        AxiosError: class AxiosError extends Error {},
        create,
    };
});

import { apiClient } from '../api';

describe('apiClient', () => {
    const originalWindow = globalThis.window;
    const originalLocalStorage = globalThis.localStorage;

    beforeEach(() => {
        vi.clearAllMocks();
        Object.defineProperty(globalThis, 'window', {
            value: originalWindow,
            configurable: true,
        });
        Object.defineProperty(globalThis, 'localStorage', {
            value: originalLocalStorage,
            configurable: true,
        });
    });

    describe('configuration', () => {
        it('should have correct baseURL', () => {
            expect(apiClient.defaults.baseURL).toBe('/api');
        });

        it('should have correct timeout', () => {
            expect(apiClient.defaults.timeout).toBe(30000);
        });

        it('should have correct Content-Type header', () => {
            expect(apiClient.defaults.headers?.['Content-Type']).toBe(
                'application/json'
            );
        });
    });

    describe('request interceptor', () => {
        it('should log request', () => {
            const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
            const mockConfig = { method: 'get', url: '/test', headers: {} };

            const { requestHandler } = getMockHandlers();
            requestHandler.fulfilled(mockConfig);

            expect(consoleSpy).toHaveBeenCalledWith('[API] GET /test');
            consoleSpy.mockRestore();
        });

        it('should attach bearer token when auth is stored', () => {
            Object.defineProperty(globalThis, 'localStorage', {
                value: {
                    getItem: vi.fn(() =>
                        JSON.stringify({
                            user: { email: 'user@example.com', name: 'User' },
                            token: 'test-session-token',
                        })
                    ),
                },
                configurable: true,
            });
            const mockConfig = {
                method: 'get',
                url: '/test',
                headers: {} as Record<string, string>,
            };

            const { requestHandler } = getMockHandlers();
            const result = requestHandler.fulfilled(mockConfig);

            expect(result.headers.Authorization).toBe('Bearer test-session-token');
        });

        it('should skip token lookup when window is unavailable', () => {
            const getItem = vi.fn();
            Object.defineProperty(globalThis, 'window', {
                value: undefined,
                configurable: true,
            });
            Object.defineProperty(globalThis, 'localStorage', {
                value: { getItem },
                configurable: true,
            });
            const mockConfig = {
                method: 'get',
                url: '/test',
                headers: {} as Record<string, string>,
            };

            const { requestHandler } = getMockHandlers();
            const result = requestHandler.fulfilled(mockConfig);

            expect(getItem).not.toHaveBeenCalled();
            expect(result.headers.Authorization).toBeUndefined();
        });

        it('should not attach authorization when no stored auth exists', () => {
            Object.defineProperty(globalThis, 'localStorage', {
                value: {
                    getItem: vi.fn(() => null),
                },
                configurable: true,
            });
            const mockConfig = {
                method: 'get',
                url: '/test',
                headers: {} as Record<string, string>,
            };

            const { requestHandler } = getMockHandlers();
            const result = requestHandler.fulfilled(mockConfig);

            expect(result.headers.Authorization).toBeUndefined();
        });

        it('should create headers when attaching a stored token to a bare config', () => {
            Object.defineProperty(globalThis, 'localStorage', {
                value: {
                    getItem: vi.fn(() =>
                        JSON.stringify({
                            token: 'test-session-token',
                        })
                    ),
                },
                configurable: true,
            });
            const mockConfig = {
                method: 'get',
                url: '/test',
            };

            const { requestHandler } = getMockHandlers();
            const result = requestHandler.fulfilled(mockConfig);

            expect(result.headers.Authorization).toBe('Bearer test-session-token');
        });

        it('should ignore stored auth entries without a string token', () => {
            Object.defineProperty(globalThis, 'localStorage', {
                value: {
                    getItem: vi.fn(() =>
                        JSON.stringify({
                            token: 42,
                        })
                    ),
                },
                configurable: true,
            });
            const mockConfig = {
                method: 'get',
                url: '/test',
                headers: {} as Record<string, string>,
            };

            const { requestHandler } = getMockHandlers();
            const result = requestHandler.fulfilled(mockConfig);

            expect(result.headers.Authorization).toBeUndefined();
        });

        it('should pass config through', () => {
            const mockConfig = { method: 'post', url: '/api', headers: {} };
            const { requestHandler } = getMockHandlers();
            const result = requestHandler.fulfilled(mockConfig);

            expect(result).toEqual(mockConfig);
        });

        it('should reject on error', async () => {
            const error = new Error('Request error');
            const { requestHandler } = getMockHandlers();

            await expect(requestHandler.rejected(error)).rejects.toBe(error);
        });
    });

    describe('response interceptor', () => {
        it('should pass through successful responses', () => {
            const mockResponse = {
                data: { success: true },
                status: 200,
                statusText: 'OK',
                headers: {},
                config: {},
            };

            const { responseHandler } = getMockHandlers();
            const result = responseHandler.fulfilled(mockResponse);

            expect(result).toBe(mockResponse);
        });

        it('should extract string detail from error', () => {
            const mockError = createMockError({
                message: 'Error',
                status: 400,
                data: { detail: 'Invalid input' },
            });
            return testErrorInterceptor(mockError, 'Invalid input', {
                code: 'common.http.400',
            });
        });

        it('should extract legacy error field from error response', () => {
            const mockError = createMockError({
                message: 'Error',
                status: 500,
                data: { error: 'Server error' },
            });
            return testErrorInterceptor(mockError, 'Server error', {
                code: 'common.http.500',
            });
        });

        it('should preserve canonical ApiError bodies', () => {
            const mockError = createMockError({
                message: 'Error',
                status: 409,
                data: {
                    code: 'pipeline.match.already_running',
                    message: 'Matching pipeline is already running.',
                    detail: 'Only one active matching task is allowed per user.',
                },
            });
            return testErrorInterceptor(mockError, 'Matching pipeline is already running.', {
                code: 'pipeline.match.already_running',
                detail: 'Only one active matching task is allowed per user.',
            });
        });

        it('should handle FastAPI validation errors', () => {
            const mockError = createMockError({
                message: 'Validation error',
                status: 422,
                data: {
                    detail: [
                        {
                            loc: ['body', 'email'],
                            msg: 'required',
                            type: 'missing',
                        },
                    ],
                },
            });
            return testErrorInterceptor(mockError, 'required', {
                code: 'common.validation.invalid_request',
                fieldsLength: 1,
            });
        });

        it('should fall back to safe defaults for malformed validation fields', () => {
            const mockError = createMockError({
                message: 'Validation error',
                status: 422,
                data: {
                    detail: [
                        {
                            loc: 'body.email',
                            msg: 7,
                            type: false,
                        },
                    ],
                },
            });
            return testErrorInterceptor(mockError, 'Invalid value', {
                code: 'common.validation.invalid_request',
                fieldsLength: 1,
            });
        });

        it('should fall back to the original error when validation detail has no record entries', () => {
            const mockError = createMockError({
                message: 'Validation error',
                status: 422,
                data: {
                    detail: [null, 'body.email'],
                },
            });
            return testErrorInterceptor(mockError, 'Validation error', {
                code: 'common.http.422',
            });
        });

        it('should use original message when no detail', () => {
            const mockError = createMockError({
                message: 'Network error',
                status: 503,
                data: {},
            });
            return testErrorInterceptor(mockError, 'Network error', {
                code: 'common.http.503',
            });
        });

        it('should handle missing response', () => {
            const mockError = createMockError({
                message: 'Network Error',
            });
            return testErrorInterceptor(mockError, 'Network Error', {
                code: 'common.network.request_failed',
            });
        });
    });
});
