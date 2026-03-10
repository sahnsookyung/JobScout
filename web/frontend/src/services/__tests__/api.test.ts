/**
 * Tests for API client
 * Covers: api.ts
 */

import axios, { AxiosError } from 'axios';
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
    beforeEach(() => {
        vi.clearAllMocks();
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
            testErrorInterceptor(mockError, 'Invalid input');
        });

        it('should extract error field from error response', () => {
            const mockError = createMockError({
                message: 'Error',
                status: 500,
                data: { error: 'Server error' },
            });
            testErrorInterceptor(mockError, 'Server error');
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
            testErrorInterceptor(mockError, 'required');
        });

        it('should use original message when no detail', () => {
            const mockError = createMockError({
                message: 'Network error',
                status: 503,
                data: {},
            });
            testErrorInterceptor(mockError, 'Network error');
        });

        it('should handle missing response', () => {
            const mockError = createMockError({
                message: 'Network Error',
            });
            testErrorInterceptor(mockError, 'Network Error');
        });
    });
});
