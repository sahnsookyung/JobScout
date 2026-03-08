/**
 * Tests for API client
 * Covers: api.ts
 */

import axios, { AxiosError } from 'axios';
import { apiClient } from '../api';

vi.mock('axios');

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

            const interceptor = apiClient.interceptors.request.handlers[0];
            interceptor.fulfilled(mockConfig);

            expect(consoleSpy).toHaveBeenCalledWith('[API] GET /test');
            consoleSpy.mockRestore();
        });

        it('should pass config through', () => {
            const mockConfig = { method: 'post', url: '/api', headers: {} };
            const interceptor = apiClient.interceptors.request.handlers[0];
            const result = interceptor.fulfilled(mockConfig);

            expect(result).toEqual(mockConfig);
        });

        it('should reject on error', async () => {
            const error = new Error('Request error');
            const interceptor = apiClient.interceptors.request.handlers[0];

            await expect(interceptor.rejected(error)).rejects.toBe(error);
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

            const interceptor = apiClient.interceptors.response.handlers[0];
            const result = interceptor.fulfilled(mockResponse);

            expect(result).toBe(mockResponse);
        });

        it('should extract string detail from error', () => {
            const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
            const mockError = {
                message: 'Error',
                response: { status: 400, data: { detail: 'Invalid input' } },
                config: {},
            } as unknown as AxiosError;

            const interceptor = apiClient.interceptors.response.handlers[0];

            try {
                interceptor.rejected(mockError);
            } catch (error) {
                expect((error as Error).message).toBe('Invalid input');
            }

            consoleSpy.mockRestore();
        });

        it('should extract error field from error response', () => {
            const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
            const mockError = {
                message: 'Error',
                response: { status: 500, data: { error: 'Server error' } },
                config: {},
            } as unknown as AxiosError;

            const interceptor = apiClient.interceptors.response.handlers[0];

            try {
                interceptor.rejected(mockError);
            } catch (error) {
                expect((error as Error).message).toBe('Server error');
            }

            consoleSpy.mockRestore();
        });

        it('should handle FastAPI validation errors', () => {
            const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
            const mockError = {
                message: 'Validation error',
                response: {
                    status: 422,
                    data: {
                        detail: [{ loc: ['body', 'email'], msg: 'required', type: 'missing' }],
                    },
                },
                config: {},
            } as unknown as AxiosError;

            const interceptor = apiClient.interceptors.response.handlers[0];

            try {
                interceptor.rejected(mockError);
            } catch (error) {
                expect((error as Error).message).toBe('required');
            }

            consoleSpy.mockRestore();
        });

        it('should use original message when no detail', () => {
            const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
            const mockError = {
                message: 'Network error',
                response: { status: 503, data: {} },
                config: {},
            } as unknown as AxiosError;

            const interceptor = apiClient.interceptors.response.handlers[0];

            try {
                interceptor.rejected(mockError);
            } catch (error) {
                expect((error as Error).message).toBe('Network error');
            }

            consoleSpy.mockRestore();
        });

        it('should handle missing response', () => {
            const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
            const mockError = {
                message: 'Network Error',
                config: {},
            } as unknown as AxiosError;

            const interceptor = apiClient.interceptors.response.handlers[0];

            try {
                interceptor.rejected(mockError);
            } catch (error) {
                expect((error as Error).message).toBe('Network Error');
            }

            consoleSpy.mockRestore();
        });

        it('should attach status to normalized error', () => {
            const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
            const mockError = {
                message: 'Error',
                response: { status: 404, data: { detail: 'Not found' } },
                config: {},
            } as unknown as AxiosError;

            const interceptor = apiClient.interceptors.response.handlers[0];

            try {
                interceptor.rejected(mockError);
            } catch (error) {
                expect((error as Error & { status?: number }).status).toBe(404);
            }

            consoleSpy.mockRestore();
        });

        it('should attach original error', () => {
            const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
            const mockError = {
                message: 'Error',
                response: { status: 401, data: { detail: 'Unauthorized' } },
                config: {},
            } as unknown as AxiosError;

            const interceptor = apiClient.interceptors.response.handlers[0];

            try {
                interceptor.rejected(mockError);
            } catch (error) {
                expect(
                    (error as Error & { originalError?: AxiosError }).originalError
                ).toBe(mockError);
            }

            consoleSpy.mockRestore();
        });
    });
});
