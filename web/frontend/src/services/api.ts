import axios, { type AxiosError } from 'axios';

import type { ApiErrorResponse, ApiFieldError } from '@/types/api';

const AUTH_STORAGE_KEY = 'jobscout_auth';

export const apiClient = axios.create({
    baseURL: import.meta.env.VITE_API_URL || '/api',
    timeout: 30000,
    withCredentials: true,
    headers: {
        'Content-Type': 'application/json',
    },
});

function readStoredToken(): string | null {
    if (typeof window === 'undefined') {
        return null;
    }

    try {
        const raw = window.localStorage.getItem(AUTH_STORAGE_KEY);
        if (!raw) {
            return null;
        }

        const parsed = JSON.parse(raw) as { token?: unknown };
        return typeof parsed.token === 'string' && parsed.token.length > 0
            ? parsed.token
            : null;
    } catch {
        return null;
    }
}

// Request interceptor for logging
apiClient.interceptors.request.use(
    (config) => {
        const token = readStoredToken();
        if (token) {
            config.headers = config.headers ?? {};
            config.headers.Authorization = `Bearer ${token}`;
        }
        console.log(`[API] ${config.method?.toUpperCase()} ${config.url}`);
        return config;
    },
    (error) => Promise.reject(error)
);

export class NormalizedApiError extends Error {
    status?: number;
    code: string;
    detail?: string;
    fields?: ApiFieldError[];
    originalError: AxiosError;

    constructor(args: {
        message: string;
        code: string;
        status?: number;
        detail?: string;
        fields?: ApiFieldError[];
        originalError: AxiosError;
    }) {
        super(args.message);
        this.name = 'NormalizedApiError';
        this.status = args.status;
        this.code = args.code;
        this.detail = args.detail;
        this.fields = args.fields;
        this.originalError = args.originalError;
    }
}

function isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null;
}

function toSafeString(value: unknown, fallback: string): string {
    return typeof value === 'string' ? value : fallback;
}

function extractValidationFields(detail: unknown): ApiFieldError[] | undefined {
    if (!Array.isArray(detail)) {
        return undefined;
    }
    const fields = detail
        .filter(isRecord)
        .map((entry) => ({
            path: Array.isArray(entry['loc']) ? entry['loc'].map(String) : [],
            code: toSafeString(entry['type'], 'validation_error'),
            message: toSafeString(entry['msg'], 'Invalid value'),
        }));
    return fields.length > 0 ? fields : undefined;
}

function normalizeApiError(error: AxiosError): NormalizedApiError {
    const status = error.response?.status;
    const data = error.response?.data;
    const fallbackCode = status ? `common.http.${status}` : 'common.network.request_failed';

    if (isRecord(data) && typeof data['code'] === 'string' && typeof data['message'] === 'string') {
        const apiError = data as unknown as ApiErrorResponse;
        return new NormalizedApiError({
            message: apiError.message,
            code: apiError.code,
            status,
            detail: apiError.detail,
            fields: apiError.fields,
            originalError: error,
        });
    }

    if (isRecord(data)) {
        const detail = data['detail'] ?? data['error'];
        if (typeof detail === 'string') {
            return new NormalizedApiError({
                message: detail,
                code: fallbackCode,
                status,
                originalError: error,
            });
        }

        const fields = extractValidationFields(detail);
        if (fields) {
            return new NormalizedApiError({
                message: fields.map((field) => field.message).join('; '),
                code: 'common.validation.invalid_request',
                status,
                fields,
                originalError: error,
            });
        }
    }

    return new NormalizedApiError({
        message: error.message,
        code: fallbackCode,
        status,
        originalError: error,
    });
}

apiClient.interceptors.response.use(
    (response) => response,
    (error: AxiosError) => {
        const normalised = normalizeApiError(error);
        console.error(`[API Error] ${normalised.status ?? 'network'}: ${normalised.message}`);
        return Promise.reject(normalised);
    }
);
