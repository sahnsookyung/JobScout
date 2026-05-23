import axios, { type AxiosError } from 'axios';

import { withAppBasePath } from '@/config/publicPath';
import type { ApiErrorResponse, ApiFieldError } from '@/types/api';

const TENANT_STORAGE_KEY = 'jobscout_tenant_id';
const AUTH_STORAGE_KEY = 'jobscout_auth';
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const CSRF_COOKIE_NAME = '__Host-jobscout_csrf';
const MUTATION_METHODS = new Set(['post', 'put', 'patch', 'delete']);

let verifiedTenantId: string | null = null;

export const apiClient = axios.create({
    baseURL: import.meta.env.VITE_API_URL || withAppBasePath('/api'),
    timeout: 30000,
    withCredentials: true,
    headers: {
        'Content-Type': 'application/json',
    },
});

function safeLocalStorageGet(key: string): string | null {
    if (globalThis.window === undefined) {
        return null;
    }

    try {
        return globalThis.localStorage.getItem(key);
    } catch {
        return null;
    }
}

function safeLocalStorageSet(key: string, value: string): void {
    if (globalThis.window === undefined) {
        return;
    }

    try {
        globalThis.localStorage.setItem(key, value);
    } catch {
        // Storage may be unavailable in privacy-restricted browser contexts.
    }
}

function safeLocalStorageRemove(key: string): void {
    if (globalThis.window === undefined) {
        return;
    }

    try {
        globalThis.localStorage.removeItem(key);
    } catch {
        // Storage may be unavailable in privacy-restricted browser contexts.
    }
}

function normalizedTenantId(value: string | null): string | null {
    const candidate = value?.trim();
    return candidate && UUID_PATTERN.test(candidate) ? candidate : null;
}

export function readRequestedTenantId(): string | null {
    if (globalThis.window === undefined) {
        return null;
    }

    const search = globalThis.window?.location?.search ?? globalThis.location?.search ?? '';
    return normalizedTenantId(
        new URLSearchParams(search).get('tenant_id')
    ) ?? normalizedTenantId(safeLocalStorageGet(TENANT_STORAGE_KEY));
}

export function setVerifiedTenantId(tenantId: string | null): void {
    verifiedTenantId = normalizedTenantId(tenantId);
    if (verifiedTenantId) {
        safeLocalStorageSet(TENANT_STORAGE_KEY, verifiedTenantId);
    } else {
        safeLocalStorageRemove(TENANT_STORAGE_KEY);
    }
}

function readCsrfCookie(): string | null {
    if (globalThis.document === undefined) {
        return null;
    }
    const prefix = `${CSRF_COOKIE_NAME}=`;
    const cookie = globalThis.document.cookie
        .split(';')
        .map((part) => part.trim())
        .find((part) => part.startsWith(prefix));
    return cookie ? decodeURIComponent(cookie.slice(prefix.length)) : null;
}

function isProductionLike(): boolean {
    return import.meta.env.PROD;
}

apiClient.interceptors.request.use(
    (config) => {
        const storedToken = readStoredToken();
        if (storedToken) {
            config.headers = config.headers ?? {};
            config.headers.Authorization = `Bearer ${storedToken}`;
        }
        const tenantId = verifiedTenantId ?? (!isProductionLike() ? readRequestedTenantId() : null);
        if (tenantId) {
            if (!verifiedTenantId && !isProductionLike()) {
                safeLocalStorageSet(TENANT_STORAGE_KEY, tenantId);
            }
            config.headers = config.headers ?? {};
            config.headers['X-Tenant-Id'] = tenantId;
        }
        const method = config.method?.toLowerCase() ?? 'get';
        if (MUTATION_METHODS.has(method)) {
            const csrfToken = readCsrfCookie();
            if (csrfToken) {
                config.headers = config.headers ?? {};
                config.headers['X-CSRF-Token'] = csrfToken;
            }
        }
        if (!isProductionLike()) {
            console.log(`[API] ${config.method?.toUpperCase()} ${config.url}`);
        }
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
        if (!isProductionLike()) {
            console.error(`[API Error] ${normalised.status ?? 'network'}: ${normalised.message}`);
        }
        return Promise.reject(normalised);
    }
);
function readStoredToken(): string | null {
    if (isProductionLike() || globalThis.window === undefined) {
        return null;
    }

    try {
        const raw = globalThis.localStorage.getItem(AUTH_STORAGE_KEY);
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
