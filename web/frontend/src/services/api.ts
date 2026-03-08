import axios, { type AxiosError } from 'axios';

export const apiClient = axios.create({
    baseURL: import.meta.env.VITE_API_URL || '/api',
    timeout: 30000,
    headers: {
        'Content-Type': 'application/json',
    },
});

// Request interceptor for logging
apiClient.interceptors.request.use(
    (config) => {
        console.log(`[API] ${config.method?.toUpperCase()} ${config.url}`);
        return config;
    },
    (error) => Promise.reject(error)
);

/** Extract a human-readable message from a FastAPI error response. */
function extractErrorMessage(error: AxiosError): string {
    const data = error.response?.data as Record<string, unknown> | undefined;
    if (!data) return error.message;
    const detail = data['detail'] ?? data['error'];
    if (typeof detail === 'string') return detail;
    if (Array.isArray(detail)) {
        // FastAPI 422 validation errors: [{ loc, msg, type }]
        return detail
            .map((d: unknown) => (d && typeof d === 'object' && 'msg' in d ? String((d as Record<string, unknown>)['msg']) : String(d)))
            .join('; ');
    }
    return error.message;
}

// Response interceptor for error handling
apiClient.interceptors.response.use(
    (response) => response,
    (error: AxiosError) => {
        const message = extractErrorMessage(error);
        const status = error.response?.status;
        console.error(`[API Error] ${status ?? 'network'}: ${message}`);
        // Attach normalised message so callers can do error.message without parsing
        const normalised = new Error(message) as Error & { status?: number; originalError: AxiosError };
        normalised.status = status;
        normalised.originalError = error;
        return Promise.reject(normalised);
    }
);