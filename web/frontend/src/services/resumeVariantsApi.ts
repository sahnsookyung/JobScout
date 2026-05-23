import { apiClient } from './api';
import type {
    ResumeVariantDownloadFormat,
    ResumeVariantEnvelope,
    ResumeVariantListResponse,
} from '@/types/api';

export interface CreateResumeVariantRequest {
    template_key?: 'compact';
    tone?: 'concise' | 'direct';
    force?: boolean;
}

export interface ResumeVariantDownload {
    blob: Blob;
    filename: string;
}

function filenameFromDisposition(value: unknown, fallback: string): string {
    if (typeof value !== 'string') {
        return fallback;
    }

    const match = /filename\*=UTF-8''([^;]+)|filename="?([^";]+)"?/i.exec(value);
    const encoded = match?.[1] || match?.[2];
    if (!encoded) {
        return fallback;
    }

    try {
        return decodeURIComponent(encoded).replace(/[\\/]/g, '-');
    } catch {
        return encoded.replace(/[\\/]/g, '-');
    }
}

export const resumeVariantsApi = {
    createForMatch: (matchId: string, request: CreateResumeVariantRequest = {}) =>
        apiClient.post<ResumeVariantEnvelope>(`/matches/${matchId}/resume-variants`, request),

    listForMatch: (matchId: string, limit = 25) =>
        apiClient.get<ResumeVariantListResponse>('/resume-variants', {
            params: { match_id: matchId, limit },
        }),

    getVariant: (variantId: string) =>
        apiClient.get<ResumeVariantEnvelope>(`/resume-variants/${variantId}`),

    downloadVariant: async (
        variantId: string,
        format: ResumeVariantDownloadFormat,
    ): Promise<ResumeVariantDownload> => {
        const response = await apiClient.get<Blob>(`/resume-variants/${variantId}/download`, {
            params: { format },
            responseType: 'blob',
        });
        const extension = format === 'markdown' ? 'md' : format;
        return {
            blob: response.data,
            filename: filenameFromDisposition(
                response.headers?.['content-disposition'],
                `resume-variant-${variantId}.${extension}`,
            ),
        };
    },
};
