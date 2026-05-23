import { vi } from 'vitest';

import { apiClient } from '../api';
import { resumeVariantsApi } from '../resumeVariantsApi';

vi.mock('../api', () => ({
    apiClient: {
        get: vi.fn(),
        post: vi.fn(),
    },
}));

const mockGet = vi.mocked(apiClient.get);
const mockPost = vi.mocked(apiClient.post);

describe('resumeVariantsApi', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('creates a variant for a match', async () => {
        const expected = { data: { success: true, variant: { id: 'variant-1' } } };
        mockPost.mockResolvedValueOnce(expected);

        const result = await resumeVariantsApi.createForMatch('match-1', {
            template_key: 'compact',
            tone: 'direct',
            force: true,
        });

        expect(mockPost).toHaveBeenCalledWith('/matches/match-1/resume-variants', {
            template_key: 'compact',
            tone: 'direct',
            force: true,
        });
        expect(result).toEqual(expected);
    });

    it('lists and gets generated variants', async () => {
        mockGet.mockResolvedValue({ data: { success: true } });

        await resumeVariantsApi.listForMatch('match-1', 7);
        await resumeVariantsApi.getVariant('variant-1');

        expect(mockGet).toHaveBeenCalledWith('/resume-variants', {
            params: { match_id: 'match-1', limit: 7 },
        });
        expect(mockGet).toHaveBeenCalledWith('/resume-variants/variant-1');
    });

    it('downloads a variant and trusts sanitized content-disposition filenames', async () => {
        const blob = new Blob(['resume'], { type: 'text/markdown' });
        mockGet.mockResolvedValueOnce({
            data: blob,
            headers: {
                'content-disposition': "attachment; filename*=UTF-8''resume%2Fvariant.md",
            },
        });

        const result = await resumeVariantsApi.downloadVariant('variant-1', 'markdown');

        expect(mockGet).toHaveBeenCalledWith('/resume-variants/variant-1/download', {
            params: { format: 'markdown' },
            responseType: 'blob',
        });
        expect(result.blob).toBe(blob);
        expect(result.filename).toBe('resume-variant.md');
    });

    it('falls back to safe generated download names', async () => {
        const blob = new Blob(['docx']);
        mockGet.mockResolvedValueOnce({ data: blob, headers: {} });

        const result = await resumeVariantsApi.downloadVariant('variant-2', 'docx');

        expect(result.filename).toBe('resume-variant-variant-2.docx');
    });

    it('falls back when content-disposition has no filename token', async () => {
        mockGet.mockResolvedValueOnce({
            data: new Blob(['resume']),
            headers: { 'content-disposition': 'attachment' },
        });

        const result = await resumeVariantsApi.downloadVariant('variant-4', 'markdown');

        expect(result.filename).toBe('resume-variant-variant-4.md');
    });

    it('keeps undecodable but sanitized content-disposition names', async () => {
        mockGet.mockResolvedValueOnce({
            data: new Blob(['html']),
            headers: {
                'content-disposition': 'attachment; filename="resume%ZZ/name.html"',
            },
        });

        const result = await resumeVariantsApi.downloadVariant('variant-3', 'html');

        expect(result.filename).toBe('resume%ZZ-name.html');
    });
});
