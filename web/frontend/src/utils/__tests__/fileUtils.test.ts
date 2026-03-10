/**
 * Tests for file utility functions
 * Covers: fileUtils.ts
 */

import { computeFileHash, validateFileSize } from '../fileUtils';
import { RESUME_MAX_SIZE, RESUME_MAX_SIZE_MB } from '@shared/constants';

// Polyfill crypto.subtle for JSDOM environment (needed for xxhash)
if (typeof globalThis.crypto?.subtle === 'undefined') {
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    Object.defineProperty(globalThis, 'crypto', { value: require('node:crypto').webcrypto });
}

describe('fileUtils', () => {
    describe('validateFileSize', () => {
        it('should return valid for file under size limit', () => {
            const smallFile = new File(['test content'], 'test.pdf', {
                type: 'application/pdf',
            });

            const result = validateFileSize(smallFile);

            expect(result.valid).toBe(true);
            expect(result.error).toBeUndefined();
        });

        it('should return invalid for file exceeding size limit', () => {
            // Create a file larger than 2MB
            const largeContent = 'x'.repeat(RESUME_MAX_SIZE + 1);
            const largeFile = new File([largeContent], 'large.pdf', {
                type: 'application/pdf',
            });

            const result = validateFileSize(largeFile);

            expect(result.valid).toBe(false);
            expect(result.error).toContain(`${RESUME_MAX_SIZE_MB}MB limit`);
            expect(result.error).toContain('exceeds');
        });

        it('should return valid for file exactly at size limit', () => {
            const exactFile = new File(['x'.repeat(RESUME_MAX_SIZE)], 'exact.pdf', {
                type: 'application/pdf',
            });

            const result = validateFileSize(exactFile);

            expect(result.valid).toBe(true);
            expect(result.error).toBeUndefined();
        });
    });

    describe('computeFileHash', () => {
        it('should compute XXH64 hash of a file', async () => {
            const file = new File(['test content'], 'test.pdf', {
                type: 'application/pdf',
            });

            const hash = await computeFileHash(file);

            expect(hash).toBeDefined();
            expect(typeof hash).toBe('string');
            expect(hash).toHaveLength(16);
            expect(hash).toMatch(/^[0-9a-f]+$/);
        });

        it('should produce deterministic hash for same content', async () => {
            const content = 'deterministic test content';
            const file1 = new File([content], 'test1.pdf', {
                type: 'application/pdf',
            });
            const file2 = new File([content], 'test2.pdf', {
                type: 'application/pdf',
            });

            const hash1 = await computeFileHash(file1);
            const hash2 = await computeFileHash(file2);

            expect(hash1).toBe(hash2);
        });

        it('should handle empty file', async () => {
            const emptyFile = new File([], 'empty.pdf', {
                type: 'application/pdf',
            });

            const hash = await computeFileHash(emptyFile);

            expect(hash).toBeDefined();
            expect(hash).toHaveLength(16);
            expect(hash).toMatch(/^[0-9a-f]+$/);
        });
    });
});
