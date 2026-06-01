/**
 * Tests for file utility functions
 * Covers: fileUtils.ts
 */

import { webcrypto } from 'node:crypto';

import { computeFileHash, validateFileSize } from '../fileUtils';
import { RESUME_MAX_SIZE, RESUME_MAX_SIZE_MB } from '@shared/constants';

// Polyfill crypto.subtle for JSDOM.
if (globalThis.crypto?.subtle === undefined) {
    Object.defineProperty(globalThis, 'crypto', { value: webcrypto });
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
        it('should compute a SHA-256 hash of a file without WebAssembly', async () => {
            const file = new File(['test content'], 'test.pdf', {
                type: 'application/pdf',
            });

            const hash = await computeFileHash(file);

            expect(hash).toBeDefined();
            expect(typeof hash).toBe('string');
            expect(hash).toHaveLength(64);
            expect(hash).toMatch(/^[0-9a-f]+$/);
            expect(hash).toBe('6ae8a75555209fd6c44157c0aed8016e763ff435a19cf186f76863140143ff72');
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
            expect(hash).toHaveLength(64);
            expect(hash).toMatch(/^[0-9a-f]+$/);
            expect(hash).toBe('e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855');
        });
    });
});
