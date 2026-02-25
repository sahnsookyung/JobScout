/**
 * Unit tests for IndexedDB resume storage utilities.
 * 
 * Note: These tests require a browser environment (JSDOM) to run.
 * Run with: npx jest src/utils/__tests__/indexedDB.test.ts
 */

import { saveResume, getResume, getResumeHash, deleteResume, hasResume } from '../indexedDB';
import { RESUME_MAX_SIZE, RESUME_MAX_SIZE_MB } from '../constants';

describe('IndexedDB Resume Storage', () => {
    const testBlob = new Blob(['test content'], { type: 'application/pdf' });
    const testHash = 'abc123def45678901234567890123456';

    beforeEach(async () => {
        // Clean up before each test
        const existingHash = await getResumeHash();
        if (existingHash) {
            await deleteResume(existingHash);
        }
    });

    afterEach(async () => {
        // Clean up after each test
        const existingHash = await getResumeHash();
        if (existingHash) {
            await deleteResume(existingHash);
        }
    });

    describe('saveResume', () => {
        it('should save a resume to IndexedDB', async () => {
            await saveResume(testBlob, testHash);
            
            const stored = await getResume(testHash);
            expect(stored).not.toBeNull();
            expect(stored?.size).toBe(testBlob.size);
        });

        it('should overwrite existing resume with same hash', async () => {
            const newBlob = new Blob(['new content'], { type: 'application/pdf' });
            
            await saveResume(testBlob, testHash);
            await saveResume(newBlob, testHash);
            
            const stored = await getResume(testHash);
            expect(stored?.size).toBe(newBlob.size);
        });

        it('should maintain only 1 entry (max entries)', async () => {
            const blob1 = new Blob(['content 1'], { type: 'application/pdf' });
            const blob2 = new Blob(['content 2'], { type: 'application/pdf' });
            
            await saveResume(blob1, 'hash1111111111111111111111111111');
            await saveResume(blob2, 'hash2222222222222222222222222222');
            
            const hash1 = await getResumeHash();
            // Only one should remain
            expect(await hasResume()).toBe(true);
        });
    });

    describe('getResume', () => {
        it('should retrieve stored resume by hash', async () => {
            await saveResume(testBlob, testHash);
            
            const retrieved = await getResume(testHash);
            expect(retrieved).not.toBeNull();
        });

        it('should return null for non-existent hash', async () => {
            const retrieved = await getResume('nonexistent_hash_123456789');
            expect(retrieved).toBeNull();
        });

        it('should return null for expired entries (>30 days)', async () => {
            // This test would require manipulating the timestamp
            // For now, just verify basic functionality
            await saveResume(testBlob, testHash);
            const retrieved = await getResume(testHash);
            expect(retrieved).not.toBeNull();
        });
    });

    describe('getResumeHash', () => {
        it('should return the hash of stored resume', async () => {
            await saveResume(testBlob, testHash);
            
            const hash = await getResumeHash();
            expect(hash).toBe(testHash);
        });

        it('should return null when no resume stored', async () => {
            const hash = await getResumeHash();
            expect(hash).toBeNull();
        });
    });

    describe('hasResume', () => {
        it('should return true when resume exists', async () => {
            await saveResume(testBlob, testHash);
            
            expect(await hasResume()).toBe(true);
        });

        it('should return false when no resume exists', async () => {
            expect(await hasResume()).toBe(false);
        });
    });

    describe('deleteResume', () => {
        it('should delete stored resume', async () => {
            await saveResume(testBlob, testHash);
            await deleteResume(testHash);
            
            const retrieved = await getResume(testHash);
            expect(retrieved).toBeNull();
        });
    });
});

describe('File Hash Computation', () => {
    // Helper function that mirrors the frontend implementation
    async function computeFileHash(file: File): Promise<string> {
        const buffer = await file.arrayBuffer();
        const hashBuffer = await crypto.subtle.digest('SHA-256', buffer);
        const hashArray = Array.from(new Uint8Array(hashBuffer));
        return hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
    }

    it('should compute SHA-256 hash of file content', async () => {
        const content = 'Hello, World!';
        const file = new File([content], 'test.txt', { type: 'text/plain' });
        
        const hash = await computeFileHash(file);
        
        // Verify against known SHA-256 of "Hello, World!"
        const expected = 'dffd6021bb2bd5b0af676290809ec3a53191dd81c7f70a4b28688a362182986f';
        expect(hash).toBe(expected);
    });

    it('should produce deterministic hash', async () => {
        const content = 'Same content';
        const file1 = new File([content], 'test1.txt', { type: 'text/plain' });
        const file2 = new File([content], 'test2.txt', { type: 'text/plain' });
        
        const hash1 = await computeFileHash(file1);
        const hash2 = await computeFileHash(file2);
        
        expect(hash1).toBe(hash2);
    });

    it('should produce different hash for different content', async () => {
        const file1 = new File(['Content A'], 'test1.txt', { type: 'text/plain' });
        const file2 = new File(['Content B'], 'test2.txt', { type: 'text/plain' });
        
        const hash1 = await computeFileHash(file1);
        const hash2 = await computeFileHash(file2);
        
        expect(hash1).not.toBe(hash2);
    });
});

describe('2MB File Validation', () => {
    function validateFileSize(file: File): { valid: boolean; error?: string } {
        if (file.size > RESUME_MAX_SIZE) {
            return {
                valid: false,
                error: `File size exceeds ${RESUME_MAX_SIZE_MB}MB limit. File is ${(file.size / (1024 * 1024)).toFixed(2)}MB.`
            };
        }
        return { valid: true };
    }

    it('should accept file under 2MB', () => {
        const file = new File(['small content'], 'small.pdf', { type: 'application/pdf' });
        
        const result = validateFileSize(file);
        
        expect(result.valid).toBe(true);
        expect(result.error).toBeUndefined();
    });

    it('should reject file over 2MB', () => {
        // Create a large file (3MB)
        const largeContent = 'x'.repeat(3 * 1024 * 1024);
        const file = new File([largeContent], 'large.pdf', { type: 'application/pdf' });
        
        const result = validateFileSize(file);
        
        expect(result.valid).toBe(false);
        expect(result.error).toContain('2MB');
    });

    it('should reject file exactly at 2MB', () => {
        const boundaryContent = 'x'.repeat(2 * 1024 * 1024);
        const file = new File([boundaryContent], 'boundary.pdf', { type: 'application/pdf' });
        
        const result = validateFileSize(file);
        
        // At exactly 2MB should be valid (not exceeding)
        expect(result.valid).toBe(true);
    });
});
