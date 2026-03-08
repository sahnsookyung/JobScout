/**
 * File utility functions for hash computation and validation.
 */

import { RESUME_MAX_SIZE, RESUME_MAX_SIZE_MB } from '@shared/constants';
import xxhash from 'xxhash-wasm';

let xxhPromise: ReturnType<typeof xxhash> | null = null;

async function getXxh() {
    if (!xxhPromise) {
        xxhPromise = xxhash();
    }
    return xxhPromise;
}

/**
 * Compute XXH64 hash of a file.
 * @param file - The file to hash
 * @returns Hex string of the XXH64 hash (16 characters)
 */
export async function computeFileHash(file: File): Promise<string> {
    const xxh = await getXxh();
    const buffer = await file.arrayBuffer();
    const hasher = xxh.create64();
    hasher.update(new Uint8Array(buffer));
    return hasher.digest().toString(16).padStart(16, '0');
}

/**
 * Validate file size against the maximum allowed resume size.
 * @param file - The file to validate
 * @returns Object with valid flag and optional error message
 */
export function validateFileSize(file: File): { valid: boolean; error?: string } {
    if (file.size > RESUME_MAX_SIZE) {
        return {
            valid: false,
            error: `File size exceeds ${RESUME_MAX_SIZE_MB}MB limit. File is ${(file.size / (1024 * 1024)).toFixed(2)}MB.`,
        };
    }
    return { valid: true };
}
