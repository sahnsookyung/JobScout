/**
 * File utility functions for hash computation and validation.
 */

import { RESUME_MAX_SIZE, RESUME_MAX_SIZE_MB } from '@shared/constants';

const HASH_ALGORITHM = 'SHA-256';

function arrayBufferToHex(buffer: ArrayBuffer): string {
    return Array.from(new Uint8Array(buffer))
        .map((byte) => byte.toString(16).padStart(2, '0'))
        .join('');
}

/**
 * Compute a CSP-safe hash of a file using the browser's native Web Crypto API.
 * @param file - The file to hash
 * @returns Hex string of the SHA-256 hash (64 characters)
 */
export async function computeFileHash(file: File): Promise<string> {
    const buffer = await file.arrayBuffer();
    const digest = await globalThis.crypto.subtle.digest(HASH_ALGORITHM, buffer);
    return arrayBufferToHex(digest);
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
