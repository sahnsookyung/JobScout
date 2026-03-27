/**
 * Tests for frontend constants
 * Covers: web/frontend/src/utils/constants.ts
 */

import { describe, it, expect } from 'vitest';
import { POLICY_PRESETS, MATCH_STATUSES, SORT_OPTIONS } from '../constants';

describe('POLICY_PRESETS', () => {
    it('has strict, balanced, and discovery keys', () => {
        expect(POLICY_PRESETS).toHaveProperty('strict');
        expect(POLICY_PRESETS).toHaveProperty('balanced');
        expect(POLICY_PRESETS).toHaveProperty('discovery');
    });

    it('strict preset has label and description', () => {
        expect(POLICY_PRESETS.strict.label).toBe('Strict');
        expect(POLICY_PRESETS.strict.description).toBeTruthy();
    });

    it('balanced preset has label and description', () => {
        expect(POLICY_PRESETS.balanced.label).toBe('Balanced');
        expect(POLICY_PRESETS.balanced.description).toBeTruthy();
    });

    it('discovery preset has label and description', () => {
        expect(POLICY_PRESETS.discovery.label).toBe('Discovery');
        expect(POLICY_PRESETS.discovery.description).toBeTruthy();
    });
});

describe('MATCH_STATUSES', () => {
    it('has 3 status options', () => {
        expect(MATCH_STATUSES).toHaveLength(3);
    });

    it('includes active, stale, and all statuses', () => {
        const values = MATCH_STATUSES.map(s => s.value);
        expect(values).toContain('active');
        expect(values).toContain('stale');
        expect(values).toContain('all');
    });

    it('each status has a label', () => {
        MATCH_STATUSES.forEach(status => {
            expect(status.label).toBeTruthy();
        });
    });
});

describe('SORT_OPTIONS', () => {
    it('has 3 sort options', () => {
        expect(SORT_OPTIONS).toHaveLength(3);
    });

    it('includes overall, fit, and want sort options', () => {
        const values = SORT_OPTIONS.map(s => s.value);
        expect(values).toContain('overall');
        expect(values).toContain('fit');
        expect(values).toContain('want');
    });

    it('each option has a label', () => {
        SORT_OPTIONS.forEach(option => {
            expect(option.label).toBeTruthy();
        });
    });

    it('overall option is labeled "Overall Score"', () => {
        const overall = SORT_OPTIONS.find(s => s.value === 'overall');
        expect(overall?.label).toBe('Overall Score');
    });
});
