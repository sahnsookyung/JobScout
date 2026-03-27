import { describe, it, expect } from 'vitest';
import {
    formatScore,
    formatPercentage,
    formatDate,
    formatSalary,
    getScoreColor,
    getScoreBadgeColor,
} from '../formatters';

describe('formatters', () => {
    describe('formatScore', () => {
        it('should format valid score with one decimal', () => {
            expect(formatScore(85.5)).toBe('85.5%');
            expect(formatScore(100)).toBe('100.0%');
            expect(formatScore(0)).toBe('0.0%');
        });

        it('should return N/A for null', () => {
            expect(formatScore(null)).toBe('N/A');
        });

        it('should return N/A for undefined', () => {
            expect(formatScore(undefined)).toBe('N/A');
        });
    });

    describe('formatPercentage', () => {
        it('should format decimal as percentage', () => {
            expect(formatPercentage(0.85)).toBe('85%');
            expect(formatPercentage(1)).toBe('100%');
            expect(formatPercentage(0)).toBe('0%');
        });

        it('should return N/A for null', () => {
            expect(formatPercentage(null)).toBe('N/A');
        });

        it('should return N/A for undefined', () => {
            expect(formatPercentage(undefined)).toBe('N/A');
        });
    });

    describe('formatDate', () => {
        it('should format valid date string', () => {
            const result = formatDate('2024-03-15T10:30:00Z');
            expect(result).toMatch(/\w+ \d{1,2}, \d{4},? \d{1,2}:\d{2}/);
        });

        it('should return N/A for null', () => {
            expect(formatDate(null)).toBe('N/A');
        });

        it('should return N/A for undefined', () => {
            expect(formatDate(undefined)).toBe('N/A');
        });

        it('should return N/A for empty string', () => {
            expect(formatDate('')).toBe('N/A');
        });
    });

    describe('formatSalary', () => {
        it('should format salary range with both min and max', () => {
            const result = formatSalary(50000, 80000, 'USD');
            expect(result).toContain('$50,000');
            expect(result).toContain('$80,000');
        });

        it('should format salary with only min', () => {
            const result = formatSalary(50000, null, 'USD');
            expect(result).toContain('From $50,000');
        });

        it('should format salary with only max', () => {
            const result = formatSalary(null, 80000, 'USD');
            expect(result).toContain('Up to $80,000');
        });

        it('should return Not specified for null values', () => {
            expect(formatSalary(null, null, 'USD')).toBe('Not specified');
        });

        it('should use USD as default currency', () => {
            const result = formatSalary(50000, null, null);
            expect(result).toContain('$50,000');
        });

        it('should fall back to USD for empty currency strings', () => {
            const result = formatSalary(50000, null, '');
            expect(result).toContain('$50,000');
        });

        it('should format with different currency', () => {
            const result = formatSalary(50000, null, 'EUR');
            expect(result).toContain('€50,000');
        });
    });

    describe('getScoreColor', () => {
        it('should return green for score >= 80', () => {
            expect(getScoreColor(80)).toBe('text-green-600');
            expect(getScoreColor(100)).toBe('text-green-600');
        });

        it('should return blue for score >= 60 and < 80', () => {
            expect(getScoreColor(60)).toBe('text-blue-600');
            expect(getScoreColor(79)).toBe('text-blue-600');
        });

        it('should return yellow for score >= 40 and < 60', () => {
            expect(getScoreColor(40)).toBe('text-yellow-600');
            expect(getScoreColor(59)).toBe('text-yellow-600');
        });

        it('should return red for score < 40', () => {
            expect(getScoreColor(0)).toBe('text-red-600');
            expect(getScoreColor(39)).toBe('text-red-600');
        });
    });

    describe('getScoreBadgeColor', () => {
        it('should return green badge for score >= 80', () => {
            expect(getScoreBadgeColor(80)).toBe('bg-green-100 text-green-800');
            expect(getScoreBadgeColor(100)).toBe('bg-green-100 text-green-800');
        });

        it('should return blue badge for score >= 60 and < 80', () => {
            expect(getScoreBadgeColor(60)).toBe('bg-blue-100 text-blue-800');
            expect(getScoreBadgeColor(79)).toBe('bg-blue-100 text-blue-800');
        });

        it('should return yellow badge for score >= 40 and < 60', () => {
            expect(getScoreBadgeColor(40)).toBe('bg-yellow-100 text-yellow-800');
            expect(getScoreBadgeColor(59)).toBe('bg-yellow-100 text-yellow-800');
        });

        it('should return red badge for score < 40', () => {
            expect(getScoreBadgeColor(0)).toBe('bg-red-100 text-red-800');
            expect(getScoreBadgeColor(39)).toBe('bg-red-100 text-red-800');
        });
    });
});
