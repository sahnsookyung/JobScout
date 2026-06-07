import type { PolicyConfig } from '@/types/api';

export const POLICY_PRESET_VALUES = {
    strict: { min_fit: 70, top_k: 25, min_jd_required_coverage: 0.8 },
    balanced: { min_fit: 55, top_k: 50, min_jd_required_coverage: 0.6 },
    discovery: { min_fit: 40, top_k: 100, min_jd_required_coverage: null },
} as const satisfies Record<string, PolicyConfig>;

export const POLICY_PRESETS = {
    strict: { label: 'Strict', description: 'High bar, fewer results' },
    balanced: { label: 'Balanced', description: 'Default filtering' },
    discovery: { label: 'Discovery', description: 'Show more matches' },
} as const;

export const MATCH_STATUSES = [
    { value: 'active', label: 'Active' },
    { value: 'stale', label: 'Stale' },
    { value: 'all', label: 'All' },
] as const;

export const RANKING_MODE_OPTIONS = [
    { value: 'balanced', label: 'Balanced' },
    { value: 'preference_first', label: 'Preference First' },
    { value: 'fit_first', label: 'Fit First' },
] as const;
