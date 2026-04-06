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
