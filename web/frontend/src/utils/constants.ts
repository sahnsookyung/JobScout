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

export const SORT_OPTIONS = [
    { value: 'overall', label: 'Overall Score' },
    { value: 'fit', label: 'Fit Score' },
    { value: 'want', label: 'Want Score' },
] as const;
