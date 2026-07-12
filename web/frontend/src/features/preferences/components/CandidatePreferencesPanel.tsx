import { useEffect, useRef, useState } from 'react';
import { toast } from 'sonner';

import { Button } from '@/components/ui/Button';
import { useCandidatePreferences } from '@/hooks/useCandidatePreferences';
import type {
    CandidatePreferences,
    CandidatePreferencesUpdateRequest,
} from '@/types/api';

type DraftPreferences = CandidatePreferencesUpdateRequest;

function toDraft(preferences: CandidatePreferences): DraftPreferences {
    const bounds = preferences.preference_rerank_top_n_bounds ?? { min: 1, max: 100, default: 25 };
    return {
        remote_mode: preferences.remote_mode,
        target_locations: preferences.target_locations,
        visa_sponsorship_required: preferences.visa_sponsorship_required,
        salary_min: preferences.salary_min,
        employment_types: preferences.employment_types,
        soft_preferences: preferences.soft_preferences,
        preference_mode: preferences.effective_preference_mode,
        preference_rerank_top_n:
            preferences.preference_rerank_top_n ??
            preferences.effective_preference_rerank_top_n ??
            bounds.default,
    };
}

const REMOTE_OPTIONS = [
    { value: 'any', label: 'Any arrangement' },
    { value: 'remote', label: 'Remote only' },
    { value: 'hybrid', label: 'Hybrid okay' },
    { value: 'onsite', label: 'Onsite okay' },
] as const;

const PREFERENCE_MODE_OPTIONS = {
    semantic_rerank: {
        label: 'Semantic rerank',
        description: 'Default semantic personalization after fit-qualified matches.',
    },
    llm_judge: {
        label: 'LLM judge',
        description: 'Higher-cost experimental reasoning path for shortlist personalization.',
    },
} as const;

const VISA_SPONSORSHIP_FIELD_ID = 'candidate-visa-sponsorship-required';
const VISA_SPONSORSHIP_HELP_ID = 'candidate-visa-sponsorship-required-help';

const inputClasses =
    'w-full rounded-md border border-rule bg-surface px-3 py-2.5 text-[14px] text-ink placeholder:text-ink-muted transition-colors focus:border-accent focus:outline-none';

export function CandidatePreferencesPanel() {
    const { preferences, isLoading, isSaving, savePreferences } = useCandidatePreferences();
    const [draft, setDraft] = useState<DraftPreferences | null>(null);
    const [hasUnsavedChanges, setHasUnsavedChanges] = useState(false);
    const lastHydratedRevisionRef = useRef<number | null>(null);

    useEffect(() => {
        const revisionChanged = lastHydratedRevisionRef.current !== preferences?.revision;
        if (preferences && (draft === null || (!hasUnsavedChanges && revisionChanged))) {
            setDraft(toDraft(preferences));
            setHasUnsavedChanges(false);
            lastHydratedRevisionRef.current = preferences.revision;
        }
    }, [draft, hasUnsavedChanges, preferences]);

    if (isLoading || !draft || !preferences) {
        return (
            <div className="space-y-3">
                <div className="h-28 animate-pulse border border-rule bg-surface-sunk" />
                <div className="h-40 animate-pulse border border-rule bg-surface-sunk" />
                <div className="h-56 animate-pulse border border-rule bg-surface-sunk" />
            </div>
        );
    }

    const updateDraft = <K extends keyof DraftPreferences>(key: K, value: DraftPreferences[K]) => {
        setDraft((current) => (current ? { ...current, [key]: value } : current));
        setHasUnsavedChanges(true);
    };
    const topNBounds = preferences.preference_rerank_top_n_bounds ?? { min: 1, max: 100, default: 25 };

    const handleSave = async () => {
        try {
            await savePreferences({
                ...draft,
                target_locations: draft.target_locations,
                employment_types: draft.employment_types,
            });
            toast.success('Preferences saved.');
            setHasUnsavedChanges(false);
        } catch (error) {
            const message = error instanceof Error ? error.message : 'Couldn’t save your preferences.';
            toast.error(message);
        }
    };

    return (
        <div className="space-y-6">
            <section className="grid gap-4 lg:grid-cols-2">
                <div className="border border-rule bg-surface">
                    <div className="border-b border-rule px-5 py-4">
                        <p className="caption">Hard filters</p>
                        <h3 className="mt-1 text-[15px] font-medium text-ink">Must-haves</h3>
                        <p className="mt-1 text-[13px] text-ink-soft">Trim the pool before ranking.</p>
                    </div>

                    <div className="space-y-5 px-5 py-5">
                        <label className="block">
                            <span className="caption">Remote mode</span>
                            <select
                                value={draft.remote_mode}
                                onChange={(event) => updateDraft('remote_mode', event.target.value as DraftPreferences['remote_mode'])}
                                className={`${inputClasses} mt-2`}
                            >
                                {REMOTE_OPTIONS.map((option) => (
                                    <option key={option.value} value={option.value}>
                                        {option.label}
                                    </option>
                                ))}
                            </select>
                        </label>

                        <label className="block">
                            <span className="caption">Target locations</span>
                            <input
                                type="text"
                                value={draft.target_locations.join(', ')}
                                onChange={(event) =>
                                    updateDraft(
                                        'target_locations',
                                        event.target.value.split(',').map((value) => value.trim()).filter(Boolean),
                                    )
                                }
                                placeholder="Tokyo, Remote Japan, Berlin"
                                className={`${inputClasses} mt-2`}
                            />
                        </label>

                        <label className="block">
                            <span className="caption">Minimum salary</span>
                            <input
                                type="number"
                                min={0}
                                value={draft.salary_min ?? ''}
                                onChange={(event) =>
                                    updateDraft(
                                        'salary_min',
                                        event.target.value ? Number(event.target.value) : null,
                                    )
                                }
                                placeholder="Leave blank if flexible"
                                className={`${inputClasses} mt-2`}
                            />
                        </label>

                        <label className="block">
                            <span className="caption">Employment types</span>
                            <input
                                type="text"
                                value={draft.employment_types.join(', ')}
                                onChange={(event) =>
                                    updateDraft(
                                        'employment_types',
                                        event.target.value.split(',').map((value) => value.trim()).filter(Boolean),
                                    )
                                }
                                placeholder="Full-time, Contract"
                                className={`${inputClasses} mt-2`}
                            />
                        </label>

                        <div className="flex items-start justify-between gap-4 border-t border-rule pt-4">
                            <label htmlFor={VISA_SPONSORSHIP_FIELD_ID} className="block">
                                <span className="text-[14px] font-medium text-ink">Visa sponsorship required</span>
                                <span
                                    id={VISA_SPONSORSHIP_HELP_ID}
                                    className="mt-1 block text-[13px] text-ink-soft"
                                >
                                    Only show roles that explicitly satisfy sponsorship needs.
                                </span>
                            </label>
                            <input
                                id={VISA_SPONSORSHIP_FIELD_ID}
                                type="checkbox"
                                checked={draft.visa_sponsorship_required}
                                onChange={(event) => updateDraft('visa_sponsorship_required', event.target.checked)}
                                aria-describedby={VISA_SPONSORSHIP_HELP_ID}
                                className="mt-1 h-4 w-4 rounded-sm border-rule accent-accent"
                            />
                        </div>
                    </div>
                </div>

                <div className="border border-rule bg-surface">
                    <div className="border-b border-rule px-5 py-4">
                        <p className="caption">Soft preferences</p>
                        <h3 className="mt-1 text-[15px] font-medium text-ink">What matters to you</h3>
                        <p className="mt-1 text-[13px] text-ink-soft">Free-text guidance used after fit passes.</p>
                    </div>

                    <div className="space-y-5 px-5 py-5">
                        <label className="block">
                            <span className="caption">Preference mode</span>
                            <select
                                value={draft.preference_mode}
                                onChange={(event) =>
                                    updateDraft(
                                        'preference_mode',
                                        event.target.value as DraftPreferences['preference_mode'],
                                    )
                                }
                                className={`${inputClasses} mt-2`}
                            >
                                {preferences.allowed_preference_modes.map((mode) => (
                                    <option key={mode} value={mode}>
                                        {PREFERENCE_MODE_OPTIONS[mode].label}
                                    </option>
                                ))}
                            </select>
                            <p className="mt-2 text-[13px] text-ink-soft">
                                {PREFERENCE_MODE_OPTIONS[draft.preference_mode].description}
                            </p>
                        </label>

                        <label className="block">
                            <span className="caption">Candidates scored for preference</span>
                            <input
                                type="number"
                                min={topNBounds.min}
                                max={topNBounds.max}
                                value={draft.preference_rerank_top_n ?? ''}
                                onChange={(event) => {
                                    const rawValue = event.target.value;
                                    if (!rawValue) {
                                        updateDraft('preference_rerank_top_n', null);
                                        return;
                                    }
                                    const nextValue = Number(rawValue);
                                    updateDraft(
                                        'preference_rerank_top_n',
                                        Math.max(topNBounds.min, Math.min(topNBounds.max, nextValue)),
                                    );
                                }}
                                className={`${inputClasses} mt-2`}
                            />
                            <p className="mt-2 text-[13px] text-ink-soft">
                                Score the top N candidates by fit. A larger window finds more preference matches but takes longer.
                            </p>
                        </label>

                        <label className="block">
                            <span className="caption">Guidance</span>
                            <textarea
                                value={draft.soft_preferences}
                                onChange={(event) => updateDraft('soft_preferences', event.target.value)}
                                placeholder="I prefer small product teams, mentorship, and Python backend work with room to grow."
                                rows={10}
                                className={`${inputClasses} mt-2 leading-relaxed`}
                            />
                        </label>
                        {preferences.soft_preference_summary && (
                            <p className="border-l-2 border-rule pl-3 text-[13px] text-ink-soft">
                                <span className="caption mr-1">Summary</span>
                                {preferences.soft_preference_summary}
                            </p>
                        )}
                    </div>
                </div>
            </section>

            <section className="flex flex-col gap-4 border border-rule bg-surface-raised px-5 py-4 sm:flex-row sm:items-center sm:justify-between">
                <div>
                    <p className="text-[14px] text-ink">
                        {hasUnsavedChanges ? 'You have unsaved changes.' : 'Your preferences are up to date.'}
                    </p>
                    <p className="mt-0.5 caption tabular-nums">Revision {preferences.revision}</p>
                </div>
                <Button
                    type="button"
                    variant="primary"
                    onClick={() => void handleSave()}
                    isLoading={isSaving}
                    disabled={!hasUnsavedChanges}
                >
                    Save preferences
                </Button>
            </section>
        </div>
    );
}
