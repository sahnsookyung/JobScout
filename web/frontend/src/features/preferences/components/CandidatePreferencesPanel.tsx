import { useEffect, useRef, useState } from 'react';
import { Compass, MapPinned, Banknote, BriefcaseBusiness, Sparkles } from 'lucide-react';
import { toast } from 'sonner';

import { Button } from '@/components/ui/Button';
import { useCandidatePreferences } from '@/hooks/useCandidatePreferences';
import type {
    CandidatePreferences,
    CandidatePreferencesUpdateRequest,
} from '@/types/api';

type DraftPreferences = CandidatePreferencesUpdateRequest;

function toDraft(preferences: CandidatePreferences): DraftPreferences {
    return {
        remote_mode: preferences.remote_mode,
        target_locations: preferences.target_locations,
        visa_sponsorship_required: preferences.visa_sponsorship_required,
        salary_min: preferences.salary_min,
        employment_types: preferences.employment_types,
        soft_preferences: preferences.soft_preferences,
        preference_mode: preferences.effective_preference_mode,
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
            <div className="space-y-4">
                <div className="h-28 animate-pulse rounded-[1.75rem] bg-slate-200" />
                <div className="h-40 animate-pulse rounded-[1.75rem] bg-slate-200" />
                <div className="h-56 animate-pulse rounded-[1.75rem] bg-slate-200" />
            </div>
        );
    }

    const updateDraft = <K extends keyof DraftPreferences>(key: K, value: DraftPreferences[K]) => {
        setDraft((current) => (current ? { ...current, [key]: value } : current));
        setHasUnsavedChanges(true);
    };

    const handleSave = async () => {
        try {
            await savePreferences({
                ...draft,
                target_locations: draft.target_locations,
                employment_types: draft.employment_types,
            });
            toast.success('Candidate preferences saved');
            setHasUnsavedChanges(false);
        } catch (error) {
            const message = error instanceof Error ? error.message : 'Failed to save candidate preferences';
            toast.error(message);
        }
    };

    return (
        <div className="space-y-6">
            <section className="grid gap-4 lg:grid-cols-[0.9fr_1.1fr]">
                <div className="rounded-[1.75rem] border border-slate-200 bg-white px-5 py-5 shadow-sm">
                    <div className="flex items-center gap-3">
                        <div className="rounded-2xl bg-gradient-to-br from-slate-950 to-sky-700 p-3 text-white shadow-lg">
                            <Compass className="h-5 w-5" />
                        </div>
                        <div>
                            <h3 className="text-lg font-black text-slate-950">Hard filters</h3>
                            <p className="text-sm text-slate-500">
                                Must-haves that should trim the job pool before ranking.
                            </p>
                        </div>
                    </div>

                    <div className="mt-5 space-y-4">
                        <label className="block">
                            <div className="mb-2 text-xs font-black uppercase tracking-[0.22em] text-slate-500">
                                Remote mode
                            </div>
                            <select
                                value={draft.remote_mode}
                                onChange={(event) => updateDraft('remote_mode', event.target.value as DraftPreferences['remote_mode'])}
                                className="w-full rounded-2xl border border-slate-300 px-4 py-3 text-sm font-semibold text-slate-900"
                            >
                                {REMOTE_OPTIONS.map((option) => (
                                    <option key={option.value} value={option.value}>
                                        {option.label}
                                    </option>
                                ))}
                            </select>
                        </label>

                        <label className="block">
                            <div className="mb-2 flex items-center gap-2 text-xs font-black uppercase tracking-[0.22em] text-slate-500">
                                <MapPinned className="h-4 w-4" />
                                Target locations
                            </div>
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
                                className="w-full rounded-2xl border border-slate-300 px-4 py-3 text-sm"
                            />
                        </label>

                        <label className="block">
                            <div className="mb-2 flex items-center gap-2 text-xs font-black uppercase tracking-[0.22em] text-slate-500">
                                <Banknote className="h-4 w-4" />
                                Minimum salary
                            </div>
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
                                className="w-full rounded-2xl border border-slate-300 px-4 py-3 text-sm"
                            />
                        </label>

                        <label className="block">
                            <div className="mb-2 flex items-center gap-2 text-xs font-black uppercase tracking-[0.22em] text-slate-500">
                                <BriefcaseBusiness className="h-4 w-4" />
                                Employment types
                            </div>
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
                                className="w-full rounded-2xl border border-slate-300 px-4 py-3 text-sm"
                            />
                        </label>

                        <label className="flex items-start justify-between gap-4 rounded-[1.5rem] border border-slate-200 bg-slate-50 px-4 py-4 shadow-sm">
                            <div>
                                <div className="text-sm font-bold text-slate-900">Visa sponsorship required</div>
                                <div className="mt-1 text-sm text-slate-500">
                                    Only surface roles that explicitly satisfy sponsorship needs.
                                </div>
                            </div>
                            <input
                                type="checkbox"
                                checked={draft.visa_sponsorship_required}
                                onChange={(event) => updateDraft('visa_sponsorship_required', event.target.checked)}
                                className="mt-1 h-4 w-4 rounded border-slate-300 text-blue-600 focus:ring-blue-500"
                            />
                        </label>
                    </div>
                </div>

                <div className="rounded-[1.75rem] border border-slate-200 bg-white px-5 py-5 shadow-sm">
                    <div className="flex items-center gap-3">
                        <div className="rounded-2xl bg-gradient-to-br from-blue-600 to-indigo-700 p-3 text-white shadow-lg">
                            <Sparkles className="h-5 w-5" />
                        </div>
                        <div>
                            <h3 className="text-lg font-black text-slate-950">Soft preferences</h3>
                            <p className="text-sm text-slate-500">
                                Free-text guidance for reranking among already strong-fit jobs.
                            </p>
                        </div>
                    </div>

                    <div className="mt-5">
                        <label className="block">
                            <div className="mb-2 text-xs font-black uppercase tracking-[0.22em] text-slate-500">
                                Preference mode
                            </div>
                            <select
                                value={draft.preference_mode}
                                onChange={(event) =>
                                    updateDraft(
                                        'preference_mode',
                                        event.target.value as DraftPreferences['preference_mode'],
                                    )
                                }
                                className="w-full rounded-2xl border border-slate-300 px-4 py-3 text-sm font-semibold text-slate-900"
                            >
                                {preferences.allowed_preference_modes.map((mode) => (
                                    <option key={mode} value={mode}>
                                        {PREFERENCE_MODE_OPTIONS[mode].label}
                                    </option>
                                ))}
                            </select>
                            <p className="mt-2 text-sm text-slate-500">
                                {PREFERENCE_MODE_OPTIONS[draft.preference_mode].description}
                            </p>
                        </label>

                        <textarea
                            value={draft.soft_preferences}
                            onChange={(event) => updateDraft('soft_preferences', event.target.value)}
                            placeholder="I prefer fast-moving product teams, mentorship, and modern Python backend work with room to grow."
                            rows={12}
                            className="mt-4 w-full rounded-[1.5rem] border border-slate-300 px-4 py-4 text-sm leading-6 text-slate-900"
                        />
                        {preferences.soft_preference_summary ? (
                            <p className="mt-3 text-sm text-slate-500">
                                Summary: {preferences.soft_preference_summary}
                            </p>
                        ) : null}
                    </div>
                </div>
            </section>

            <section className="flex flex-col gap-4 rounded-[1.75rem] border border-slate-200 bg-slate-950 px-5 py-5 text-white shadow-xl sm:flex-row sm:items-center sm:justify-between">
                <div>
                    <div className="text-sm font-bold">
                        {hasUnsavedChanges
                            ? 'You have unsaved preference changes.'
                            : 'Preference settings are up to date.'}
                    </div>
                    <div className="mt-1 text-sm text-slate-300">
                        Revision {preferences.revision}
                    </div>
                </div>
                <Button
                    type="button"
                    onClick={() => void handleSave()}
                    isLoading={isSaving}
                    className="rounded-2xl bg-white text-slate-950 hover:bg-slate-100"
                >
                    Save preferences
                </Button>
            </section>
        </div>
    );
}
