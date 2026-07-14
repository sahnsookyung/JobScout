import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { vi } from 'vitest';
import { toast } from 'sonner';

import { CandidatePreferencesPanel } from '../CandidatePreferencesPanel';
import { useCandidatePreferences } from '@/hooks/useCandidatePreferences';

vi.mock('@/hooks/useCandidatePreferences');
vi.mock('sonner');
vi.mock('../PreferenceRankingSettings', () => ({
    PreferenceRankingSettings: () => <section>Preference ranking settings</section>,
}));

const mockUseCandidatePreferences = vi.mocked(useCandidatePreferences);

describe('CandidatePreferencesPanel', () => {
    const savePreferences = vi.fn();
    let hookState: ReturnType<typeof useCandidatePreferences>;

    beforeEach(() => {
        vi.clearAllMocks();
        hookState = {
            preferences: {
                remote_mode: 'remote',
                target_locations: ['Berlin'],
                visa_sponsorship_required: false,
                salary_min: 120000,
                employment_types: ['Full-time'],
                soft_preferences: 'Mentorship and backend growth',
                soft_preference_summary: 'mentorship, backend growth',
                preference_mode: 'semantic_rerank',
                preference_rerank_top_n: 25,
                effective_preference_rerank_top_n: 25,
                preference_rerank_top_n_bounds: { min: 1, max: 100, default: 25 },
                allowed_preference_modes: ['semantic_rerank', 'llm_judge'],
                effective_preference_mode: 'semantic_rerank',
                revision: 1,
            },
            isLoading: false,
            isSaving: false,
            savePreferences,
        };
        mockUseCandidatePreferences.mockImplementation(() => hookState);
        savePreferences.mockResolvedValue({ data: hookState.preferences });
    });

    it('places preference ranking settings inside candidate preferences', () => {
        render(<CandidatePreferencesPanel />);

        expect(screen.getByText('Preference ranking settings')).toBeInTheDocument();
    });

    it('preserves unsaved edits across background preference refetches', async () => {
        const { rerender } = render(<CandidatePreferencesPanel />);

        const textarea = screen.getByPlaceholderText(/small product teams/i);
        await userEvent.clear(textarea);
        await userEvent.type(textarea, 'Unsaved local preference');

        hookState = {
            ...hookState,
            preferences: {
                ...hookState.preferences!,
                revision: 2,
                soft_preferences: 'Server pushed a different preference',
            },
        };
        rerender(<CandidatePreferencesPanel />);

        expect(screen.getByDisplayValue('Unsaved local preference')).toBeInTheDocument();
    });

    it('hydrates the latest saved preferences once the form is clean again', async () => {
        const { rerender } = render(<CandidatePreferencesPanel />);

        const textarea = screen.getByPlaceholderText(/small product teams/i);
        await userEvent.clear(textarea);
        await userEvent.type(textarea, 'Updated saved preference');

        savePreferences.mockImplementation(async (payload) => {
            hookState = {
                ...hookState,
                preferences: {
                    ...payload,
                    soft_preference_summary: 'updated saved preference',
                    effective_preference_rerank_top_n: payload.preference_rerank_top_n ?? 25,
                    preference_rerank_top_n_bounds: { min: 1, max: 100, default: 25 },
                    allowed_preference_modes: ['semantic_rerank', 'llm_judge'],
                    effective_preference_mode: payload.preference_mode,
                    revision: 2,
                },
            };
            return { data: hookState.preferences };
        });

        await userEvent.click(screen.getByRole('button', { name: /save preferences/i }));

        await waitFor(() => {
            expect(savePreferences).toHaveBeenCalledWith({
                remote_mode: 'remote',
                target_locations: ['Berlin'],
                visa_sponsorship_required: false,
                salary_min: 120000,
                employment_types: ['Full-time'],
                soft_preferences: 'Updated saved preference',
                preference_mode: 'semantic_rerank',
                preference_rerank_top_n: 25,
            });
        });
        expect(toast.success).toHaveBeenCalledWith('Preferences saved.');

        rerender(<CandidatePreferencesPanel />);

        expect(screen.getByDisplayValue('Updated saved preference')).toBeInTheDocument();
        expect(screen.getByText('Revision 2')).toBeInTheDocument();
    });

    it('saves edited hard filters as normalized arrays and flags unsaved changes', async () => {
        render(<CandidatePreferencesPanel />);

        await userEvent.selectOptions(screen.getAllByRole('combobox')[0], 'hybrid');
        fireEvent.change(screen.getByPlaceholderText('Tokyo, Remote Japan, Berlin'), {
            target: { value: 'Tokyo, Remote Japan' },
        });
        await userEvent.clear(screen.getByPlaceholderText('Leave blank if flexible'));
        await userEvent.type(screen.getByPlaceholderText('Leave blank if flexible'), '150000');
        fireEvent.change(screen.getByPlaceholderText('Full-time, Contract'), {
            target: { value: 'Contract, Part-time' },
        });
        await userEvent.click(screen.getByRole('checkbox'));
        await userEvent.selectOptions(screen.getAllByRole('combobox')[1], 'llm_judge');
        await userEvent.clear(screen.getByLabelText(/candidates scored for preference/i));
        await userEvent.type(screen.getByLabelText(/candidates scored for preference/i), '15');

        expect(screen.getByText('You have unsaved changes.')).toBeInTheDocument();

        await userEvent.click(screen.getByRole('button', { name: /save preferences/i }));

        await waitFor(() => {
            expect(savePreferences).toHaveBeenCalledWith({
                remote_mode: 'hybrid',
                target_locations: ['Tokyo', 'Remote Japan'],
                visa_sponsorship_required: true,
                salary_min: 150000,
                employment_types: ['Contract', 'Part-time'],
                soft_preferences: 'Mentorship and backend growth',
                preference_mode: 'llm_judge',
                preference_rerank_top_n: 15,
            });
        });
    });

    it('shows a save error toast when saving fails', async () => {
        savePreferences.mockRejectedValueOnce(new Error('Save exploded'));
        render(<CandidatePreferencesPanel />);

        await userEvent.type(screen.getByPlaceholderText(/small product teams/i), ' with extra detail');
        await userEvent.click(screen.getByRole('button', { name: /save preferences/i }));

        await waitFor(() => {
            expect(toast.error).toHaveBeenCalledWith('Save exploded');
        });
    });

    it('falls back to a generic save error message for non-Error failures', async () => {
        savePreferences.mockRejectedValueOnce('unexpected');
        render(<CandidatePreferencesPanel />);

        await userEvent.type(screen.getByPlaceholderText(/small product teams/i), ' with extra detail');
        await userEvent.click(screen.getByRole('button', { name: /save preferences/i }));

        await waitFor(() => {
            expect(toast.error).toHaveBeenCalledWith('Couldn’t save your preferences.');
        });
    });

    it('renders a loading skeleton while preferences are loading', () => {
        hookState = {
            preferences: undefined,
            isLoading: true,
            isSaving: false,
            savePreferences,
        };

        const { container } = render(<CandidatePreferencesPanel />);

        expect(container.querySelectorAll('.animate-pulse')).toHaveLength(3);
    });

    it('exposes an accessible label for the visa sponsorship checkbox', () => {
        render(<CandidatePreferencesPanel />);

        expect(screen.getByLabelText(/visa sponsorship required/i)).toBeInTheDocument();
    });
});
