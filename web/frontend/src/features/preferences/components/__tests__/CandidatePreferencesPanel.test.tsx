import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { vi } from 'vitest';
import { toast } from 'sonner';

import { CandidatePreferencesPanel } from '../CandidatePreferencesPanel';
import { useCandidatePreferences } from '@/hooks/useCandidatePreferences';

vi.mock('@/hooks/useCandidatePreferences');
vi.mock('sonner');

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

    it('preserves unsaved edits across background preference refetches', async () => {
        const { rerender } = render(<CandidatePreferencesPanel />);

        const textarea = screen.getByPlaceholderText(/i prefer fast-moving product teams/i);
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

        const textarea = screen.getByPlaceholderText(/i prefer fast-moving product teams/i);
        await userEvent.clear(textarea);
        await userEvent.type(textarea, 'Updated saved preference');

        savePreferences.mockImplementation(async (payload) => {
            hookState = {
                ...hookState,
                preferences: {
                    ...payload,
                    soft_preference_summary: 'updated saved preference',
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
            });
        });
        expect(toast.success).toHaveBeenCalledWith('Candidate preferences saved');

        rerender(<CandidatePreferencesPanel />);

        expect(screen.getByDisplayValue('Updated saved preference')).toBeInTheDocument();
        expect(screen.getByText('Revision 2')).toBeInTheDocument();
    });

    it('saves edited hard filters as normalized arrays and flags unsaved changes', async () => {
        render(<CandidatePreferencesPanel />);

        await userEvent.selectOptions(screen.getAllByRole('combobox')[0], 'hybrid');
        fireEvent.change(screen.getByPlaceholderText(/tokyo, remote japan, berlin/i), {
            target: { value: 'Tokyo, Remote Japan' },
        });
        await userEvent.clear(screen.getByPlaceholderText(/leave blank if flexible/i));
        await userEvent.type(screen.getByPlaceholderText(/leave blank if flexible/i), '150000');
        fireEvent.change(screen.getByPlaceholderText(/full-time, contract/i), {
            target: { value: 'Contract, Part-time' },
        });
        await userEvent.click(screen.getByRole('checkbox'));
        await userEvent.selectOptions(screen.getAllByRole('combobox')[1], 'llm_judge');

        expect(screen.getByText('You have unsaved preference changes.')).toBeInTheDocument();

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
            });
        });
    });

    it('shows a save error toast when saving fails', async () => {
        savePreferences.mockRejectedValueOnce(new Error('Save exploded'));
        render(<CandidatePreferencesPanel />);

        await userEvent.click(screen.getByRole('button', { name: /save preferences/i }));

        await waitFor(() => {
            expect(toast.error).toHaveBeenCalledWith('Save exploded');
        });
    });

    it('falls back to a generic save error message for non-Error failures', async () => {
        savePreferences.mockRejectedValueOnce('unexpected');
        render(<CandidatePreferencesPanel />);

        await userEvent.click(screen.getByRole('button', { name: /save preferences/i }));

        await waitFor(() => {
            expect(toast.error).toHaveBeenCalledWith('Failed to save candidate preferences');
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
});
