import { render, screen, waitFor } from '@testing-library/react';
import { vi } from 'vitest';

import { EmailVerificationPage } from '../EmailVerificationPage';
import { useNotificationSettings } from '@/hooks/useNotificationSettings';

vi.mock('@/hooks/useNotificationSettings', () => ({
    useNotificationSettings: vi.fn(),
}));

const mockUseNotificationSettings = vi.mocked(useNotificationSettings);

describe('EmailVerificationPage', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        window.history.replaceState({}, '', '/verify-email');
    });

    it('shows a missing-token error immediately', () => {
        mockUseNotificationSettings.mockReturnValue({
            verifyEmailOverride: vi.fn(),
        } as never);

        render(<EmailVerificationPage />);

        expect(screen.getByRole('alert')).toHaveTextContent('Verification token is missing.');
        expect(screen.getByRole('heading', { name: /verification didn’t complete/i })).toBeInTheDocument();
    });

    it('renders a success message after verifying the token', async () => {
        window.history.replaceState({}, '', '/verify-email?token=abc123');
        const verifyEmailOverride = vi.fn().mockResolvedValue({
            data: { message: 'Email override verified.' },
        });
        mockUseNotificationSettings.mockReturnValue({
            verifyEmailOverride,
        } as never);

        render(<EmailVerificationPage />);

        await waitFor(() => {
            expect(screen.getByText('Email override verified.')).toBeInTheDocument();
        });

        expect(verifyEmailOverride).toHaveBeenCalledWith('abc123');
        expect(screen.getByRole('heading', { name: /email verified/i })).toBeInTheDocument();
    });

    it('surfaces verification failures from the API', async () => {
        window.history.replaceState({}, '', '/verify-email?token=expired');
        const verifyEmailOverride = vi.fn().mockRejectedValue(new Error('Link expired.'));
        mockUseNotificationSettings.mockReturnValue({
            verifyEmailOverride,
        } as never);

        render(<EmailVerificationPage />);

        await waitFor(() => {
            expect(screen.getByRole('alert')).toHaveTextContent('Link expired.');
        });

        expect(screen.getByRole('heading', { name: /verification didn’t complete/i })).toBeInTheDocument();
    });
});
