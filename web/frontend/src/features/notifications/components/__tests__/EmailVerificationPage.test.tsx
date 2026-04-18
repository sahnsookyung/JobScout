import { render, screen, waitFor } from '@testing-library/react';
import { vi } from 'vitest';

import { EmailVerificationPage } from '../EmailVerificationPage';
import { notificationSettingsApi } from '@/services/notificationSettingsApi';

vi.mock('@/services/notificationSettingsApi', () => ({
    notificationSettingsApi: {
        verifyEmailOverride: vi.fn(),
    },
}));

const mockNotificationSettingsApi = vi.mocked(notificationSettingsApi);

describe('EmailVerificationPage', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        window.history.replaceState({}, '', '/verify-email');
    });

    it('shows a missing-token error immediately', () => {
        render(<EmailVerificationPage />);

        expect(screen.getByRole('alert')).toHaveTextContent('Verification token is missing.');
        expect(screen.getByRole('heading', { name: /verification didn’t complete/i })).toBeInTheDocument();
    });

    it('renders a success message after verifying the token', async () => {
        window.history.replaceState({}, '', '/verify-email#token=abc123');
        mockNotificationSettingsApi.verifyEmailOverride.mockResolvedValue({
            data: { success: true, message: 'Email override verified.' },
        } as never);

        render(<EmailVerificationPage />);

        await waitFor(() => {
            expect(screen.getByText('Email override verified.')).toBeInTheDocument();
        });

        expect(mockNotificationSettingsApi.verifyEmailOverride).toHaveBeenCalledWith({
            token: 'abc123',
        });
        expect(screen.getByRole('heading', { name: /email verified/i })).toBeInTheDocument();
    });

    it('surfaces verification failures from the API', async () => {
        window.history.replaceState({}, '', '/verify-email#token=expired');
        mockNotificationSettingsApi.verifyEmailOverride.mockRejectedValue(new Error('Link expired.'));

        render(<EmailVerificationPage />);

        await waitFor(() => {
            expect(screen.getByRole('alert')).toHaveTextContent('Link expired.');
        });

        expect(screen.getByRole('heading', { name: /verification didn’t complete/i })).toBeInTheDocument();
    });
});
