import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { vi } from 'vitest';

import { DashboardHeader } from '../DashboardHeader';

vi.mock('@/features/auth/useAuth', () => ({
    useAuth: vi.fn(() => ({
        user: {
            name: 'Ada Lovelace',
            email: 'ada@example.com',
            picture: 'https://example.com/ada.png',
        },
        logout: vi.fn(),
    })),
}));

vi.mock('@/features/notifications/components/NotificationSettingsPanel', () => ({
    NotificationSettingsPanel: () => <div>Notification settings content</div>,
}));

describe('DashboardHeader', () => {
    it('opens notification settings from the top bar', async () => {
        render(<DashboardHeader />);

        await userEvent.click(screen.getByRole('button', { name: /open notification settings/i }));

        expect(screen.getByRole('dialog', { name: /notification preferences/i })).toBeInTheDocument();
        expect(screen.getByText('Notification settings content')).toBeInTheDocument();
    });

    it('closes notification settings from the backdrop control', async () => {
        render(<DashboardHeader />);

        await userEvent.click(screen.getByRole('button', { name: /open notification settings/i }));
        await userEvent.click(screen.getAllByRole('button', { name: /close notification settings/i })[0]);

        expect(
            screen.queryByRole('dialog', { name: /notification preferences/i }),
        ).not.toBeInTheDocument();
    });

    it('shows the profile panel from the top bar', async () => {
        render(<DashboardHeader />);

        await userEvent.click(screen.getByRole('button', { name: /open profile menu/i }));

        const menu = screen.getByRole('menu');

        expect(menu).toBeInTheDocument();
        expect(within(menu).getByText('Ada Lovelace')).toBeInTheDocument();
        expect(within(menu).getByText('ada@example.com')).toBeInTheDocument();
        expect(within(menu).getByRole('button', { name: /sign out/i })).toBeInTheDocument();
    });

    it('renders the notification button before the profile button', () => {
        render(<DashboardHeader />);

        const notificationButton = screen.getByRole('button', { name: /open notification settings/i });
        const profileButton = screen.getByRole('button', { name: /open profile menu/i });

        expect(notificationButton.compareDocumentPosition(profileButton) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    });
});
