import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { vi } from 'vitest';

import { ThemeToggle } from '../ThemeToggle';
import { useTheme } from '@/hooks/useTheme';

vi.mock('@/hooks/useTheme', () => ({
    useTheme: vi.fn(),
}));

const mockUseTheme = vi.mocked(useTheme);

describe('ThemeToggle', () => {
    it('renders a dark mode affordance while the light theme is active', () => {
        mockUseTheme.mockReturnValue({
            theme: 'light',
            setTheme: vi.fn(),
            toggle: vi.fn(),
        });

        render(<ThemeToggle />);

        expect(screen.getByRole('button', { name: /switch to dark theme/i })).toBeInTheDocument();
        expect(screen.getByTitle('Dark mode')).toBeInTheDocument();
    });

    it('toggles back to light mode from the dark theme', async () => {
        const toggle = vi.fn();
        mockUseTheme.mockReturnValue({
            theme: 'dark',
            setTheme: vi.fn(),
            toggle,
        });

        render(<ThemeToggle />);
        await userEvent.click(screen.getByRole('button', { name: /switch to light theme/i }));

        expect(toggle).toHaveBeenCalledTimes(1);
    });
});
