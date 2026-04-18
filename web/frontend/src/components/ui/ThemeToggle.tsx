import { Moon, Sun } from 'lucide-react';
import { useTheme } from '@/hooks/useTheme';

export function ThemeToggle() {
    const { theme, toggle } = useTheme();
    const isDark = theme === 'dark';
    const Icon = isDark ? Sun : Moon;

    return (
        <button
            type="button"
            onClick={toggle}
            className="inline-flex h-9 w-9 items-center justify-center rounded-md border border-rule bg-surface text-ink-soft transition-colors duration-200 hover:border-rule-strong hover:text-ink"
            aria-label={isDark ? 'Switch to light theme' : 'Switch to dark theme'}
            title={isDark ? 'Light mode' : 'Dark mode'}
        >
            <Icon className="h-4 w-4" aria-hidden="true" />
        </button>
    );
}
