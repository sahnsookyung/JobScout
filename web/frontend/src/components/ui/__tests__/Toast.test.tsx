/**
 * Tests for Toast component
 * Covers: src/components/ui/Toast.tsx
 */

import { render } from '@testing-library/react';
import { ToastProvider, toast } from '../Toast';

vi.mock('sonner', () => ({
    Toaster: ({ position, theme, visibleToasts, toastOptions, style }: any) => (
        <div
            data-testid="toaster"
            data-position={position}
            data-theme={theme}
            data-visible-toasts={visibleToasts}
            data-class-name={toastOptions?.className}
            style={style}
        />
    ),
    toast: {
        success: vi.fn(),
        error: vi.fn(),
        info: vi.fn(),
    },
}));

describe('ToastProvider', () => {
    it('renders the Toaster component', () => {
        const { getByTestId } = render(<ToastProvider />);
        expect(getByTestId('toaster')).toBeTruthy();
    });

    it('passes position="bottom-right"', () => {
        const { getByTestId } = render(<ToastProvider />);
        expect(getByTestId('toaster').getAttribute('data-position')).toBe('bottom-right');
    });

    it('passes theme="system"', () => {
        const { getByTestId } = render(<ToastProvider />);
        expect(getByTestId('toaster').getAttribute('data-theme')).toBe('system');
    });

    it('passes visibleToasts=3', () => {
        const { getByTestId } = render(<ToastProvider />);
        expect(getByTestId('toaster').getAttribute('data-visible-toasts')).toBe('3');
    });

    it('passes sonner-toast className', () => {
        const { getByTestId } = render(<ToastProvider />);
        expect(getByTestId('toaster').getAttribute('data-class-name')).toBe('sonner-toast');
    });
});

describe('toast export', () => {
    it('re-exports toast from sonner', () => {
        expect(toast).toBeDefined();
    });
});
