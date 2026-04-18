import { render, screen } from '@testing-library/react';

import { Badge } from '../Badge';

describe('Badge', () => {
    it('renders the default variant styles', () => {
        render(<Badge>Default</Badge>);

        expect(screen.getByText('Default')).toHaveClass(
            'bg-surface-sunk',
            'text-ink-soft',
            'border',
            'border-rule',
        );
    });

    it('renders semantic variants', () => {
        const { rerender } = render(<Badge variant="accent">Accent</Badge>);
        expect(screen.getByText('Accent')).toHaveClass('bg-accent-soft', 'text-accent-ink');

        rerender(<Badge variant="warning">Warning</Badge>);
        expect(screen.getByText('Warning')).toHaveClass('bg-warn-soft', 'text-warn');
    });
});
