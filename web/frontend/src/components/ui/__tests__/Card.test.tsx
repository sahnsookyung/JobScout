/**
 * Tests for Card component
 * Covers: web/frontend/src/components/ui/Card.tsx
 */

import { render, screen, fireEvent } from '@testing-library/react';
import { describe, it, expect, vi } from 'vitest';
import { Card } from '../Card';

const defaultCardProps: React.ComponentProps<typeof Card> = { children: 'Content' };

function renderCard(props: React.ComponentProps<typeof Card> = defaultCardProps) {
    const { container } = render(<Card {...props} />);
    return { container, element: container.firstChild as HTMLElement };
}

describe('Card', () => {
    it('renders children', () => {
        render(<Card><p>Card content</p></Card>);
        expect(screen.getByText('Card content')).toBeInTheDocument();
    });

    it('applies base styling classes', () => {
        const { element } = renderCard();
        expect(element).toHaveClass('bg-white', 'rounded-xl', 'shadow-md');
    });

    it('merges custom className', () => {
        const { element } = renderCard({ children: 'Content', className: 'p-4 extra' });
        expect(element).toHaveClass('p-4', 'extra');
    });

    it('has no role when onClick is not provided', () => {
        const { element } = renderCard({ children: 'Static' });
        expect(element.tagName).toBe('DIV');
        expect(element).not.toHaveAttribute('role');
    });

    it('renders a native button when onClick is provided', () => {
        const { element } = renderCard({ children: 'Clickable', onClick: vi.fn() });
        expect(element.tagName).toBe('BUTTON');
        expect(element).toHaveAttribute('type', 'button');
    });

    it('has no tabIndex when onClick is not provided', () => {
        const { element } = renderCard({ children: 'Static' });
        expect(element).not.toHaveAttribute('tabIndex');
    });

    it('uses native keyboard behavior when onClick is provided', () => {
        const { element } = renderCard({ children: 'Clickable', onClick: vi.fn() });
        expect(element).not.toHaveAttribute('role');
        expect(element).not.toHaveAttribute('tabIndex');
    });

    it('applies cursor-pointer class when onClick is provided', () => {
        const { element } = renderCard({ children: 'Clickable', onClick: vi.fn() });
        expect(element).toHaveClass('cursor-pointer');
    });

    it('does not apply cursor-pointer class when onClick is not provided', () => {
        const { element } = renderCard({ children: 'Static' });
        expect(element).not.toHaveClass('cursor-pointer');
    });

    it('calls onClick when clicked', () => {
        const onClick = vi.fn();
        render(<Card onClick={onClick}>Click me</Card>);
        fireEvent.click(screen.getByText('Click me'));
        expect(onClick).toHaveBeenCalledTimes(1);
    });

    it('renders static cards as divs', () => {
        const { element } = renderCard({ children: 'No click' });
        expect(element.tagName).toBe('DIV');
    });
});
