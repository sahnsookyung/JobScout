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
    return { container, div: container.firstChild as HTMLElement };
}

describe('Card', () => {
    it('renders children', () => {
        render(<Card><p>Card content</p></Card>);
        expect(screen.getByText('Card content')).toBeInTheDocument();
    });

    it('applies base styling classes', () => {
        const { div } = renderCard();
        expect(div).toHaveClass('bg-white', 'rounded-xl', 'shadow-md');
    });

    it('merges custom className', () => {
        const { div } = renderCard({ children: 'Content', className: 'p-4 extra' });
        expect(div).toHaveClass('p-4', 'extra');
    });

    it('has no role when onClick is not provided', () => {
        const { div } = renderCard({ children: 'Static' });
        expect(div).not.toHaveAttribute('role');
    });

    it('has role="button" when onClick is provided', () => {
        const { div } = renderCard({ children: 'Clickable', onClick: vi.fn() });
        expect(div).toHaveAttribute('role', 'button');
    });

    it('has no tabIndex when onClick is not provided', () => {
        const { div } = renderCard({ children: 'Static' });
        expect(div).not.toHaveAttribute('tabIndex');
    });

    it('has tabIndex="0" when onClick is provided', () => {
        const { div } = renderCard({ children: 'Clickable', onClick: vi.fn() });
        expect(div).toHaveAttribute('tabIndex', '0');
    });

    it('applies cursor-pointer class when onClick is provided', () => {
        const { div } = renderCard({ children: 'Clickable', onClick: vi.fn() });
        expect(div).toHaveClass('cursor-pointer');
    });

    it('does not apply cursor-pointer class when onClick is not provided', () => {
        const { div } = renderCard({ children: 'Static' });
        expect(div).not.toHaveClass('cursor-pointer');
    });

    it('calls onClick when clicked', () => {
        const onClick = vi.fn();
        render(<Card onClick={onClick}>Click me</Card>);
        fireEvent.click(screen.getByText('Click me'));
        expect(onClick).toHaveBeenCalledTimes(1);
    });

    it.each([
        ['Enter', true],
        [' ', true],
        ['Tab', false],
    ])('key "%s" %s call onClick', (key, shouldCall) => {
        const onClick = vi.fn();
        const { div } = renderCard({ children: 'Keyboard', onClick });
        fireEvent.keyDown(div, { key });
        expect(onClick).toHaveBeenCalledTimes(shouldCall ? 1 : 0);
    });

    it('does not attach keyDown handler when no onClick', () => {
        const { div } = renderCard({ children: 'No click' });
        expect(() => fireEvent.keyDown(div, { key: 'Enter' })).not.toThrow();
    });
});
