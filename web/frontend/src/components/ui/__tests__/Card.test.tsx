/**
 * Tests for Card component
 * Covers: web/frontend/src/components/ui/Card.tsx
 */

import { render, screen, fireEvent } from '@testing-library/react';
import { describe, it, expect, vi } from 'vitest';
import { Card } from '../Card';

describe('Card', () => {
    it('renders children', () => {
        render(<Card><p>Card content</p></Card>);
        expect(screen.getByText('Card content')).toBeInTheDocument();
    });

    it('applies base styling classes', () => {
        const { container } = render(<Card>Content</Card>);
        const div = container.firstChild as HTMLElement;
        expect(div).toHaveClass('bg-white', 'rounded-xl', 'shadow-md');
    });

    it('merges custom className', () => {
        const { container } = render(<Card className="p-4 extra">Content</Card>);
        const div = container.firstChild as HTMLElement;
        expect(div).toHaveClass('p-4', 'extra');
    });

    it('has no role when onClick is not provided', () => {
        const { container } = render(<Card>Static</Card>);
        const div = container.firstChild as HTMLElement;
        expect(div).not.toHaveAttribute('role');
    });

    it('has role="button" when onClick is provided', () => {
        const { container } = render(<Card onClick={vi.fn()}>Clickable</Card>);
        const div = container.firstChild as HTMLElement;
        expect(div).toHaveAttribute('role', 'button');
    });

    it('has no tabIndex when onClick is not provided', () => {
        const { container } = render(<Card>Static</Card>);
        const div = container.firstChild as HTMLElement;
        expect(div).not.toHaveAttribute('tabIndex');
    });

    it('has tabIndex="0" when onClick is provided', () => {
        const { container } = render(<Card onClick={vi.fn()}>Clickable</Card>);
        const div = container.firstChild as HTMLElement;
        expect(div).toHaveAttribute('tabIndex', '0');
    });

    it('applies cursor-pointer class when onClick is provided', () => {
        const { container } = render(<Card onClick={vi.fn()}>Clickable</Card>);
        const div = container.firstChild as HTMLElement;
        expect(div).toHaveClass('cursor-pointer');
    });

    it('does not apply cursor-pointer class when onClick is not provided', () => {
        const { container } = render(<Card>Static</Card>);
        const div = container.firstChild as HTMLElement;
        expect(div).not.toHaveClass('cursor-pointer');
    });

    it('calls onClick when clicked', () => {
        const onClick = vi.fn();
        render(<Card onClick={onClick}>Click me</Card>);
        fireEvent.click(screen.getByText('Click me'));
        expect(onClick).toHaveBeenCalledTimes(1);
    });

    it('calls onClick on Enter key press', () => {
        const onClick = vi.fn();
        const { container } = render(<Card onClick={onClick}>Keyboard</Card>);
        const div = container.firstChild as HTMLElement;
        fireEvent.keyDown(div, { key: 'Enter' });
        expect(onClick).toHaveBeenCalledTimes(1);
    });

    it('calls onClick on Space key press', () => {
        const onClick = vi.fn();
        const { container } = render(<Card onClick={onClick}>Keyboard</Card>);
        const div = container.firstChild as HTMLElement;
        fireEvent.keyDown(div, { key: ' ' });
        expect(onClick).toHaveBeenCalledTimes(1);
    });

    it('does not call onClick on other key presses', () => {
        const onClick = vi.fn();
        const { container } = render(<Card onClick={onClick}>Keyboard</Card>);
        const div = container.firstChild as HTMLElement;
        fireEvent.keyDown(div, { key: 'Tab' });
        expect(onClick).not.toHaveBeenCalled();
    });

    it('does not attach keyDown handler when no onClick', () => {
        const { container } = render(<Card>No click</Card>);
        const div = container.firstChild as HTMLElement;
        // Should not throw when key is pressed
        expect(() => fireEvent.keyDown(div, { key: 'Enter' })).not.toThrow();
    });
});
